package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

// fakeSource lets a test decide exactly what a source does.
type fakeSource struct {
	name string
	read func(ctx context.Context, emit source.Emit) error
}

func (f fakeSource) Name() string                                     { return f.name }
func (f fakeSource) Read(ctx context.Context, emit source.Emit) error { return f.read(ctx, emit) }

// endless emits records until emit fails, then closes stopped.
func endless(name string, stopped chan<- struct{}) fakeSource {
	return fakeSource{name: name, read: func(ctx context.Context, emit source.Emit) error {
		defer close(stopped)
		for i := 1; ; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			r := record.New(name, i)
			r.Fields["id"] = "1"
			if err := emit(source.Item{Record: r}); err != nil {
				return err
			}
		}
	}}
}

func failing(name string) fakeSource {
	return fakeSource{name: name, read: func(context.Context, source.Emit) error {
		return errors.New("connection refused")
	}}
}

func fixed(name string, n int) fakeSource {
	return fakeSource{name: name, read: func(_ context.Context, emit source.Emit) error {
		for i := 1; i <= n; i++ {
			r := record.New(name, i)
			r.Fields["id"] = "1"
			if err := emit(source.Item{Record: r}); err != nil {
				return err
			}
		}
		return nil
	}}
}

func simpleSpec() job.Spec {
	s := job.Spec{Schema: job.Schema{Fields: []job.Field{{Name: "id", Type: "int", Required: true}}}}
	s.ApplyDefaults()
	return s
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not stop: goroutine leak", what)
	}
}

func TestRunSampleFiles(t *testing.T) {
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec job.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	spec.ApplyDefaults()

	srcs, err := job.BuildSources(spec.Sources, job.SourceDeps{Files: os.DirFS("../..")})
	if err != nil {
		t.Fatal(err)
	}

	var c Counters
	errs := NewErrors(100)
	if err := Run(context.Background(), spec, srcs, &c, errs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := job.Progress{RecordsRead: 15, RecordsValid: 7, RecordsFailed: 8}
	if got := c.Snapshot(); got != want {
		t.Errorf("progress = %+v, want %+v", got, want)
	}

	items, total := errs.Snapshot()
	if total != 8 || len(items) != 8 {
		t.Fatalf("stored %d of %d errors, want 8", len(items), total)
	}
	var failed []string
	for _, e := range items {
		failed = append(failed, e.Source+"#"+strings.Join(e.Reasons, ","))
	}
	slices.Sort(failed)
	for _, want := range []string{
		"orders-csv#amount: must be a number",
		"orders-csv#category: must be one of books, electronics, toys",
		"orders-json#quantity: must be a whole number",
	} {
		if !slices.Contains(failed, want) {
			t.Errorf("missing failure %q in\n%s", want, strings.Join(failed, "\n"))
		}
	}
}

func TestRunSourceFailureStopsEverything(t *testing.T) {
	stopped := make(chan struct{})
	srcs := []source.Source{endless("slow", stopped), failing("api")}

	var c Counters
	errs := NewErrors(10)
	err := Run(context.Background(), simpleSpec(), srcs, &c, errs)

	if !errors.Is(err, ErrSourceFailed) || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v, want ErrSourceFailed with the cause", err)
	}
	waitClosed(t, stopped, "the other source")
	if c.SourceErrors.Load() != 1 {
		t.Errorf("source errors = %d, want 1", c.SourceErrors.Load())
	}
	if items, _ := errs.Snapshot(); items[0].Stage != "source" || items[0].Source != "api" {
		t.Errorf("source failure not stored: %+v", items)
	}
}

func TestRunContinueOnSourceError(t *testing.T) {
	spec := simpleSpec()
	spec.Options.ContinueOnSourceError = true

	var c Counters
	err := Run(context.Background(), spec, []source.Source{failing("api"), fixed("file", 5)}, &c, NewErrors(10))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Valid.Load() != 5 || c.SourceErrors.Load() != 1 {
		t.Errorf("progress = %+v", c.Snapshot())
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	errUser := errors.New("cancelled by user")
	stopped := make(chan struct{})

	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel(errUser)
	}()

	var c Counters
	err := Run(ctx, simpleSpec(), []source.Source{endless("big", stopped)}, &c, NewErrors(10))
	if !errors.Is(err, errUser) {
		t.Fatalf("err = %v, want the cancel cause", err)
	}
	waitClosed(t, stopped, "the source")
	if c.Valid.Load() == 0 {
		t.Error("no records were processed before the cancel")
	}
}

func TestRunStopsOnTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})

	err := Run(ctx, simpleSpec(), []source.Source{endless("big", stopped)}, &Counters{}, NewErrors(10))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	waitClosed(t, stopped, "the source")
}

func TestRunRowErrorsFromSources(t *testing.T) {
	src := fakeSource{name: "csv", read: func(_ context.Context, emit source.Emit) error {
		_ = emit(source.Item{Err: &source.RowError{Source: "csv", Index: 3, Err: source.ErrTooManyFields}})
		r := record.New("csv", 4)
		r.Fields["id"] = "4"
		return emit(source.Item{Record: r})
	}}

	var c Counters
	errs := NewErrors(10)
	if err := Run(context.Background(), simpleSpec(), []source.Source{src}, &c, errs); err != nil {
		t.Fatal(err)
	}
	if got := c.Snapshot(); got.RecordsRead != 2 || got.RecordsValid != 1 || got.RecordsFailed != 1 {
		t.Errorf("progress = %+v", got)
	}
	if items, _ := errs.Snapshot(); items[0].Stage != "ingest" || items[0].Index != 3 {
		t.Errorf("row error = %+v", items[0])
	}
}

func TestStageUsesAllWorkers(t *testing.T) {
	const workers = 4
	in := make(chan record.Record, workers)
	for i := range workers {
		in <- record.New("t", i)
	}
	close(in)

	var active, peak atomic.Int32
	var once sync.Once
	allIn := make(chan struct{})
	fn := func(r record.Record) (record.Record, []string) {
		if n := active.Add(1); n > peak.Load() {
			peak.Store(n)
		}
		if active.Load() == workers {
			once.Do(func() { close(allIn) })
		}
		select {
		case <-allIn:
		case <-time.After(time.Second):
		}
		active.Add(-1)
		return r, nil
	}

	out := runStage(context.Background(), "test", workers, 1, in, fn, &Counters{}, NewErrors(1))
	var n int
	for range out {
		n++
	}
	if n != workers || peak.Load() != workers {
		t.Errorf("got %d records, peak %d concurrent workers; want %d and %d", n, peak.Load(), workers, workers)
	}
}

func TestStageDropsRejectedRecords(t *testing.T) {
	in := make(chan record.Record, 3)
	for i := 1; i <= 3; i++ {
		in <- record.New("t", i)
	}
	close(in)

	var c Counters
	errs := NewErrors(10)
	evenOnly := func(r record.Record) (record.Record, []string) {
		if r.Index%2 == 1 {
			return record.Record{}, []string{"odd"}
		}
		return r, nil
	}

	var got []int
	for r := range runStage(context.Background(), "check", 2, 1, in, evenOnly, &c, errs) {
		got = append(got, r.Index)
	}
	if !slices.Equal(got, []int{2}) || c.Failed.Load() != 2 {
		t.Errorf("passed %v, failed %d", got, c.Failed.Load())
	}
	if items, _ := errs.Snapshot(); items[0].Stage != "check" {
		t.Errorf("stage name not recorded: %+v", items[0])
	}
}
