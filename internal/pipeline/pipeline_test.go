package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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
	res, err := Run(context.Background(), spec, srcs, nil, &c, errs)
	if err != nil {
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

	checkSampleAggregates(t, res.Aggregates)
}

func checkSampleAggregates(t *testing.T, got []job.AggregateResult) {
	t.Helper()
	group := func(cat string, m map[string]float64) job.GroupResult {
		return job.GroupResult{Key: map[string]any{"category": cat}, Metrics: m}
	}
	region := func(r string, count, sum float64) job.GroupResult {
		return job.GroupResult{Key: map[string]any{"region": r}, Metrics: map[string]float64{"count": count, "sum_total": sum}}
	}
	want := []job.AggregateResult{
		{Name: "by_category", GroupBy: []string{"category"}, Groups: []job.GroupResult{
			group("books", map[string]float64{"count": 2, "sum_total": 561, "avg_amount": 131.25, "min_amount": 12, "max_amount": 250.5}),
			group("electronics", map[string]float64{"count": 4, "sum_total": 4018.98, "avg_amount": 804.75, "min_amount": 320, "max_amount": 1200}),
			group("toys", map[string]float64{"count": 1, "sum_total": 142, "avg_amount": 35.5, "min_amount": 35.5, "max_amount": 35.5}),
		}},
		{Name: "by_region", GroupBy: []string{"region"}, Groups: []job.GroupResult{
			region("AMER", 1, 1200), region("APAC", 4, 1862), region("EMEA", 2, 1659.98),
		}},
		{Name: "overall", GroupBy: []string{}, Groups: []job.GroupResult{
			{Key: map[string]any{}, Metrics: map[string]float64{"count": 7, "sum_total": 4721.98}},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		gj, _ := json.MarshalIndent(got, "", "  ")
		t.Errorf("aggregates =\n%s", gj)
	}
}

func TestRunSourceFailureStopsEverything(t *testing.T) {
	stopped := make(chan struct{})
	srcs := []source.Source{endless("slow", stopped), failing("api")}

	var c Counters
	errs := NewErrors(10)
	_, err := Run(context.Background(), simpleSpec(), srcs, nil, &c, errs)

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
	_, err := Run(context.Background(), spec, []source.Source{failing("api"), fixed("file", 5)}, nil, &c, NewErrors(10))
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
	_, err := Run(ctx, simpleSpec(), []source.Source{endless("big", stopped)}, nil, &c, NewErrors(10))
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

	_, err := Run(ctx, simpleSpec(), []source.Source{endless("big", stopped)}, nil, &Counters{}, NewErrors(10))
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
	if _, err := Run(context.Background(), simpleSpec(), []source.Source{src}, nil, &c, errs); err != nil {
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

	out := runStage(context.Background(), "test", workers, 1, in, fn, &StageTimer{}, &Counters{}, NewErrors(1))
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
	for r := range runStage(context.Background(), "check", 2, 1, in, evenOnly, &StageTimer{}, &c, errs) {
		got = append(got, r.Index)
	}
	if !slices.Equal(got, []int{2}) || c.Failed.Load() != 2 {
		t.Errorf("passed %v, failed %d", got, c.Failed.Load())
	}
	if items, _ := errs.Snapshot(); items[0].Stage != "check" {
		t.Errorf("stage name not recorded: %+v", items[0])
	}
}

// recordingWriter collects records, or fails after failAfter writes.
type recordingWriter struct {
	got       []record.Record
	failAfter int
}

func (w *recordingWriter) WriteRecord(r record.Record) error {
	if w.failAfter > 0 && len(w.got) == w.failAfter {
		return errors.New("disk full")
	}
	w.got = append(w.got, r)
	return nil
}

func TestRunWritesEveryValidRecord(t *testing.T) {
	w := &recordingWriter{}
	var c Counters
	if _, err := Run(context.Background(), simpleSpec(), []source.Source{fixed("a", 30), fixed("b", 20)}, w, &c, NewErrors(10)); err != nil {
		t.Fatal(err)
	}
	if len(w.got) != 50 || c.Valid.Load() != 50 {
		t.Errorf("writer got %d records, valid %d; want 50", len(w.got), c.Valid.Load())
	}
}

func TestRunStopsWhenExportFails(t *testing.T) {
	stopped := make(chan struct{})
	w := &recordingWriter{failAfter: 3}

	_, err := Run(context.Background(), simpleSpec(), []source.Source{endless("big", stopped)}, w, &Counters{}, NewErrors(10))
	if !errors.Is(err, ErrExportFailed) || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("err = %v, want ErrExportFailed with the cause", err)
	}
	waitClosed(t, stopped, "the source")
}

func TestRunRecordsStageLatencies(t *testing.T) {
	var c Counters
	if _, err := Run(context.Background(), simpleSpec(), []source.Source{fixed("a", 10)}, nil, &c, NewErrors(10)); err != nil {
		t.Fatal(err)
	}

	stages := c.Stages()
	if len(stages) != 3 {
		t.Fatalf("got %d stages", len(stages))
	}
	for _, s := range stages {
		if s.Records != 10 || s.MaxMicros < s.AvgMicros {
			t.Errorf("stage %+v: want 10 records and max >= avg", s)
		}
	}
	if stages[0].Stage != "validate" || stages[1].Stage != "transform" || stages[2].Stage != "sink" {
		t.Errorf("stage names = %v", stages)
	}
}

func TestStageTimerConcurrent(t *testing.T) {
	var st StageTimer
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st.Observe(time.Duration(i) * time.Microsecond)
		}()
	}
	wg.Wait()

	s := st.stat("x")
	if s.Records != 100 || s.MaxMicros != 100 || s.AvgMicros != 50.5 {
		t.Errorf("stat = %+v, want 100 records, max 100, avg 50.5", s)
	}
}
