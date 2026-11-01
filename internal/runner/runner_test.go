package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

type setup struct {
	store  *job.MemoryStore
	runner *Runner
}

func newSetup(t *testing.T, workers, queue int, deps job.SourceDeps) setup {
	t.Helper()
	store := job.NewMemoryStore()
	r := New(Options{
		Store:            store,
		Deps:             deps,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Workers:          workers,
		QueueSize:        queue,
		ProgressInterval: 10 * time.Millisecond,
	})
	r.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return setup{store: store, runner: r}
}

func (s setup) submit(t *testing.T, spec job.Spec) string {
	t.Helper()
	spec.ApplyDefaults()
	j := job.New(spec, "req-test")
	if err := s.store.Create(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if err := s.runner.Submit(j.ID); err != nil {
		t.Fatal(err)
	}
	return j.ID
}

func (s setup) waitFor(t *testing.T, id string, ok func(job.Job) bool) job.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		j, err := s.store.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if ok(j) {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting; job is %s %+v", j.Status, j.Progress)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func finished(j job.Job) bool { return j.Finished() }

func httpSpec(url string) job.Spec {
	return job.Spec{
		Name:    "api",
		Sources: []job.SourceSpec{{Name: "api", Type: "http", URL: url}},
		Schema:  job.Schema{Fields: []job.Field{{Name: "id", Type: "int", Required: true}}},
	}
}

// slowServer sends two records, then holds the response open until the
// client goes away, like a slow upstream API.
func slowServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `[{"id":1},{"id":2},`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return srv
}

func httpDeps() job.SourceDeps {
	return job.SourceDeps{Client: source.NewHTTPClient(true)}
}

func TestRunnerCompletesSampleJob(t *testing.T) {
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec job.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}

	s := newSetup(t, 2, 10, job.SourceDeps{Files: os.DirFS("../..")})
	j := s.waitFor(t, s.submit(t, spec), finished)

	if j.Status != job.StatusCompleted || j.Error != "" {
		t.Fatalf("status %s error %q", j.Status, j.Error)
	}
	want := job.Progress{RecordsRead: 15, RecordsValid: 7, RecordsFailed: 8}
	if j.Progress != want {
		t.Errorf("progress = %+v, want %+v", j.Progress, want)
	}
	if len(j.FailedRecords) != 8 {
		t.Errorf("stored %d failed records, want 8", len(j.FailedRecords))
	}
	if first := j.FailedRecords[0]; first.Source != "orders-csv" || first.Index != 3 {
		t.Errorf("failed records not sorted by source and row: first is %s #%d", first.Source, first.Index)
	}
	if j.StartedAt == nil || j.FinishedAt == nil || j.FinishedAt.Before(*j.StartedAt) {
		t.Errorf("bad timestamps: started %v finished %v", j.StartedAt, j.FinishedAt)
	}
	if len(j.Aggregates) != 3 || j.Aggregates[2].Groups[0].Metrics["sum_total"] != 4721.98 {
		t.Errorf("aggregates not saved: %+v", j.Aggregates)
	}
}

func TestRunnerFailsWhenSourceFails(t *testing.T) {
	deps := job.SourceDeps{Files: fstest.MapFS{"bad.json": {Data: []byte(`[{"id":1}, oops]`)}}}
	s := newSetup(t, 1, 10, deps)

	spec := job.Spec{
		Sources: []job.SourceSpec{{Name: "file", Type: "json", Path: "bad.json"}},
		Schema:  job.Schema{Fields: []job.Field{{Name: "id", Type: "int"}}},
	}
	j := s.waitFor(t, s.submit(t, spec), finished)

	if j.Status != job.StatusFailed || !strings.Contains(j.Error, "source failed") {
		t.Errorf("status %s error %q", j.Status, j.Error)
	}
	if j.Progress.SourceErrors != 1 {
		t.Errorf("source errors = %d", j.Progress.SourceErrors)
	}
}

func TestRunnerTimeout(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())

	spec := httpSpec(slowServer(t).URL)
	spec.Options.TimeoutSeconds = 1
	j := s.waitFor(t, s.submit(t, spec), finished)

	if j.Status != job.StatusFailed || j.Error != "timed out after 1s" {
		t.Errorf("status %s error %q", j.Status, j.Error)
	}
}

func TestRunnerReportsProgressWhileRunning(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())
	id := s.submit(t, httpSpec(slowServer(t).URL))

	j := s.waitFor(t, id, func(j job.Job) bool { return j.Progress.RecordsValid == 2 })
	if j.Status != job.StatusRunning {
		t.Errorf("status = %s, want running while progress updates", j.Status)
	}
	if _, err := s.runner.Cancel(context.Background(), id); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerCancelRunningJob(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())
	id := s.submit(t, httpSpec(slowServer(t).URL))
	s.waitFor(t, id, func(j job.Job) bool { return j.Status == job.StatusRunning })

	if _, err := s.runner.Cancel(context.Background(), id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	j := s.waitFor(t, id, finished)

	if j.Status != job.StatusCancelled || j.Error != "cancelled by user" {
		t.Errorf("status %s error %q", j.Status, j.Error)
	}
	if j.Progress.RecordsRead != 2 {
		t.Errorf("progress before cancel lost: %+v", j.Progress)
	}
	if j.Aggregates != nil {
		t.Error("a cancelled job must not keep partial aggregates")
	}
}

func TestRunnerCancelPendingJob(t *testing.T) {
	s := newSetup(t, 0, 10, job.SourceDeps{})
	id := s.submit(t, httpSpec("http://unused"))

	j, err := s.runner.Cancel(context.Background(), id)
	if err != nil || j.Status != job.StatusCancelled || j.FinishedAt == nil {
		t.Fatalf("cancel pending: %+v, %v", j, err)
	}
	if _, err := s.runner.Cancel(context.Background(), id); !errors.Is(err, ErrJobFinished) {
		t.Errorf("second cancel: err = %v, want ErrJobFinished", err)
	}
	if _, err := s.runner.Cancel(context.Background(), "missing"); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("unknown job: err = %v, want ErrNotFound", err)
	}
}

func TestRunnerSkipsCancelledJobInQueue(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())
	srv := slowServer(t)

	blocker := s.submit(t, httpSpec(srv.URL))
	s.waitFor(t, blocker, func(j job.Job) bool { return j.Status == job.StatusRunning })

	queued := s.submit(t, httpSpec(srv.URL))
	if _, err := s.runner.Cancel(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	if _, err := s.runner.Cancel(context.Background(), blocker); err != nil {
		t.Fatal(err)
	}
	s.waitFor(t, blocker, finished)

	time.Sleep(50 * time.Millisecond)
	if j, _ := s.store.Get(context.Background(), queued); j.Status != job.StatusCancelled || j.StartedAt != nil {
		t.Errorf("cancelled job was started: %+v", j)
	}
}

func TestRunnerQueueFull(t *testing.T) {
	s := newSetup(t, 0, 1, job.SourceDeps{})

	if err := s.runner.Submit("a"); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := s.runner.Submit("b"); !errors.Is(err, ErrQueueFull) {
		t.Errorf("second submit: err = %v, want ErrQueueFull", err)
	}
}

func TestRunnerShutdownCancelsRunningJobs(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())
	id := s.submit(t, httpSpec(slowServer(t).URL))
	s.waitFor(t, id, func(j job.Job) bool { return j.Status == job.StatusRunning })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.runner.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	j, _ := s.store.Get(context.Background(), id)
	if j.Status != job.StatusCancelled || j.Error != "server shutting down" {
		t.Errorf("status %s error %q", j.Status, j.Error)
	}
	if err := s.runner.Submit("late"); !errors.Is(err, ErrStopped) {
		t.Errorf("submit after shutdown: err = %v, want ErrStopped", err)
	}
}
