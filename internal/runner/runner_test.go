package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/export"
	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

type setup struct {
	store  *job.MemoryStore
	runner *Runner
	outDir string
	db     *sql.DB
}

func newSetup(t *testing.T, workers, queue int, deps job.SourceDeps) setup {
	t.Helper()
	store := job.NewMemoryStore()
	outDir := t.TempDir()
	db, err := export.OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	r := New(Options{
		Store:            store,
		Deps:             deps,
		Exports:          export.Deps{OutputDir: outDir, DB: db},
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
		_ = db.Close()
	})
	return setup{store: store, runner: r, outDir: outDir, db: db}
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
	if len(j.Outputs) != 5 {
		t.Fatalf("outputs = %+v, want 5", j.Outputs)
	}
	for _, o := range j.Outputs {
		if o.Rows != 7 {
			t.Errorf("output %s/%s has %d rows, want 7 (records or groups)", o.Type, o.Content, o.Rows)
		}
		if o.Type == "sqlite" {
			continue
		}
		if _, err := os.Stat(o.Location); err != nil {
			t.Errorf("output file missing: %v", err)
		}
	}
	if len(j.Stages) != 3 || j.Stages[0].Records != 15 || j.Stages[1].Records != 7 {
		t.Errorf("stages = %+v, want validate 15 and transform 7 records", j.Stages)
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

// panicFS panics when boom.csv is opened, like a bug inside a source.
type panicFS struct{ fstest.MapFS }

func (f panicFS) Open(name string) (fs.File, error) {
	if name == "boom.csv" {
		panic("bug in a source")
	}
	return f.MapFS.Open(name)
}

func TestRunnerSurvivesPanicInJob(t *testing.T) {
	deps := job.SourceDeps{Files: panicFS{fstest.MapFS{"good.json": {Data: []byte(`[{"id":1},{"id":2}]`)}}}}
	s := newSetup(t, 1, 10, deps)
	spec := func(typ, path string) job.Spec {
		return job.Spec{
			Sources: []job.SourceSpec{{Name: "file", Type: typ, Path: path}},
			Schema:  job.Schema{Fields: []job.Field{{Name: "id", Type: "int"}}},
		}
	}

	bad := s.waitFor(t, s.submit(t, spec("csv", "boom.csv")), finished)
	if bad.Status != job.StatusFailed || bad.Error != "internal error in source file: bug in a source" {
		t.Errorf("panicking job: status %s error %q", bad.Status, bad.Error)
	}

	// The same worker goroutine must still be alive to run the next job.
	good := s.waitFor(t, s.submit(t, spec("json", "good.json")), finished)
	if good.Status != job.StatusCompleted || good.Progress.RecordsValid != 2 {
		t.Errorf("next job: status %s progress %+v", good.Status, good.Progress)
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
	spec := httpSpec(slowServer(t).URL)
	spec.Exports = []job.Export{{Type: "json", Content: "records"}, {Type: "sqlite", Content: "records"}}
	id := s.submit(t, spec)
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
	if _, err := os.Stat(filepath.Join(s.outDir, id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a cancelled job left an output directory: %v", err)
	}
	var rows int
	_ = s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM job_records").Scan(&rows)
	if rows != 0 {
		t.Errorf("a cancelled job left %d sqlite rows", rows)
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

func TestRunnerExportsToSQLite(t *testing.T) {
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec job.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	spec.Exports = []job.Export{{Type: "sqlite", Content: "records"}, {Type: "sqlite", Content: "aggregates"}}

	s := newSetup(t, 1, 10, job.SourceDeps{Files: os.DirFS("../..")})
	id := s.submit(t, spec)
	j := s.waitFor(t, id, finished)
	if j.Status != job.StatusCompleted {
		t.Fatalf("status %s error %q", j.Status, j.Error)
	}

	var records, groups int
	var total float64
	_ = s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM job_records WHERE job_id = ?", id).Scan(&records)
	_ = s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM job_aggregates WHERE job_id = ?", id).Scan(&groups)
	_ = s.db.QueryRowContext(t.Context(),
		"SELECT json_extract(metrics, '$.sum_total') FROM job_aggregates WHERE job_id = ? AND aggregation = 'overall'", id).Scan(&total)
	if records != 7 || groups != 7 || total != 4721.98 {
		t.Errorf("sqlite has %d records, %d groups, overall total %v; want 7, 7, 4721.98", records, groups, total)
	}
}

func TestWorkerDoesNotStartJobsAfterShutdown(t *testing.T) {
	for range 30 {
		store := job.NewMemoryStore()
		r := New(Options{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), QueueSize: 1})

		j := job.New(httpSpec("http://unused"), "")
		if err := store.Create(context.Background(), j); err != nil {
			t.Fatal(err)
		}
		if err := r.Submit(j.ID); err != nil {
			t.Fatal(err)
		}
		r.stop(errShutdown)

		r.wg.Add(1)
		r.work()

		if got, _ := store.Get(context.Background(), j.ID); got.Status != job.StatusPending || got.StartedAt != nil {
			t.Fatalf("a queued job was started after shutdown began: status %s", got.Status)
		}
	}
}

func TestRunnerDelete(t *testing.T) {
	data, err := os.ReadFile("../../examples/orders-job.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec job.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}

	s := newSetup(t, 1, 10, job.SourceDeps{Files: os.DirFS("../..")})
	id := s.submit(t, spec)
	s.waitFor(t, id, finished)

	if err := s.runner.Delete(context.Background(), id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.store.Get(context.Background(), id); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("job still in the store: %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.outDir, id)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("output directory still there: %v", err)
	}
	var rows int
	_ = s.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM job_records WHERE job_id = ?", id).Scan(&rows)
	if rows != 0 {
		t.Errorf("%d sqlite rows left", rows)
	}
	if err := s.runner.Delete(context.Background(), id); !errors.Is(err, job.ErrNotFound) {
		t.Errorf("second delete: err = %v, want ErrNotFound", err)
	}
}

func TestRunnerDeleteRefusesActiveJobs(t *testing.T) {
	s := newSetup(t, 1, 10, httpDeps())
	srv := slowServer(t)

	running := s.submit(t, httpSpec(srv.URL))
	s.waitFor(t, running, func(j job.Job) bool { return j.Status == job.StatusRunning })
	pending := s.submit(t, httpSpec(srv.URL))

	for _, id := range []string{running, pending} {
		if err := s.runner.Delete(context.Background(), id); !errors.Is(err, ErrJobActive) {
			t.Errorf("delete %s: err = %v, want ErrJobActive", id, err)
		}
	}
	for _, id := range []string{pending, running} {
		if _, err := s.runner.Cancel(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	s.waitFor(t, running, finished)
}

func TestRunnerEvictsOldFinishedJobs(t *testing.T) {
	store := job.NewMemoryStore()
	r := New(Options{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), JobTTL: time.Hour})
	now := time.Now()
	old, recent := now.Add(-2*time.Hour), now.Add(-time.Minute)

	jobs := map[string]job.Job{
		"old-done":    {ID: "old-done", Status: job.StatusCompleted, FinishedAt: &old},
		"old-failed":  {ID: "old-failed", Status: job.StatusFailed, FinishedAt: &old},
		"recent-done": {ID: "recent-done", Status: job.StatusCompleted, FinishedAt: &recent},
		"running":     {ID: "running", Status: job.StatusRunning, StartedAt: &old},
		"pending":     {ID: "pending", Status: job.StatusPending},
	}
	for _, j := range jobs {
		if err := store.Create(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}

	r.evict(now)

	for id := range jobs {
		_, err := store.Get(context.Background(), id)
		gone := errors.Is(err, job.ErrNotFound)
		if want := strings.HasPrefix(id, "old-"); gone != want {
			t.Errorf("%s: evicted = %v, want %v", id, gone, want)
		}
	}
}

func TestRunnerEvictLoopRunsAndStops(t *testing.T) {
	store := job.NewMemoryStore()
	old := time.Now().Add(-time.Hour)
	if err := store.Create(context.Background(), job.Job{ID: "a", Status: job.StatusCompleted, FinishedAt: &old}); err != nil {
		t.Fatal(err)
	}
	r := New(Options{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		JobTTL: time.Minute, EvictInterval: 10 * time.Millisecond})
	r.Start()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := store.Get(context.Background(), "a"); errors.Is(err, job.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job was never evicted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Shutdown(ctx); err != nil {
		t.Errorf("eviction loop did not stop on shutdown: %v", err)
	}
}

func TestRunnerFailsWhenExportCannotOpen(t *testing.T) {
	s := newSetup(t, 1, 10, job.SourceDeps{Files: fstest.MapFS{"a.json": {Data: []byte(`[{"id":1}]`)}}})
	s.runner.exports.OutputDir = filepath.Join(s.outDir, "file-not-dir")
	if err := os.WriteFile(s.runner.exports.OutputDir, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	spec := job.Spec{
		Sources: []job.SourceSpec{{Name: "a", Type: "json", Path: "a.json"}},
		Schema:  job.Schema{Fields: []job.Field{{Name: "id", Type: "int"}}},
		Exports: []job.Export{{Type: "json", Content: "records"}},
	}
	j := s.waitFor(t, s.submit(t, spec), finished)
	if j.Status != job.StatusFailed || !strings.Contains(j.Error, "export failed") {
		t.Errorf("status %s error %q", j.Status, j.Error)
	}
}
