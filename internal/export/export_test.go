package export

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

func testJob(id string, exports ...job.Export) job.Job {
	return job.Job{ID: id, Spec: job.Spec{
		Schema: job.Schema{Fields: []job.Field{
			{Name: "id", Type: "int"}, {Name: "day", Type: "date", Format: "2006-01-02"}, {Name: "amount", Type: "float"},
		}},
		Transforms: []job.Transform{{Op: "multiply", Fields: []string{"amount", "id"}, Target: "total"}},
		Exports:    exports,
	}}
}

func order(i int, amount float64) record.Record {
	r := record.New("orders", i)
	r.Fields["id"] = int64(i)
	r.Fields["day"] = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	r.Fields["amount"] = amount
	r.Fields["total"] = amount * float64(i)
	return r
}

var testAggs = []job.AggregateResult{{
	Name: "by_day", GroupBy: []string{"day"},
	Groups: []job.GroupResult{{Key: map[string]any{"day": "2026-10-01"}, Metrics: map[string]float64{"count": 2, "sum_total": 25.5}}},
}}

func allFileExports() []job.Export {
	return []job.Export{
		{Type: "json", Content: "records"}, {Type: "csv", Content: "records"},
		{Type: "json", Content: "aggregates"}, {Type: "csv", Content: "aggregates"},
	}
}

func TestFilesCommit(t *testing.T) {
	root := t.TempDir()
	set, err := Open(t.Context(), testJob("JOB1", allFileExports()...), Deps{OutputDir: root})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []record.Record{order(1, 10.5), order(2, 7.5)} {
		if err := set.WriteRecord(r); err != nil {
			t.Fatal(err)
		}
	}

	dir := filepath.Join(root, "JOB1")
	if _, err := os.Stat(filepath.Join(dir, "records.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("records.json is visible before commit")
	}

	outputs, err := set.Commit(t.Context(), testAggs)
	if err != nil {
		t.Fatal(err)
	}
	if len(outputs) != 4 || outputs[0].Rows != 2 || outputs[0].Location != filepath.Join(dir, "records.json") || outputs[3].Rows != 2 {
		t.Errorf("outputs = %+v", outputs)
	}

	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if strings.Join(names, ",") != "aggregates.csv,aggregates.json,records.csv,records.json" {
		t.Errorf("files = %v (temp files must be gone)", names)
	}

	var records []map[string]any
	readJSON(t, filepath.Join(dir, "records.json"), &records)
	if len(records) != 2 || records[0]["day"] != "2026-10-01" || records[0]["_source"] != "orders" || records[1]["total"] != 15.0 {
		t.Errorf("records.json = %v", records)
	}

	rows := readCSV(t, filepath.Join(dir, "records.csv"))
	want := [][]string{
		{"_source", "_index", "id", "day", "amount", "total"},
		{"orders", "1", "1", "2026-10-01", "10.5", "10.5"},
		{"orders", "2", "2", "2026-10-01", "7.5", "15"},
	}
	if !equalRows(rows, want) {
		t.Errorf("records.csv = %v", rows)
	}

	var aggDoc map[string][]job.AggregateResult
	readJSON(t, filepath.Join(dir, "aggregates.json"), &aggDoc)
	if aggDoc["aggregates"][0].Groups[0].Metrics["sum_total"] != 25.5 {
		t.Errorf("aggregates.json = %v", aggDoc)
	}

	aggRows := readCSV(t, filepath.Join(dir, "aggregates.csv"))
	wantAgg := [][]string{
		{"aggregation", "group", "metric", "value"},
		{"by_day", "day=2026-10-01", "count", "2"},
		{"by_day", "day=2026-10-01", "sum_total", "25.5"},
	}
	if !equalRows(aggRows, wantAgg) {
		t.Errorf("aggregates.csv = %v", aggRows)
	}
}

func TestFilesAbortLeavesNothing(t *testing.T) {
	root := t.TempDir()
	set, err := Open(t.Context(), testJob("JOB1", allFileExports()...), Deps{OutputDir: root})
	if err != nil {
		t.Fatal(err)
	}
	_ = set.WriteRecord(order(1, 1))
	set.Abort()
	set.Abort()

	if _, err := os.Stat(filepath.Join(root, "JOB1")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("job directory still exists after abort: %v", err)
	}
}

func TestOpenNeedsConfiguration(t *testing.T) {
	if _, err := Open(t.Context(), testJob("J", job.Export{Type: "json", Content: "records"}), Deps{}); !errors.Is(err, ErrNoOutputDir) {
		t.Errorf("file export without output dir: err = %v", err)
	}
	if _, err := Open(t.Context(), testJob("J", job.Export{Type: "sqlite", Content: "records"}), Deps{}); !errors.Is(err, ErrNoDatabase) {
		t.Errorf("sqlite export without database: err = %v", err)
	}
	bad := testJob("../escape", job.Export{Type: "json", Content: "records"})
	if _, err := Open(t.Context(), bad, Deps{OutputDir: t.TempDir()}); err == nil {
		t.Error("a job id with .. must be refused")
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenSQLite(t.Context(), filepath.Join(t.TempDir(), "sub", "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func count(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestSQLiteSettingsAndMigrations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := OpenSQLite(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}

	var mode string
	var timeout int
	_ = db.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode)
	_ = db.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout)
	if mode != "wal" || timeout != 5000 {
		t.Errorf("journal_mode %q busy_timeout %d, want wal and 5000", mode, timeout)
	}
	if db.Stats().MaxOpenConnections != 1 {
		t.Errorf("max open connections = %d, want 1", db.Stats().MaxOpenConnections)
	}
	if n := count(t, db, "SELECT COUNT(*) FROM schema_migrations"); n != 1 {
		t.Errorf("applied migrations = %d, want 1", n)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenSQLite(t.Context(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	if n := count(t, db, "SELECT COUNT(*) FROM schema_migrations"); n != 1 {
		t.Errorf("migrations ran again on reopen: %d rows", n)
	}
}

func TestSQLiteCommitInBatches(t *testing.T) {
	db := openTestDB(t)
	j := testJob("JOB1", job.Export{Type: "sqlite", Content: "records"}, job.Export{Type: "sqlite", Content: "aggregates"})
	set, err := Open(t.Context(), j, Deps{DB: db})
	if err != nil {
		t.Fatal(err)
	}

	const n = 1234 // crosses two 500-row batches plus a remainder
	for i := 1; i <= n; i++ {
		if err := set.WriteRecord(order(i, 2)); err != nil {
			t.Fatal(err)
		}
	}
	outputs, err := set.Commit(t.Context(), testAggs)
	if err != nil {
		t.Fatal(err)
	}

	if outputs[0].Rows != n || outputs[0].Location != "job_records" || outputs[1].Rows != 1 {
		t.Errorf("outputs = %+v", outputs)
	}
	if got := count(t, db, "SELECT COUNT(*) FROM job_records WHERE job_id = ?", "JOB1"); got != n {
		t.Errorf("job_records rows = %d, want %d", got, n)
	}
	if got := count(t, db, "SELECT COUNT(*) FROM job_records WHERE json_extract(data, '$.day') = '2026-10-01' AND json_extract(data, '$.total') = 20"); got != 1 {
		t.Errorf("record 10 not stored as expected JSON (%d matches)", got)
	}
	var metrics string
	_ = db.QueryRowContext(t.Context(), "SELECT metrics FROM job_aggregates WHERE job_id = 'JOB1' AND aggregation = 'by_day'").Scan(&metrics)
	if metrics != `{"count":2,"sum_total":25.5}` {
		t.Errorf("metrics = %s", metrics)
	}
}

func TestSQLiteAbortRollsBack(t *testing.T) {
	db := openTestDB(t)
	set, err := Open(t.Context(), testJob("JOB1", job.Export{Type: "sqlite", Content: "records"}), Deps{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 600; i++ { // 500 already inserted inside the transaction
		_ = set.WriteRecord(order(i, 1))
	}
	set.Abort()

	if got := count(t, db, "SELECT COUNT(*) FROM job_records"); got != 0 {
		t.Errorf("rows after abort = %d, want 0", got)
	}
}

func TestSQLiteJobsTakeTurns(t *testing.T) {
	db := openTestDB(t)
	exports := job.Export{Type: "sqlite", Content: "records"}

	first, err := Open(t.Context(), testJob("A", exports), Deps{DB: db})
	if err != nil {
		t.Fatal(err)
	}

	opened := make(chan *Set)
	go func() {
		second, err := Open(t.Context(), testJob("B", exports), Deps{DB: db})
		if err != nil {
			t.Error(err)
		}
		opened <- second
	}()

	select {
	case <-opened:
		t.Fatal("second job got the single writer connection while the first held it")
	case <-time.After(50 * time.Millisecond):
	}

	_ = first.WriteRecord(order(1, 1))
	if _, err := first.Commit(t.Context(), nil); err != nil {
		t.Fatal(err)
	}

	select {
	case second := <-opened:
		_ = second.WriteRecord(order(2, 1))
		if _, err := second.Commit(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second job never got the connection after the first committed")
	}
	if got := count(t, db, "SELECT COUNT(*) FROM job_records"); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

func TestSQLiteOpenGivesUpWhenCancelled(t *testing.T) {
	db := openTestDB(t)
	exports := job.Export{Type: "sqlite", Content: "records"}
	first, err := Open(t.Context(), testJob("A", exports), Deps{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Abort()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := Open(ctx, testJob("B", exports), Deps{DB: db}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded while waiting for the writer", err)
	}
}

func TestRemoveDeletesFilesAndRows(t *testing.T) {
	db := openTestDB(t)
	root := t.TempDir()
	deps := Deps{OutputDir: root, DB: db}

	exports := append(allFileExports(), job.Export{Type: "sqlite", Content: "records"}, job.Export{Type: "sqlite", Content: "aggregates"})
	for _, id := range []string{"KEEP", "GONE"} {
		set, err := Open(t.Context(), testJob(id, exports...), deps)
		if err != nil {
			t.Fatal(err)
		}
		_ = set.WriteRecord(order(1, 1))
		if _, err := set.Commit(t.Context(), testAggs); err != nil {
			t.Fatal(err)
		}
	}

	gone := testJob("GONE", exports...)
	gone.Status = job.StatusCompleted
	if err := Remove(t.Context(), gone, deps); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if err := Remove(t.Context(), gone, deps); err != nil {
		t.Errorf("second remove: %v", err)
	}

	if _, err := os.Stat(filepath.Join(root, "GONE")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("output directory still there: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "KEEP", "records.json")); err != nil {
		t.Errorf("another job's files were removed: %v", err)
	}
	for _, table := range []string{"job_records", "job_aggregates"} {
		if n := count(t, db, "SELECT COUNT(*) FROM "+table+" WHERE job_id = 'GONE'"); n != 0 {
			t.Errorf("%s still has %d rows for the removed job", table, n)
		}
		if n := count(t, db, "SELECT COUNT(*) FROM "+table+" WHERE job_id = 'KEEP'"); n != 1 {
			t.Errorf("%s has %d rows for the other job, want 1", table, n)
		}
	}
}

func TestRemoveWaitsForDatabaseThenGivesUp(t *testing.T) {
	db := openTestDB(t)
	busy, err := Open(t.Context(), testJob("BUSY", job.Export{Type: "sqlite", Content: "records"}), Deps{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Abort()

	j := testJob("OLD", job.Export{Type: "sqlite", Content: "records"})
	j.Status = job.StatusCompleted
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := Remove(ctx, j, Deps{DB: db}); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded while another job holds the database", err)
	}

	j.Status = job.StatusFailed
	if err := Remove(ctx, j, Deps{DB: db}); err != nil {
		t.Errorf("a failed job committed no rows, so it must not wait for the database: %v", err)
	}
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v\n%s", path, err, b)
	}
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func equalRows(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.Join(a[i], "|") != strings.Join(b[i], "|") {
			return false
		}
	}
	return true
}
