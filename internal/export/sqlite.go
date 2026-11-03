package export

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"

	_ "modernc.org/sqlite" // registers the pure-Go "sqlite" driver (no CGO)
)

// batchSize rows go into one multi-row INSERT. 500 rows × 4 columns stays far
// below SQLite's limit of 32766 bound parameters per statement.
const batchSize = 500

//go:embed migrations/*.sql
var migrations embed.FS

// OpenSQLite opens (creating if needed) the database at path and applies
// pending migrations.
//
//   - WAL journal mode: readers do not block the writer.
//   - busy_timeout 5000: wait up to 5 s for a lock instead of failing at once.
//   - One open connection: SQLite allows a single writer, so database/sql
//     queues callers instead of getting "database is locked" errors.
func OpenSQLite(ctx context.Context, path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate sqlite %s: %w", path, err)
	}
	return db, nil
}

// migrate applies every migrations/NNN_name.sql not yet recorded in
// schema_migrations, in order, each in its own transaction.
func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    INTEGER PRIMARY KEY,
		name       TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return err
	}

	entries, err := fs.ReadDir(migrations, "migrations")
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("migration %s: name must start with a number", name)
		}

		var applied int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, version).Scan(&applied); err != nil {
			return err
		}
		if applied > 0 {
			continue
		}

		body, err := fs.ReadFile(migrations, "migrations/"+name)
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, db, version, name, string(body)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, version int, name, body string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after a successful Commit

	if _, err := tx.ExecContext(ctx, body); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)`,
		version, name, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	return tx.Commit()
}

// insertRows inserts rows in chunks of batchSize using multi-row INSERTs:
// INSERT INTO t (a, b) VALUES (?, ?), (?, ?), ...
// table and columns are constants from this package, never user input.
func insertRows(ctx context.Context, tx *sql.Tx, table string, columns []string, rows [][]any) error {
	one := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", ") + ")"
	for start := 0; start < len(rows); start += batchSize {
		chunk := rows[start:min(start+batchSize, len(rows))]

		// Only identifiers are formatted in; every value is a bound ? parameter.
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(columns, ", "), //nolint:gosec // G201: table and columns are package constants
			strings.TrimSuffix(strings.Repeat(one+", ", len(chunk)), ", "))
		args := make([]any, 0, len(chunk)*len(columns))
		for _, r := range chunk {
			args = append(args, r...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert into %s: %w", table, err)
		}
	}
	return nil
}

func deleteRows(ctx context.Context, db *sql.DB, jobID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, q := range []string{
		`DELETE FROM job_records WHERE job_id = ?`,
		`DELETE FROM job_aggregates WHERE job_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, jobID); err != nil {
			return fmt.Errorf("delete sqlite rows: %w", err)
		}
	}
	return tx.Commit()
}

// sqliteRecords buffers records and inserts them batchSize at a time inside
// the job's transaction, so memory stays bounded for large jobs. ctx is the
// job's context, kept because write is called per record without one.
type sqliteRecords struct {
	ctx     context.Context
	tx      *sql.Tx
	jobID   string
	format  formatter
	pending [][]any
	rows    int64
}

var recordColumns = []string{"job_id", "source", "record_index", "data"}

func (w *sqliteRecords) write(rec record.Record) error {
	data, err := json.Marshal(w.format.fields(rec))
	if err != nil {
		return fmt.Errorf("sqlite record: %w", err)
	}
	w.pending = append(w.pending, []any{w.jobID, rec.Source, rec.Index, string(data)})
	w.rows++
	if len(w.pending) >= batchSize {
		return w.flush(w.ctx)
	}
	return nil
}

func (w *sqliteRecords) flush(ctx context.Context) error {
	if len(w.pending) == 0 {
		return nil
	}
	err := insertRows(ctx, w.tx, "job_records", recordColumns, w.pending)
	w.pending = w.pending[:0]
	return err
}

func (w *sqliteRecords) prepare(ctx context.Context, _ []job.AggregateResult) error {
	return w.flush(ctx)
}

func (w *sqliteRecords) publish() (job.Output, error) {
	return job.Output{Type: "sqlite", Content: "records", Location: "job_records", Rows: w.rows}, nil
}

func (w *sqliteRecords) abort() { w.pending = nil }

// sqliteAggregates inserts one row per group when the job finishes.
type sqliteAggregates struct {
	tx    *sql.Tx
	jobID string
	rows  int64
}

var aggregateColumns = []string{"job_id", "aggregation", "group_key", "metrics"}

func (w *sqliteAggregates) write(record.Record) error { return nil }

func (w *sqliteAggregates) prepare(ctx context.Context, aggs []job.AggregateResult) error {
	var rows [][]any
	for _, a := range aggs {
		for _, g := range a.Groups {
			key, err := json.Marshal(g.Key)
			if err != nil {
				return err
			}
			metrics, err := json.Marshal(g.Metrics)
			if err != nil {
				return err
			}
			rows = append(rows, []any{w.jobID, a.Name, string(key), string(metrics)})
		}
	}
	w.rows = int64(len(rows))
	return insertRows(ctx, w.tx, "job_aggregates", aggregateColumns, rows)
}

func (w *sqliteAggregates) publish() (job.Output, error) {
	return job.Output{Type: "sqlite", Content: "aggregates", Location: "job_aggregates", Rows: w.rows}, nil
}

func (w *sqliteAggregates) abort() {}
