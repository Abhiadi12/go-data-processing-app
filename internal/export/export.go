package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

var (
	ErrNoOutputDir = errors.New("output directory is not configured")
	ErrNoDatabase  = errors.New("sqlite database is not configured")
)

type Deps struct {
	OutputDir string
	DB        *sql.DB
}

// writer is one export entry of a job. Records writers stream; aggregates
// writers do their work in prepare.
type writer interface {
	write(rec record.Record) error
	prepare(ctx context.Context, aggs []job.AggregateResult) error
	publish() (job.Output, error)
	abort()
}

type Set struct {
	dir     string
	writers []writer
	tx      *sql.Tx
	format  formatter
}

func Open(ctx context.Context, j job.Job, deps Deps) (*Set, error) {
	s := &Set{format: newFormatter(j.Spec.Schema)}
	columns := j.Spec.OutputFields()

	for _, e := range j.Spec.Exports {
		w, err := s.newWriter(ctx, j.ID, e, columns, deps)
		if err != nil {
			s.Abort()
			return nil, fmt.Errorf("export %s/%s: %w", e.Type, e.Content, err)
		}
		s.writers = append(s.writers, w)
	}
	return s, nil
}

func (s *Set) newWriter(ctx context.Context, jobID string, e job.Export, columns []string, deps Deps) (writer, error) {
	if e.Type == "sqlite" {
		if deps.DB == nil {
			return nil, ErrNoDatabase
		}
		if s.tx == nil {
			tx, err := deps.DB.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}
			s.tx = tx
		}
		if e.Content == "records" {
			return &sqliteRecords{ctx: ctx, tx: s.tx, jobID: jobID, format: s.format}, nil
		}
		return &sqliteAggregates{tx: s.tx, jobID: jobID}, nil
	}

	if err := s.ensureDir(deps.OutputDir, jobID); err != nil {
		return nil, err
	}
	switch {
	case e.Type == "json" && e.Content == "records":
		return newJSONRecords(s.dir, s.format)
	case e.Type == "csv" && e.Content == "records":
		return newCSVRecords(s.dir, columns, s.format)
	case e.Type == "json":
		return &jsonAggregates{dir: s.dir}, nil
	default:
		return &csvAggregates{dir: s.dir}, nil
	}
}

func (s *Set) ensureDir(root, jobID string) error {
	if s.dir != "" {
		return nil
	}
	if root == "" {
		return ErrNoOutputDir
	}
	if !filepath.IsLocal(jobID) {
		return fmt.Errorf("invalid job id %q", jobID)
	}
	dir := filepath.Join(root, jobID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	s.dir = dir
	return nil
}

func (s *Set) WriteRecord(rec record.Record) error {
	for _, w := range s.writers {
		if err := w.write(rec); err != nil {
			return err
		}
	}
	return nil
}

func (s *Set) Commit(ctx context.Context, aggs []job.AggregateResult) ([]job.Output, error) {
	for _, w := range s.writers {
		if err := w.prepare(ctx, aggs); err != nil {
			s.Abort()
			return nil, err
		}
	}
	if s.tx != nil {
		if err := s.tx.Commit(); err != nil {
			s.tx = nil
			s.Abort()
			return nil, fmt.Errorf("commit sqlite: %w", err)
		}
		s.tx = nil
	}

	outputs := make([]job.Output, 0, len(s.writers))
	for _, w := range s.writers {
		out, err := w.publish()
		if err != nil {
			s.Abort()
			return nil, err
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

func (s *Set) Abort() {
	for _, w := range s.writers {
		w.abort()
	}
	if s.tx != nil {
		_ = s.tx.Rollback()
		s.tx = nil
	}
	if s.dir != "" {
		_ = os.Remove(s.dir) // only succeeds when the directory is empty
	}
}

func Remove(ctx context.Context, j job.Job, deps Deps) error {
	if j.Status == job.StatusCompleted && usesSQLite(j.Spec.Exports) {
		if deps.DB == nil {
			return ErrNoDatabase
		}
		if err := deleteRows(ctx, deps.DB, j.ID); err != nil {
			return err
		}
	}
	if deps.OutputDir == "" {
		return nil
	}
	if !filepath.IsLocal(j.ID) {
		return fmt.Errorf("invalid job id %q", j.ID)
	}
	return os.RemoveAll(filepath.Join(deps.OutputDir, j.ID))
}

func usesSQLite(exps []job.Export) bool {
	for _, e := range exps {
		if e.Type == "sqlite" {
			return true
		}
	}
	return false
}

// formatter turns record values into plain values for JSON, CSV and SQLite.
// Dates are written in the layout their schema field declares.
type formatter struct {
	dateLayouts map[string]string
}

func newFormatter(s job.Schema) formatter {
	f := formatter{dateLayouts: make(map[string]string)}
	for _, field := range s.Fields {
		if field.Type == "date" {
			f.dateLayouts[field.Name] = field.Format
		}
	}
	return f
}

func (f formatter) value(field string, v any) any {
	if t, ok := v.(time.Time); ok {
		layout := f.dateLayouts[field]
		if layout == "" {
			layout = time.RFC3339
		}
		return t.Format(layout)
	}
	return v
}

func (f formatter) fields(rec record.Record) map[string]any {
	out := make(map[string]any, len(rec.Fields))
	for k, v := range rec.Fields {
		out[k] = f.value(k, v)
	}
	return out
}

func (f formatter) text(field string, v any) string {
	switch x := f.value(field, v).(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		return strconv.FormatBool(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x)
		}
		return string(b)
	}
}
