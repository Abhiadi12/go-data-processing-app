package source

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

var (
	ErrNoHeader      = errors.New("csv has no header row")
	ErrBadHeader     = errors.New("invalid csv header")
	ErrTooManyFields = errors.New("row has more fields than the header")
)

var _ Source = (*CSVSource)(nil)

type CSVSource struct {
	name  string
	files fs.FS
	path  string
}

// NewCSV reads path from files. In production files is os.Root.FS() for DATA_DIR,
// so a path cannot escape the data directory.
func NewCSV(name string, files fs.FS, path string) *CSVSource {
	return &CSVSource{name: name, files: files, path: path}
}

func (s *CSVSource) Name() string {
	return s.name
}

func (s *CSVSource) Read(ctx context.Context, emit Emit) error {
	f, err := s.files.Open(s.path)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.name, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("source %s: %w", s.name, ErrNoHeader)
	}
	if err != nil {
		return fmt.Errorf("source %s: read header: %w", s.name, err)
	}

	header, err = cleanHeader(header)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.name, err)
	}

	return s.readRows(ctx, r, header, emit)
}

func (s *CSVSource) readRows(ctx context.Context, r *csv.Reader, header []string, emit Emit) error {
	for index := 1; ; index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("source %s: %w", s.name, err)
		}

		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}

		var item Item
		var parseErr *csv.ParseError
		switch {
		case errors.As(err, &parseErr):
			item.Err = &RowError{Source: s.name, Index: index, Err: err}
		case err != nil:
			return fmt.Errorf("source %s: row %d: %w", s.name, index, err)
		case len(row) > len(header):
			item.Err = &RowError{Source: s.name, Index: index,
				Err: fmt.Errorf("%w: got %d, want %d", ErrTooManyFields, len(row), len(header))}
		default:
			item.Record = toRecord(s.name, index, header, row)
		}

		if err := emit(item); err != nil {
			return err
		}
	}
}

func toRecord(source string, index int, header, row []string) record.Record {
	rec := record.New(source, index)
	for i, value := range row {
		rec.Fields[header[i]] = value
	}
	return rec
}

func cleanHeader(header []string) ([]string, error) {
	seen := make(map[string]bool, len(header))
	out := make([]string, len(header))
	for i, h := range header {
		if i == 0 {
			h = strings.TrimPrefix(h, "\ufeff")
		}
		h = strings.TrimSpace(h)
		if h == "" {
			return nil, fmt.Errorf("%w: column %d has no name", ErrBadHeader, i+1)
		}
		if seen[h] {
			return nil, fmt.Errorf("%w: duplicate column %q", ErrBadHeader, h)
		}
		seen[h] = true
		out[i] = h
	}
	return out, nil
}
