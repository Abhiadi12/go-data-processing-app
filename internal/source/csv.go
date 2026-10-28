package source

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	ErrNoHeader  = errors.New("csv has no header row")
	ErrBadHeader = errors.New("invalid csv header")
)

var _ Source = (*CSVSource)(nil)

type CSVSource struct {
	name string
	path string
}

func NewCSV(name, path string) *CSVSource {
	return &CSVSource{name: name, path: path}
}

func (s *CSVSource) Name() string {
	return s.name
}

func (s *CSVSource) Read(ctx context.Context, emit Emit) error {
	f, err := os.Open(s.path)
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

	return nil
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