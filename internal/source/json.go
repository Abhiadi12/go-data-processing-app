package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"

	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

var (
	ErrNotArray       = errors.New("json input is not an array")
	ErrNotObject      = errors.New("json element is not an object")
	ErrTrailingData   = errors.New("unexpected data after the json array")
	ErrRecordsPathKey = errors.New("records_path key not found")
)

var _ Source = (*JSONSource)(nil)

type JSONSource struct {
	name        string
	files       fs.FS
	path        string
	recordsPath string
}

// NewJSON reads path from files. recordsPath names the top-level key holding
// the array when the document is an object; empty means the document is the array.
func NewJSON(name string, files fs.FS, path, recordsPath string) *JSONSource {
	return &JSONSource{name: name, files: files, path: path, recordsPath: recordsPath}
}

func (s *JSONSource) Name() string {
	return s.name
}

func (s *JSONSource) Read(ctx context.Context, emit Emit) error {
	f, err := s.files.Open(s.path)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.name, err)
	}
	defer f.Close()

	return readJSON(ctx, s.name, f, s.recordsPath, emit)
}

// readJSON streams a JSON array one element at a time. It takes an io.Reader
// so the file and HTTP sources share it.
func readJSON(ctx context.Context, name string, r io.Reader, recordsPath string, emit Emit) error {
	dec := json.NewDecoder(r)

	if recordsPath != "" {
		if err := seekKey(dec, recordsPath); err != nil {
			return fmt.Errorf("source %s: %w", name, err)
		}
		return readArray(ctx, name, dec, emit)
	}

	if err := readArray(ctx, name, dec, emit); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("source %s: %w", name, ErrTrailingData)
	}
	return nil
}

// seekKey moves dec to the value of key in a top-level object, skipping
// the values of earlier keys.
func seekKey(dec *json.Decoder, key string) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("%w: %q: %w", ErrRecordsPathKey, key, err)
	}
	if tok != json.Delim('{') {
		return fmt.Errorf("%w: %q: document is not an object", ErrRecordsPathKey, key)
	}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if tok == key {
			return nil
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: %q", ErrRecordsPathKey, key)
}

func readArray(ctx context.Context, name string, dec *json.Decoder, emit Emit) error {
	tok, err := dec.Token()
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("source %s: %w: input is empty", name, ErrNotArray)
	}
	if err != nil {
		return fmt.Errorf("source %s: %w", name, err)
	}
	if tok != json.Delim('[') {
		return fmt.Errorf("source %s: %w: starts with %v", name, ErrNotArray, tok)
	}

	for index := 1; dec.More(); index++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("source %s: %w", name, err)
		}

		var v any
		if err := dec.Decode(&v); err != nil {
			return fmt.Errorf("source %s: element %d: %w", name, index, err)
		}

		var item Item
		if obj, ok := v.(map[string]any); ok {
			item.Record = record.Record{Source: name, Index: index, Fields: obj}
		} else {
			item.Err = &RowError{Source: name, Index: index, Err: fmt.Errorf("%w: got %s", ErrNotObject, jsonKind(v))}
		}

		if err := emit(item); err != nil {
			return err
		}
	}

	if _, err := dec.Token(); err != nil {
		return fmt.Errorf("source %s: %w", name, err)
	}
	return nil
}

func jsonKind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	default:
		return fmt.Sprintf("%T", v)
	}
}
