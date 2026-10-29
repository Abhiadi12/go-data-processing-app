package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

var (
	ErrNotArray     = errors.New("json input is not an array")
	ErrNotObject    = errors.New("json element is not an object")
	ErrTrailingData = errors.New("unexpected data after the json array")
)

var _ Source = (*JSONSource)(nil)

type JSONSource struct {
	name string
	path string
}

func NewJSON(name, path string) *JSONSource {
	return &JSONSource{name: name, path: path}
}

func (s *JSONSource) Name() string {
	return s.name
}

func (s *JSONSource) Read(ctx context.Context, emit Emit) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("source %s: %w", s.name, err)
	}
	defer f.Close()

	return readJSONArray(ctx, s.name, f, emit)
}

// readJSONArray streams a top-level JSON array one element at a time.
// It takes an io.Reader so the HTTP source can reuse it for response bodies.
func readJSONArray(ctx context.Context, name string, r io.Reader, emit Emit) error {
	dec := json.NewDecoder(r)

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
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("source %s: %w", name, ErrTrailingData)
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
