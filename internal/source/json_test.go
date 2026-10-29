package source

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"testing"
)

func TestJSONFatalErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr error
	}{
		{"empty file", "", ErrNotArray},
		{"only whitespace", " \n\t", ErrNotArray},
		{"object instead of array", `{"id":1}`, ErrNotArray},
		{"number instead of array", `42`, ErrNotArray},
		{"second array after the first", `[{"id":1}] [{"id":2}]`, ErrTrailingData},
		{"garbage after the array", `[{"id":1}] oops`, ErrTrailingData},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readAll(t, NewJSON("test", writeFile(t, "in.json", tc.content)))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestJSONMissingFile(t *testing.T) {
	_, err := readAll(t, NewJSON("test", "does/not/exist.json"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestJSONEmptyArray(t *testing.T) {
	items, err := readAll(t, NewJSON("test", writeFile(t, "in.json", " [ ] \n")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("got %d items, want 0", len(items))
	}
}

func TestJSONReadsElements(t *testing.T) {
	content := `[
		{"id": 1, "name": "Asha", "tags": ["a"], "meta": {"vip": true}},
		5,
		null,
		"text",
		{"id": 2}
	]`

	items, err := readAll(t, NewJSON("orders", writeFile(t, "in.json", content)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("got %d items, want 5", len(items))
	}

	first := map[string]any{
		"id":   float64(1),
		"name": "Asha",
		"tags": []any{"a"},
		"meta": map[string]any{"vip": true},
	}
	if items[0].Err != nil || !reflect.DeepEqual(items[0].Record.Fields, first) {
		t.Errorf("item 0 = %+v, want fields %v", items[0], first)
	}

	for i := 1; i <= 3; i++ {
		rowErr := items[i].Err
		if rowErr == nil || rowErr.Index != i+1 || !errors.Is(rowErr, ErrNotObject) {
			t.Errorf("item %d: want ErrNotObject at index %d, got %v", i, i+1, rowErr)
		}
	}

	last := items[4]
	if last.Err != nil || last.Record.Index != 5 || last.Record.Fields["id"] != float64(2) {
		t.Errorf("item 4: reading should continue after bad elements, got %+v", last)
	}
}

func TestJSONSyntaxErrorStopsReading(t *testing.T) {
	content := `[{"id": 1}, {"id": oops}, {"id": 3}]`

	items, err := readAll(t, NewJSON("orders", writeFile(t, "in.json", content)))

	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("err = %v, want a *json.SyntaxError", err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d items before the error, want 1", len(items))
	}
}

func TestJSONTruncatedInput(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"array never closed", `[{"id":1}`},
		{"object cut in half", `[{"id":1},{"id":`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items, err := readAll(t, NewJSON("test", writeFile(t, "in.json", tc.content)))
			if err == nil {
				t.Fatal("want an error for truncated input, got nil")
			}
			if len(items) != 1 {
				t.Fatalf("got %d items before the error, want 1", len(items))
			}
		})
	}
}

func TestJSONStopsWhenCancelled(t *testing.T) {
	path := writeFile(t, "in.json", `[{"id":1},{"id":2},{"id":3}]`)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got int
	err := NewJSON("orders", path).Read(ctx, func(Item) error {
		got++
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got != 1 {
		t.Fatalf("emitted %d items, want 1", got)
	}
}

func TestJSONStopsWhenEmitFails(t *testing.T) {
	path := writeFile(t, "in.json", `[{"id":1},{"id":2},{"id":3}]`)
	errStop := errors.New("stop")

	var got int
	err := NewJSON("orders", path).Read(context.Background(), func(Item) error {
		got++
		return errStop
	})

	if !errors.Is(err, errStop) {
		t.Fatalf("err = %v, want errStop", err)
	}
	if got != 1 {
		t.Fatalf("emitted %d items, want 1", got)
	}
}

func TestJSONSampleFile(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "orders.json")
	items, err := readAll(t, NewJSON("orders-json", path))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("got %d items, want 5", len(items))
	}
	for i, it := range items {
		if it.Err != nil {
			t.Errorf("element %d: unexpected row error: %v", i+1, it.Err)
		}
		if it.Record.Source != "orders-json" || it.Record.Index != i+1 {
			t.Errorf("element %d: source %q index %d", i+1, it.Record.Source, it.Record.Index)
		}
	}

	tests := []struct {
		item  int
		field string
		want  any
	}{
		{0, "order_id", float64(2001)},
		{0, "amount", float64(320)},
		{1, "amount", "12.00"},
		{2, "amount", nil},
		{3, "quantity", 2.5},
		{4, "coupon", "DIWALI"},
	}
	for _, tc := range tests {
		got, ok := items[tc.item].Record.Get(tc.field)
		if !ok || got != tc.want {
			t.Errorf("element %d %s = %#v (present: %v), want %#v", tc.item+1, tc.field, got, ok, tc.want)
		}
	}
}
