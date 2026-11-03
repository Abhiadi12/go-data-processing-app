package source

import (
	"context"
	"encoding/csv"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/fstest"
)

func newCSV(name, content string) *CSVSource {
	return NewCSV(name, fstest.MapFS{"in.csv": {Data: []byte(content)}}, "in.csv")
}

func newJSON(name, content string) *JSONSource {
	return NewJSON(name, fstest.MapFS{"in.json": {Data: []byte(content)}}, "in.json", "")
}

func readAll(t *testing.T, src Source) ([]Item, error) {
	t.Helper()
	var items []Item
	err := src.Read(context.Background(), func(it Item) error {
		items = append(items, it)
		return nil
	})
	return items, err
}

func TestCSVHeaderErrors(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr error
	}{
		{"empty file", "", ErrNoHeader},
		{"only blank lines", "\n\n", ErrNoHeader},
		{"empty column name", "id,,amount\n", ErrBadHeader},
		{"blank column name", "id, ,amount\n", ErrBadHeader},
		{"duplicate column", "id,amount,amount\n", ErrBadHeader},
		{"duplicate after trim", "id,amount, amount \n", ErrBadHeader},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readAll(t, newCSV("test", tc.content))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCSVMissingFile(t *testing.T) {
	_, err := readAll(t, NewCSV("test", fstest.MapFS{}, "does/not/exist.csv"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestCSVHeaderOnly(t *testing.T) {
	items, err := readAll(t, newCSV("test", "id,amount\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("got %d items, want 0", len(items))
	}
}
func TestCSVReadsRows(t *testing.T) {
	content := "\ufeffid, name ,amount\n" +
		"1, Asha ,10.5\n" +
		"2,Ravi\n" +
		"3,Mei,7,extra\n" +
		"4,Tom,3\n"

	items, err := readAll(t, newCSV("orders", content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 4 {
		t.Fatalf("got %d items, want 4", len(items))
	}

	want := []map[string]any{
		{"id": "1", "name": " Asha ", "amount": "10.5"},
		{"id": "2", "name": "Ravi"},
		nil,
		{"id": "4", "name": "Tom", "amount": "3"},
	}
	for i, it := range items {
		if i == 2 {
			continue
		}
		if it.Err != nil {
			t.Fatalf("item %d: unexpected row error: %v", i, it.Err)
		}
		if it.Record.Source != "orders" || it.Record.Index != i+1 {
			t.Errorf("item %d: source %q index %d", i, it.Record.Source, it.Record.Index)
		}
		if !reflect.DeepEqual(it.Record.Fields, want[i]) {
			t.Errorf("item %d: fields = %v, want %v", i, it.Record.Fields, want[i])
		}
	}

	rowErr := items[2].Err
	if rowErr == nil {
		t.Fatal("item 2: expected a row error")
	}
	if rowErr.Index != 3 || !errors.Is(rowErr, ErrTooManyFields) {
		t.Errorf("item 2: got %v", rowErr)
	}
}

func TestCSVBadQuoteIsRowError(t *testing.T) {
	content := "id,name\n" +
		"1,\"Asha\"Rao\n" +
		"2,Ravi\n"

	items, err := readAll(t, newCSV("orders", content))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}

	var parseErr *csv.ParseError
	if items[0].Err == nil || !errors.As(items[0].Err, &parseErr) {
		t.Errorf("item 0: want a csv.ParseError row error, got %v", items[0].Err)
	}
	if items[1].Err != nil || items[1].Record.Fields["name"] != "Ravi" {
		t.Errorf("item 1: reading should continue after a bad row, got %+v", items[1])
	}
}

func TestCSVStopsWhenCancelled(t *testing.T) {
	src := newCSV("orders", "id\n1\n2\n3\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got int
	err := src.Read(ctx, func(Item) error {
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

func TestCSVStopsWhenEmitFails(t *testing.T) {
	src := newCSV("orders", "id\n1\n2\n3\n")
	errStop := errors.New("stop")

	var got int
	err := src.Read(context.Background(), func(Item) error {
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

func TestCSVSampleFile(t *testing.T) {
	files := os.DirFS(filepath.Join("..", "..", "testdata"))
	items, err := readAll(t, NewCSV("orders-csv", files, "orders.csv"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 10 {
		t.Fatalf("got %d items, want 10", len(items))
	}

	for i, it := range items {
		if it.Err != nil {
			t.Errorf("row %d: unexpected row error: %v", i+1, it.Err)
		}
		if it.Record.Source != "orders-csv" || it.Record.Index != i+1 {
			t.Errorf("row %d: source %q index %d", i+1, it.Record.Source, it.Record.Index)
		}
	}

	first := map[string]any{
		"order_id": "1001", "customer": "Asha Rao", "country": "IN", "category": "books",
		"amount": "250.50", "quantity": "2", "order_date": "2026-10-01",
	}
	if !reflect.DeepEqual(items[0].Record.Fields, first) {
		t.Errorf("row 1 = %v, want %v", items[0].Record.Fields, first)
	}

	if v, ok := items[3].Record.Get("customer"); !ok || v != "" {
		t.Errorf("row 4 customer = %q (present: %v), want empty but present", v, ok)
	}

	short := items[8].Record
	if len(short.Fields) != 4 {
		t.Errorf("row 9 has %d fields, want 4", len(short.Fields))
	}
	if _, ok := short.Get("quantity"); ok {
		t.Error("row 9 should have no quantity field")
	}
}
