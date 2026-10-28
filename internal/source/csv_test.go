package source

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
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
			_, err := readAll(t, NewCSV("test", writeFile(t, "in.csv", tc.content)))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCSVMissingFile(t *testing.T) {
	_, err := readAll(t, NewCSV("test", "does/not/exist.csv"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want fs.ErrNotExist", err)
	}
}

func TestCSVHeaderOnly(t *testing.T) {
	items, err := readAll(t, NewCSV("test", writeFile(t, "in.csv", "id,amount\n")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("got %d items, want 0", len(items))
	}
}