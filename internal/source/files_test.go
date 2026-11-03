package source

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestFileSourcesCannotEscapeRoot(t *testing.T) {
	outer := t.TempDir()
	if err := os.WriteFile(filepath.Join(outer, "secret.csv"), []byte("id\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(outer, "data")
	if err := os.Mkdir(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outer, "secret.csv"), filepath.Join(dataDir, "link.csv")); err != nil {
		t.Fatal(err)
	}

	root, err := os.OpenRoot(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	for _, path := range []string{"../secret.csv", "link.csv", "/etc/passwd"} {
		t.Run(path, func(t *testing.T) {
			if _, err := readAll(t, NewCSV("test", root.FS(), path)); err == nil {
				t.Fatalf("reading %q outside the root succeeded", path)
			}
		})
	}
}

func TestJSONRecordsPath(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		wantItems int
		wantErr   error
	}{
		{"array under key", `{"info":{"page":1},"results":[{"id":1},{"id":2}],"more":true}`, 2, nil},
		{"key is first", `{"results":[{"id":1}]}`, 1, nil},
		{"key missing", `{"data":[{"id":1}]}`, 0, ErrRecordsPathKey},
		{"document is an array", `[{"id":1}]`, 0, ErrRecordsPathKey},
		{"value is not an array", `{"results":{"id":1}}`, 0, ErrNotArray},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := fstest.MapFS{"in.json": {Data: []byte(tc.content)}}
			items, err := readAll(t, NewJSON("api", files, "in.json", "results"))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if len(items) != tc.wantItems {
				t.Fatalf("got %d items, want %d", len(items), tc.wantItems)
			}
		})
	}
}
