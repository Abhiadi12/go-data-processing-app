package source

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

func TestRowError(t *testing.T) {
	err := fmt.Errorf("validate: %w", &RowError{Source: "orders-csv", Index: 3, Err: record.ErrNotNumber})

	if got, want := err.Error(), "validate: orders-csv row 3: not a number"; got != want {
		t.Errorf("message = %q, want %q", got, want)
	}

	if !errors.Is(err, record.ErrNotNumber) {
		t.Error("errors.Is should find ErrNotNumber inside RowError")
	}

	var rowErr *RowError
	if !errors.As(err, &rowErr) {
		t.Fatal("errors.As should find a *RowError")
	}
	if rowErr.Source != "orders-csv" || rowErr.Index != 3 {
		t.Errorf("got source %q index %d", rowErr.Source, rowErr.Index)
	}
}
