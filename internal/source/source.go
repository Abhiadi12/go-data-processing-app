package source

import (
	"context"
	"fmt"

	"github.com/Abhiadi12/go-data-processing-app/internal/record"
)

type Source interface {
	Name() string
	Read(ctx context.Context, emit Emit) error
}

type Emit func(Item) error

type Item struct {
	Record record.Record
	Err    *RowError
}

type RowError struct {
	Source string
	Index  int
	Err    error
}

func (e *RowError) Error() string {
	return fmt.Sprintf("%s row %d: %v", e.Source, e.Index, e.Err)
}

func (e *RowError) Unwrap() error {
	return e.Err
}
