package pipeline

import (
	"sync"
	"sync/atomic"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

// Counters are updated by many goroutines at once, so every field is atomic.
type Counters struct {
	Read         atomic.Int64
	Valid        atomic.Int64
	Failed       atomic.Int64
	SourceErrors atomic.Int64
}

func (c *Counters) Snapshot() job.Progress {
	return job.Progress{
		RecordsRead:   c.Read.Load(),
		RecordsValid:  c.Valid.Load(),
		RecordsFailed: c.Failed.Load(),
		SourceErrors:  c.SourceErrors.Load(),
	}
}

// Errors keeps the first max failures and counts all of them, so a file full
// of bad rows cannot exhaust memory. It is safe for concurrent use.
type Errors struct {
	mu    sync.Mutex
	max   int
	items []job.RecordError
	total int64
}

func NewErrors(max int) *Errors {
	return &Errors{max: max}
}

func (e *Errors) Add(re job.RecordError) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.total++
	if len(e.items) < e.max {
		e.items = append(e.items, re)
	}
}

// Snapshot returns a copy of the stored failures and the total count.
func (e *Errors) Snapshot() ([]job.RecordError, int64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	out := make([]job.RecordError, len(e.items))
	copy(out, e.items)
	return out, e.total
}
