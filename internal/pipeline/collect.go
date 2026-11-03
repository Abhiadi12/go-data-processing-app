package pipeline

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

// Counters are updated by many goroutines at once, so every field is atomic.
type Counters struct {
	Read         atomic.Int64
	Valid        atomic.Int64
	Failed       atomic.Int64
	SourceErrors atomic.Int64

	Validate  StageTimer
	Transform StageTimer
	Sink      StageTimer
}

func (c *Counters) Snapshot() job.Progress {
	return job.Progress{
		RecordsRead:   c.Read.Load(),
		RecordsValid:  c.Valid.Load(),
		RecordsFailed: c.Failed.Load(),
		SourceErrors:  c.SourceErrors.Load(),
	}
}

// Stages returns the per-record latency of each stage. "sink" is the
// aggregate + export step at the end of the pipeline.
func (c *Counters) Stages() []job.StageStat {
	return []job.StageStat{
		c.Validate.stat("validate"),
		c.Transform.stat("transform"),
		c.Sink.stat("sink"),
	}
}

// StageTimer records how long a stage spends per record. Many workers call
// Observe at once, so it uses atomics; the maximum is updated with a
// compare-and-swap loop.
type StageTimer struct {
	count   atomic.Int64
	totalNs atomic.Int64
	maxNs   atomic.Int64
}

func (s *StageTimer) Observe(d time.Duration) {
	ns := d.Nanoseconds()
	s.count.Add(1)
	s.totalNs.Add(ns)
	for {
		cur := s.maxNs.Load()
		if ns <= cur || s.maxNs.CompareAndSwap(cur, ns) {
			return
		}
	}
}

func (s *StageTimer) stat(name string) job.StageStat {
	st := job.StageStat{Stage: name, Records: s.count.Load(), MaxMicros: micros(s.maxNs.Load())}
	if st.Records > 0 {
		st.AvgMicros = micros(s.totalNs.Load() / st.Records)
	}
	return st
}

func micros(ns int64) float64 {
	return math.Round(float64(ns)/10) / 100
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
