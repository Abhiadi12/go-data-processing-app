package job

import (
	"crypto/rand"
	"time"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

type Job struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Status     Status     `json:"status"`
	RequestID  string     `json:"request_id,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Progress   Progress   `json:"progress"`
	Spec       Spec       `json:"spec"`

	// FailedRecords holds the first options.max_stored_errors failures;
	// Progress.RecordsFailed is the full count. Served by GET .../errors.
	FailedRecords []RecordError `json:"-"`

	Aggregates []AggregateResult `json:"-"`
}

type AggregateResult struct {
	Name    string        `json:"name"`
	GroupBy []string      `json:"group_by"`
	Groups  []GroupResult `json:"groups"`
}

type GroupResult struct {
	Key     map[string]any     `json:"key"`
	Metrics map[string]float64 `json:"metrics"`
}

// Finished reports whether the job reached a final status.
func (j Job) Finished() bool {
	return j.Status == StatusCompleted || j.Status == StatusFailed || j.Status == StatusCancelled
}

// Progress counts records as they move through the pipeline.
type Progress struct {
	RecordsRead   int64 `json:"records_read"`
	RecordsValid  int64 `json:"records_valid"`
	RecordsFailed int64 `json:"records_failed"`
	SourceErrors  int64 `json:"source_errors"`
}

// RecordError describes one record that was dropped, or one source that failed (Index 0).
type RecordError struct {
	Source  string         `json:"source"`
	Index   int            `json:"index,omitempty"`
	Stage   string         `json:"stage"`
	Reasons []string       `json:"reasons"`
	Record  map[string]any `json:"record,omitempty"`
}

// New returns a pending job for a spec that has already been validated.
func New(spec Spec, requestID string) Job {
	return Job{
		ID:        rand.Text(),
		Name:      spec.Name,
		Status:    StatusPending,
		RequestID: requestID,
		CreatedAt: time.Now().UTC(),
		Spec:      spec,
	}
}
