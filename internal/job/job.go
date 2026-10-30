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
	Spec       Spec       `json:"spec"`
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
