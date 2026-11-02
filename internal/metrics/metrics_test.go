package metrics

import (
	"reflect"
	"testing"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

var t0 = time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)

func at(sec int) *time.Time {
	t := t0.Add(time.Duration(sec) * time.Second)
	return &t
}

func TestForJob(t *testing.T) {
	tests := []struct {
		name        string
		job         job.Job
		wantElapsed float64
		wantRate    float64
		wantPercent *float64
	}{
		{"pending", job.Job{Status: job.StatusPending}, 0, 0, ptr(0)},
		{"running", job.Job{Status: job.StatusRunning, StartedAt: at(0), Progress: job.Progress{RecordsRead: 500}}, 4, 125, nil},
		{"completed", job.Job{Status: job.StatusCompleted, StartedAt: at(0), FinishedAt: at(2), Progress: job.Progress{RecordsRead: 15}}, 2, 7.5, ptr(100)},
		{"cancelled", job.Job{Status: job.StatusCancelled, StartedAt: at(0), FinishedAt: at(1), Progress: job.Progress{RecordsRead: 3}}, 1, 3, nil},
		{"cancelled before start", job.Job{Status: job.StatusCancelled, FinishedAt: at(1)}, 0, 0, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := ForJob(tc.job, t0.Add(4*time.Second))
			if p.ElapsedSeconds != tc.wantElapsed || p.RecordsPerSecond != tc.wantRate || !reflect.DeepEqual(p.PercentComplete, tc.wantPercent) {
				t.Errorf("elapsed %v rate %v percent %v; want %v %v %v",
					p.ElapsedSeconds, p.RecordsPerSecond, p.PercentComplete, tc.wantElapsed, tc.wantRate, tc.wantPercent)
			}
			if p.Stages == nil {
				t.Error("stages should be an empty list, not null")
			}
		})
	}
}

func TestSummarize(t *testing.T) {
	jobs := []job.Job{
		{
			Status: job.StatusCompleted, StartedAt: at(0), FinishedAt: at(2),
			Progress: job.Progress{RecordsRead: 15, RecordsValid: 7, RecordsFailed: 8},
			Stages:   []job.StageStat{{Stage: "validate", Records: 15, AvgMicros: 2, MaxMicros: 10}},
		},
		{
			Status: job.StatusFailed, StartedAt: at(0), FinishedAt: at(3),
			Progress: job.Progress{RecordsRead: 5, RecordsValid: 4, SourceErrors: 1},
			Stages:   []job.StageStat{{Stage: "validate", Records: 5, AvgMicros: 6, MaxMicros: 40}},
		},
		{Status: job.StatusPending},
	}

	s := Summarize(jobs, t0.Add(10*time.Second))

	if s.Jobs[job.StatusCompleted] != 1 || s.Jobs[job.StatusFailed] != 1 || s.Jobs[job.StatusPending] != 1 || s.Jobs[job.StatusRunning] != 0 {
		t.Errorf("jobs = %v", s.Jobs)
	}
	if want := (job.Progress{RecordsRead: 20, RecordsValid: 11, RecordsFailed: 8, SourceErrors: 1}); s.Records != want {
		t.Errorf("records = %+v, want %+v", s.Records, want)
	}
	if s.ErrorRate != 0.45 || s.RecordsPerSecond != 4 {
		t.Errorf("error rate %v (want 0.45), rate %v (want 20 records / 5 s = 4)", s.ErrorRate, s.RecordsPerSecond)
	}
	want := []job.StageStat{{Stage: "validate", Records: 20, AvgMicros: 3, MaxMicros: 40}}
	if !reflect.DeepEqual(s.Stages, want) {
		t.Errorf("stages = %+v, want %+v (avg weighted by records: (15×2 + 5×6) / 20)", s.Stages, want)
	}
}

func TestSummarizeEmpty(t *testing.T) {
	s := Summarize(nil, t0)
	if len(s.Jobs) != 5 || s.ErrorRate != 0 || s.RecordsPerSecond != 0 || s.Stages == nil {
		t.Errorf("empty summary = %+v", s)
	}
}
