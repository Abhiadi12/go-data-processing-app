// Package metrics turns job state into the numbers the API reports: per-job
// progress (rate, percent) and a service-wide summary. Everything is computed
// from the job store on request, so there is no extra state to keep in sync.
package metrics

import (
	"math"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

// Progress is one job's progress, as served by GET /pipelines/{id}/progress.
type Progress struct {
	JobID            string          `json:"job_id"`
	Status           job.Status      `json:"status"`
	Counts           job.Progress    `json:"counts"`
	ElapsedSeconds   float64         `json:"elapsed_seconds"`
	RecordsPerSecond float64         `json:"records_per_second"`
	PercentComplete  *float64        `json:"percent_complete"`
	Stages           []job.StageStat `json:"stages"`
}

// ForJob reports a job's progress at time now.
//
// percent_complete: sources are streamed, so the total number of records is
// not known while a job runs. It is 0 while pending, 100 once completed, and
// null (unknown) while running or after a failure or cancel.
func ForJob(j job.Job, now time.Time) Progress {
	p := Progress{JobID: j.ID, Status: j.Status, Counts: j.Progress, Stages: j.Stages}
	if p.Stages == nil {
		p.Stages = []job.StageStat{}
	}

	if j.StartedAt != nil {
		end := now
		if j.FinishedAt != nil {
			end = *j.FinishedAt
		}
		p.ElapsedSeconds = round2(end.Sub(*j.StartedAt).Seconds())
		if secs := end.Sub(*j.StartedAt).Seconds(); secs > 0 {
			p.RecordsPerSecond = round2(float64(j.Progress.RecordsRead) / secs)
		}
	}

	switch j.Status {
	case job.StatusPending:
		p.PercentComplete = ptr(0)
	case job.StatusCompleted:
		p.PercentComplete = ptr(100)
	}
	return p
}

// Summary is the service-wide view served by GET /api/v1/metrics.
type Summary struct {
	Jobs             map[job.Status]int `json:"jobs"`
	Records          job.Progress       `json:"records"`
	ErrorRate        float64            `json:"error_rate"`
	RecordsPerSecond float64            `json:"records_per_second"`
	Stages           []job.StageStat    `json:"stages"`
}

// Summarize combines all jobs:
//   - jobs: count per status (every status is listed, even when 0)
//   - records: totals over all jobs
//   - error_rate: (failed records + source errors) / records read
//   - records_per_second: records read / time spent running, over jobs that started
//   - stages: per-stage latency over all jobs (average weighted by records, max of maxes)
func Summarize(jobs []job.Job, now time.Time) Summary {
	s := Summary{Jobs: map[job.Status]int{
		job.StatusPending: 0, job.StatusRunning: 0, job.StatusCompleted: 0, job.StatusFailed: 0, job.StatusCancelled: 0,
	}}

	var runSeconds float64
	stages := map[string]*job.StageStat{}
	var order []string
	var weighted = map[string]float64{}

	for _, j := range jobs {
		s.Jobs[j.Status]++
		s.Records.RecordsRead += j.Progress.RecordsRead
		s.Records.RecordsValid += j.Progress.RecordsValid
		s.Records.RecordsFailed += j.Progress.RecordsFailed
		s.Records.SourceErrors += j.Progress.SourceErrors

		if j.StartedAt != nil {
			end := now
			if j.FinishedAt != nil {
				end = *j.FinishedAt
			}
			runSeconds += end.Sub(*j.StartedAt).Seconds()
		}

		for _, st := range j.Stages {
			agg, ok := stages[st.Stage]
			if !ok {
				agg = &job.StageStat{Stage: st.Stage}
				stages[st.Stage] = agg
				order = append(order, st.Stage)
			}
			agg.Records += st.Records
			agg.MaxMicros = math.Max(agg.MaxMicros, st.MaxMicros)
			weighted[st.Stage] += st.AvgMicros * float64(st.Records)
		}
	}

	if s.Records.RecordsRead > 0 {
		s.ErrorRate = round4(float64(s.Records.RecordsFailed+s.Records.SourceErrors) / float64(s.Records.RecordsRead))
	}
	if runSeconds > 0 {
		s.RecordsPerSecond = round2(float64(s.Records.RecordsRead) / runSeconds)
	}

	s.Stages = make([]job.StageStat, 0, len(order))
	for _, name := range order {
		st := *stages[name]
		if st.Records > 0 {
			st.AvgMicros = round2(weighted[name] / float64(st.Records))
		}
		s.Stages = append(s.Stages, st)
	}
	return s
}

func ptr(f float64) *float64 { return &f }

func round2(x float64) float64 { return math.Round(x*100) / 100 }

func round4(x float64) float64 { return math.Round(x*10000) / 10000 }
