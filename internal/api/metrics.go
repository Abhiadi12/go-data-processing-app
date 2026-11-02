package api

import (
	"errors"
	"net/http"
	"runtime"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/metrics"
)

func (s *server) handleJobProgress(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case err != nil:
		s.logger.ErrorContext(r.Context(), "get job progress", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read the job")
	default:
		writeJSON(w, http.StatusOK, metrics.ForJob(j, time.Now()))
	}
}

type metricsResponse struct {
	metrics.Summary
	UptimeSeconds float64 `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.List(r.Context())
	if err != nil {
		s.logger.ErrorContext(r.Context(), "list jobs for metrics", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read jobs")
		return
	}
	now := time.Now()
	writeJSON(w, http.StatusOK, metricsResponse{
		Summary:       metrics.Summarize(jobs, now),
		UptimeSeconds: now.Sub(s.started).Round(time.Second).Seconds(),
		Goroutines:    runtime.NumGoroutine(),
	})
}
