package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
	"github.com/Abhiadi12/go-data-processing-app/internal/runner"
)

func (s *server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		if mt, _, err := mime.ParseMediaType(ct); err != nil || mt != "application/json" {
			writeError(w, http.StatusUnsupportedMediaType, "content type must be application/json")
			return
		}
	}

	var spec job.Spec
	if status, err := decodeJSON(r, &spec); err != nil {
		writeError(w, status, err.Error())
		return
	}

	spec.ApplyDefaults()
	if err := spec.Validate(s.files); err != nil {
		var vErr *job.ValidationError
		if errors.As(err, &vErr) {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid job spec", Details: vErr.Problems})
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	j := job.New(spec, logging.RequestID(r.Context()))
	if err := s.store.Create(r.Context(), j); err != nil {
		s.logger.ErrorContext(r.Context(), "create job", "err", err)
		writeError(w, http.StatusInternalServerError, "could not save the job")
		return
	}

	ctx := logging.WithJobID(r.Context(), j.ID)
	if err := s.runner.Submit(j.ID); err != nil {
		s.reject(ctx, w, j.ID, err)
		return
	}

	s.logger.InfoContext(ctx, "job queued", "name", j.Name, "sources", len(spec.Sources))
	w.Header().Set("Location", "/api/v1/pipelines/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

// reject marks a job that could not be queued as failed and answers 503,
// so the client knows to retry later and the job is not left pending forever.
func (s *server) reject(ctx context.Context, w http.ResponseWriter, id string, err error) {
	msg := "job queue is full, try again later"
	if errors.Is(err, runner.ErrStopped) {
		msg = "server is shutting down"
	}
	s.logger.WarnContext(ctx, "job rejected", "err", err)

	_, uerr := s.store.Update(ctx, id, func(j *job.Job) error {
		now := time.Now().UTC()
		j.Status, j.FinishedAt, j.Error = job.StatusFailed, &now, "rejected: "+err.Error()
		return nil
	})
	if uerr != nil {
		s.logger.ErrorContext(ctx, "mark rejected job", "err", uerr)
	}

	w.Header().Set("Retry-After", "5")
	writeError(w, http.StatusServiceUnavailable, msg)
}

// decodeJSON reads exactly one JSON object into dst, rejecting unknown fields.
// It returns the HTTP status to use when it fails.
func decodeJSON(r *http.Request, dst any) (int, error) {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	err := dec.Decode(dst)
	var maxErr *http.MaxBytesError
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case err == nil:
	case errors.As(err, &maxErr):
		return http.StatusRequestEntityTooLarge, fmt.Errorf("request body must be at most %d bytes", maxErr.Limit)
	case errors.Is(err, io.EOF):
		return http.StatusBadRequest, errors.New("request body is empty")
	case errors.As(err, &syntaxErr):
		return http.StatusBadRequest, fmt.Errorf("malformed json at byte %d", syntaxErr.Offset)
	case errors.Is(err, io.ErrUnexpectedEOF):
		return http.StatusBadRequest, errors.New("malformed json: body ends too early")
	case errors.As(err, &typeErr):
		return http.StatusBadRequest, fmt.Errorf("field %q must be a %s", typeErr.Field, typeErr.Type)
	default:
		return http.StatusBadRequest, fmt.Errorf("invalid json: %w", err)
	}

	if dec.More() {
		return http.StatusBadRequest, errors.New("request body must contain a single json object")
	}
	return 0, nil
}

type jobSummary struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Status     job.Status `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

const (
	defaultPageLimit = 50
	maxPageLimit     = 500
)

type listResponse struct {
	Jobs   []jobSummary `json:"jobs"`
	Count  int          `json:"count"`
	Total  int          `json:"total"`
	Limit  int          `json:"limit"`
	Offset int          `json:"offset"`
}

func (s *server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, err := queryInt(q.Get("limit"), defaultPageLimit, 1, maxPageLimit)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("limit must be a whole number between 1 and %d", maxPageLimit))
		return
	}
	offset, err := queryInt(q.Get("offset"), 0, 0, math.MaxInt32)
	if err != nil {
		writeError(w, http.StatusBadRequest, "offset must be a whole number, 0 or more")
		return
	}

	jobs, err := s.store.List(r.Context())
	if err != nil {
		s.logger.ErrorContext(r.Context(), "list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}

	page := jobs[min(offset, len(jobs)):min(offset+limit, len(jobs))]
	out := make([]jobSummary, len(page))
	for i, j := range page {
		out[i] = jobSummary{ID: j.ID, Name: j.Name, Status: j.Status, CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt}
	}
	writeJSON(w, http.StatusOK, listResponse{Jobs: out, Count: len(out), Total: len(jobs), Limit: limit, Offset: offset})
}

// queryInt parses an optional query parameter, returning def when it is empty.
func queryInt(raw string, def, lo, hi int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		return 0, errors.New("out of range")
	}
	return n, nil
}

func (s *server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case err != nil:
		s.logger.ErrorContext(r.Context(), "get job", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read the job")
	default:
		writeJSON(w, http.StatusOK, j)
	}
}

func (s *server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	j, err := s.runner.Cancel(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, runner.ErrJobFinished):
		writeError(w, http.StatusConflict, "job has already finished")
	case err != nil:
		s.logger.ErrorContext(r.Context(), "cancel job", "err", err)
		writeError(w, http.StatusInternalServerError, "could not cancel the job")
	case j.Status == job.StatusCancelled:
		writeJSON(w, http.StatusOK, j)
	default:
		writeJSON(w, http.StatusAccepted, j)
	}
}

func (s *server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.runner.Delete(r.Context(), id)
	switch {
	case err == nil:
		s.logger.InfoContext(logging.WithJobID(r.Context(), id), "job deleted")
		w.WriteHeader(http.StatusNoContent)
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, runner.ErrJobActive):
		writeError(w, http.StatusConflict, "job is still pending or running; cancel it first")
	case errors.Is(err, runner.ErrBusy):
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "the database is busy with another job, try again later")
	default:
		s.logger.ErrorContext(r.Context(), "delete job", "job_id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "could not delete the job")
	}
}

type errorsResponse struct {
	JobID    string            `json:"job_id"`
	Status   job.Status        `json:"status"`
	Total    int64             `json:"total"`
	Stored   int               `json:"stored"`
	Complete bool              `json:"complete"`
	Errors   []job.RecordError `json:"errors"`
}

func (s *server) handleJobErrors(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
		return
	case err != nil:
		s.logger.ErrorContext(r.Context(), "get job errors", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read the job")
		return
	}

	errs := j.FailedRecords
	if errs == nil {
		errs = []job.RecordError{}
	}
	writeJSON(w, http.StatusOK, errorsResponse{
		JobID:    j.ID,
		Status:   j.Status,
		Total:    j.Progress.RecordsFailed + j.Progress.SourceErrors,
		Stored:   len(errs),
		Complete: j.Finished(),
		Errors:   errs,
	})
}

type resultsResponse struct {
	JobID      string                `json:"job_id"`
	Status     job.Status            `json:"status"`
	Available  bool                  `json:"available"`
	Progress   job.Progress          `json:"progress"`
	Aggregates []job.AggregateResult `json:"aggregates"`
	Outputs    []job.Output          `json:"outputs"`
}

func (s *server) handleJobResults(w http.ResponseWriter, r *http.Request) {
	j, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
		return
	case err != nil:
		s.logger.ErrorContext(r.Context(), "get job results", "err", err)
		writeError(w, http.StatusInternalServerError, "could not read the job")
		return
	}

	aggs := j.Aggregates
	if aggs == nil {
		aggs = []job.AggregateResult{}
	}
	outs := j.Outputs
	if outs == nil {
		outs = []job.Output{}
	}
	writeJSON(w, http.StatusOK, resultsResponse{
		JobID:      j.ID,
		Status:     j.Status,
		Available:  j.Status == job.StatusCompleted,
		Progress:   j.Progress,
		Aggregates: aggs,
		Outputs:    outs,
	})
}
