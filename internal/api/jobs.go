package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
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

	s.logger.InfoContext(logging.WithJobID(r.Context(), j.ID), "job created", "name", j.Name, "sources", len(spec.Sources))
	w.Header().Set("Location", "/api/v1/pipelines/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
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

func (s *server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.List(r.Context())
	if err != nil {
		s.logger.ErrorContext(r.Context(), "list jobs", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}

	out := make([]jobSummary, len(jobs))
	for i, j := range jobs {
		out[i] = jobSummary{ID: j.ID, Name: j.Name, Status: j.Status, CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": out, "count": len(out)})
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
