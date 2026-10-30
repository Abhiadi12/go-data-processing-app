// Package api is the HTTP layer: routing, middleware and handlers.
package api

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

type Options struct {
	Logger       *slog.Logger
	Store        job.Store
	Files        fs.FS
	MaxBodyBytes int64
}

type server struct {
	logger *slog.Logger
	store  job.Store
	files  fs.FS
}

// NewHandler returns the API with all routes and middleware.
func NewHandler(opts Options) http.Handler {
	s := &server{logger: opts.Logger, store: opts.Store, files: opts.Files}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /api/v1/pipelines", s.handleCreateJob)
	mux.HandleFunc("GET /api/v1/pipelines", s.handleListJobs)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", s.handleGetJob)

	return chain(mux,
		requestID,
		logRequests(opts.Logger),
		recoverPanic(opts.Logger),
		securityHeaders,
		limitBody(opts.MaxBodyBytes),
	)
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type errorBody struct {
	Error   string        `json:"error"`
	Details []job.Problem `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("write json response", "err", err)
	}
}
