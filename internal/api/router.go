// Package api is the HTTP layer: routing, middleware and handlers.
package api

import (
	"context"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
)

type Options struct {
	Logger       *slog.Logger
	Store        job.Store
	Runner       JobRunner
	Files        fs.FS
	MaxBodyBytes int64

	// APIKey, when set, is required in the X-API-Key header on /api/ routes.
	APIKey string
	// CORSOrigins are the browser origins allowed to call the API ("*" for any).
	CORSOrigins []string
}

type server struct {
	logger  *slog.Logger
	store   job.Store
	runner  JobRunner
	files   fs.FS
	started time.Time
}

// JobRunner runs jobs in the background (implemented by runner.Runner).
type JobRunner interface {
	Submit(id string) error
	Cancel(ctx context.Context, id string) (job.Job, error)
	Delete(ctx context.Context, id string) error
}

func NewHandler(opts Options) http.Handler {
	s := &server{logger: opts.Logger, store: opts.Store, runner: opts.Runner, files: opts.Files, started: time.Now()}

	mux := http.NewServeMux()
	for _, rt := range s.routes() {
		mux.HandleFunc(rt.pattern, rt.handler)
	}
	mux.HandleFunc("GET /openapi.json", handleOpenAPI)
	mux.HandleFunc("GET /docs", handleDocs)

	return chain(mux,
		requestID,
		logRequests(opts.Logger),
		recoverPanic(opts.Logger),
		securityHeaders,
		cors(opts.CORSOrigins),
		requireAPIKey(opts.APIKey),
		limitBody(opts.MaxBodyBytes),
	)
}

type route struct {
	pattern string
	handler http.HandlerFunc
}

func (s *server) routes() []route {
	return []route{
		{"GET /healthz", s.handleHealth},
		{"POST /api/v1/pipelines", s.handleCreateJob},
		{"GET /api/v1/pipelines", s.handleListJobs},
		{"GET /api/v1/pipelines/{id}", s.handleGetJob},
		{"DELETE /api/v1/pipelines/{id}", s.handleDeleteJob},
		{"POST /api/v1/pipelines/{id}/cancel", s.handleCancelJob},
		{"GET /api/v1/pipelines/{id}/errors", s.handleJobErrors},
		{"GET /api/v1/pipelines/{id}/results", s.handleJobResults},
		{"GET /api/v1/pipelines/{id}/progress", s.handleJobProgress},
		{"GET /api/v1/metrics", s.handleMetrics},
	}
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
