package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/runner"
)

const validBody = `{
	"name": "orders",
	"sources": [{"name": "orders-csv", "type": "csv", "path": "orders.csv"}],
	"schema": {"fields": [{"name": "order_id", "type": "int", "required": true}]},
	"exports": [{"type": "json", "content": "records"}]
}`

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("response is not json: %v\n%s", err, rec.Body.String())
	}
	return v
}

type jobResponse struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	RequestID string `json:"request_id"`
	Spec      struct {
		Options struct {
			ValidateWorkers int `json:"validate_workers"`
		} `json:"options"`
	} `json:"spec"`
}

func TestCreateGetListJob(t *testing.T) {
	h := newTestHandler(t, nil)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/pipelines", strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Request-ID", "req-42")
	created := httptest.NewRecorder()
	h.ServeHTTP(created, req)

	if created.Code != http.StatusAccepted {
		t.Fatalf("create status = %d, want 202: %s", created.Code, created.Body.String())
	}
	job := decode[jobResponse](t, created)
	if job.ID == "" || job.Status != "pending" || job.Name != "orders" || job.RequestID != "req-42" {
		t.Errorf("created job = %+v", job)
	}
	if job.Spec.Options.ValidateWorkers != 4 {
		t.Errorf("defaults not applied: validate_workers = %d", job.Spec.Options.ValidateWorkers)
	}
	if loc := created.Header().Get("Location"); loc != "/api/v1/pipelines/"+job.ID {
		t.Errorf("location = %q", loc)
	}

	got := do(t, h, http.MethodGet, "/api/v1/pipelines/"+job.ID, "")
	if got.Code != http.StatusOK || decode[jobResponse](t, got).ID != job.ID {
		t.Errorf("get: status %d body %s", got.Code, got.Body.String())
	}

	list := do(t, h, http.MethodGet, "/api/v1/pipelines", "")
	body := decode[struct {
		Count int              `json:"count"`
		Jobs  []map[string]any `json:"jobs"`
	}](t, list)
	if list.Code != http.StatusOK || body.Count != 1 || body.Jobs[0]["id"] != job.ID {
		t.Errorf("list: status %d body %s", list.Code, list.Body.String())
	}
	if _, hasSpec := body.Jobs[0]["spec"]; hasSpec {
		t.Error("list should return summaries without the spec")
	}
}

func TestListEmpty(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/api/v1/pipelines", "")

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"jobs":[]`) {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestGetUnknownJob(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/api/v1/pipelines/nope", "")

	if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "job not found") {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestCreateJobRejectsBadRequests(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		contentType string
		wantStatus  int
		wantText    string
	}{
		{"empty body", "", "application/json", http.StatusBadRequest, "body is empty"},
		{"broken json", `{"name": `, "application/json", http.StatusBadRequest, "ends too early"},
		{"syntax error", `{"name" "x"}`, "application/json", http.StatusBadRequest, "malformed json"},
		{"wrong type", `{"name": 5}`, "application/json", http.StatusBadRequest, `field \"name\"`},
		{"unknown field", `{"nme": "x"}`, "application/json", http.StatusBadRequest, "unknown field"},
		{"two objects", validBody + validBody, "application/json", http.StatusBadRequest, "single json object"},
		{"too large", `{"name": "` + strings.Repeat("a", 5000) + `"}`, "application/json", http.StatusRequestEntityTooLarge, "at most 4096 bytes"},
		{"not json content type", validBody, "text/plain", http.StatusUnsupportedMediaType, "application/json"},
		{"invalid spec", `{"name": "x", "sources": [{"name": "a", "type": "csv", "path": "missing.csv"}]}`, "application/json", http.StatusBadRequest, "sources[0].path"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/pipelines", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			rec := httptest.NewRecorder()
			newTestHandler(t, nil).ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantText) {
				t.Errorf("body %s does not contain %q", rec.Body.String(), tc.wantText)
			}
		})
	}
}

func TestInvalidSpecListsEveryProblem(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodPost, "/api/v1/pipelines", `{"options": {"validate_workers": 99}}`)

	body := decode[errorBody](t, rec)
	if rec.Code != http.StatusBadRequest || body.Error != "invalid job spec" {
		t.Fatalf("status %d body %+v", rec.Code, body)
	}
	fields := map[string]bool{}
	for _, p := range body.Details {
		fields[p.Field] = true
	}
	for _, want := range []string{"name", "sources", "schema.fields", "exports", "options.validate_workers"} {
		if !fields[want] {
			t.Errorf("no problem reported for %s: %+v", want, body.Details)
		}
	}
}

func TestCreateSubmitsJobToRunner(t *testing.T) {
	fr := &fakeRunner{}
	h, _ := newTestServer(t, nil, fr)

	rec := do(t, h, http.MethodPost, "/api/v1/pipelines", validBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	id := decode[jobResponse](t, rec).ID
	if len(fr.submitted) != 1 || fr.submitted[0] != id {
		t.Errorf("submitted %v, want [%s]", fr.submitted, id)
	}
}

func TestCreateRejectedByRunner(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{"queue full", runner.ErrQueueFull, "job queue is full"},
		{"shutting down", runner.ErrStopped, "server is shutting down"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, store := newTestServer(t, nil, &fakeRunner{submitErr: tc.err})

			rec := do(t, h, http.MethodPost, "/api/v1/pipelines", validBody)
			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), tc.wantMsg) {
				t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Retry-After") != "5" {
				t.Errorf("Retry-After = %q", rec.Header().Get("Retry-After"))
			}

			jobs, _ := store.List(t.Context())
			if len(jobs) != 1 || jobs[0].Status != job.StatusFailed || !strings.HasPrefix(jobs[0].Error, "rejected: ") {
				t.Errorf("rejected job not marked failed: %+v", jobs)
			}
		})
	}
}

func TestCancelJob(t *testing.T) {
	tests := []struct {
		name       string
		result     job.Job
		err        error
		wantStatus int
	}{
		{"pending job cancelled", job.Job{ID: "a", Status: job.StatusCancelled}, nil, http.StatusOK},
		{"running job signalled", job.Job{ID: "a", Status: job.StatusRunning}, nil, http.StatusAccepted},
		{"unknown job", job.Job{}, job.ErrNotFound, http.StatusNotFound},
		{"finished job", job.Job{}, runner.ErrJobFinished, http.StatusConflict},
		{"store failure", job.Job{}, errors.New("disk on fire"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotID string
			fr := &fakeRunner{cancel: func(_ context.Context, id string) (job.Job, error) {
				gotID = id
				return tc.result, tc.err
			}}
			h, _ := newTestServer(t, nil, fr)

			rec := do(t, h, http.MethodPost, "/api/v1/pipelines/a/cancel", "")
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if gotID != "a" {
				t.Errorf("runner got id %q", gotID)
			}
			if strings.Contains(rec.Body.String(), "disk on fire") {
				t.Error("internal error leaked to the client")
			}
		})
	}
}

func TestJobErrors(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})

	j := job.New(job.Spec{Name: "x"}, "")
	j.Status = job.StatusCompleted
	j.Progress = job.Progress{RecordsRead: 10, RecordsFailed: 3}
	j.FailedRecords = []job.RecordError{
		{Source: "csv", Index: 2, Stage: "validate", Reasons: []string{"amount: must be a number"}},
		{Source: "csv", Index: 5, Stage: "ingest", Reasons: []string{"too many fields"}},
	}
	empty := job.New(job.Spec{Name: "y"}, "")
	for _, x := range []job.Job{j, empty} {
		if err := store.Create(t.Context(), x); err != nil {
			t.Fatal(err)
		}
	}

	rec := do(t, h, http.MethodGet, "/api/v1/pipelines/"+j.ID+"/errors", "")
	body := decode[errorsResponse](t, rec)
	if rec.Code != http.StatusOK || body.Total != 3 || body.Stored != 2 || !body.Complete || body.Errors[0].Index != 2 {
		t.Errorf("status %d body %+v", rec.Code, body)
	}

	rec = do(t, h, http.MethodGet, "/api/v1/pipelines/"+empty.ID+"/errors", "")
	if !strings.Contains(rec.Body.String(), `"errors":[]`) || !strings.Contains(rec.Body.String(), `"complete":false`) {
		t.Errorf("pending job errors body = %s", rec.Body.String())
	}

	if rec := do(t, h, http.MethodGet, "/api/v1/pipelines/nope/errors", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown job: status %d", rec.Code)
	}
}

func TestJobResponseHidesFailedRecords(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})
	j := job.New(job.Spec{Name: "x"}, "")
	j.FailedRecords = []job.RecordError{{Source: "csv", Reasons: []string{"secret-ish"}}}
	if err := store.Create(t.Context(), j); err != nil {
		t.Fatal(err)
	}

	rec := do(t, h, http.MethodGet, "/api/v1/pipelines/"+j.ID, "")
	if strings.Contains(rec.Body.String(), "secret-ish") || !strings.Contains(rec.Body.String(), `"progress"`) {
		t.Errorf("body = %s", rec.Body.String())
	}
}
