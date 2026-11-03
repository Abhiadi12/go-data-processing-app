package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/metrics"
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

func TestDeleteJob(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{"finished job deleted", nil, http.StatusNoContent, ""},
		{"unknown job", job.ErrNotFound, http.StatusNotFound, "job not found"},
		{"still active", runner.ErrJobActive, http.StatusConflict, "cancel it first"},
		{"database busy", runner.ErrBusy, http.StatusServiceUnavailable, "try again later"},
		{"other failure", errors.New("disk on fire"), http.StatusInternalServerError, "could not delete the job"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotID string
			fr := &fakeRunner{del: func(_ context.Context, id string) error {
				gotID = id
				return tc.err
			}}
			h, _ := newTestServer(t, nil, fr)

			rec := do(t, h, http.MethodDelete, "/api/v1/pipelines/a", "")
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("status %d body %q, want %d containing %q", rec.Code, rec.Body.String(), tc.wantStatus, tc.wantBody)
			}
			if gotID != "a" {
				t.Errorf("runner got id %q", gotID)
			}
			if tc.wantStatus == http.StatusNoContent && rec.Body.Len() != 0 {
				t.Errorf("204 must have no body, got %q", rec.Body.String())
			}
			if tc.wantStatus == http.StatusServiceUnavailable && rec.Header().Get("Retry-After") != "5" {
				t.Error("503 without Retry-After")
			}
			if strings.Contains(rec.Body.String(), "disk on fire") {
				t.Error("internal error leaked to the client")
			}
		})
	}
}

func TestListPagination(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})
	start := time.Now().UTC()
	for i := range 5 {
		j := job.New(job.Spec{Name: "job"}, "")
		j.ID = string(rune('a' + i))
		j.CreatedAt = start.Add(time.Duration(i) * time.Second)
		if err := store.Create(context.Background(), j); err != nil {
			t.Fatal(err)
		}
	}

	type counts struct{ Count, Total, Limit, Offset int }
	tests := []struct {
		query string
		want  string
		page  counts
	}{
		{"", "edcba", counts{Count: 5, Total: 5, Limit: 50, Offset: 0}},
		{"?limit=2", "ed", counts{Count: 2, Total: 5, Limit: 2, Offset: 0}},
		{"?limit=2&offset=2", "cb", counts{Count: 2, Total: 5, Limit: 2, Offset: 2}},
		{"?limit=2&offset=4", "a", counts{Count: 1, Total: 5, Limit: 2, Offset: 4}},
		{"?offset=10", "", counts{Count: 0, Total: 5, Limit: 50, Offset: 10}},
	}
	for _, tc := range tests {
		t.Run(tc.query, func(t *testing.T) {
			rec := do(t, h, http.MethodGet, "/api/v1/pipelines"+tc.query, "")
			got := decode[struct {
				Jobs []struct{ ID string }
				counts
			}](t, rec)
			var ids string
			for _, j := range got.Jobs {
				ids += j.ID
			}
			if rec.Code != http.StatusOK || ids != tc.want {
				t.Errorf("status %d ids %q, want %q", rec.Code, ids, tc.want)
			}
			if got.counts != tc.page {
				t.Errorf("page = %+v, want %+v", got.counts, tc.page)
			}
		})
	}

	for _, q := range []string{"?limit=0", "?limit=501", "?limit=abc", "?offset=-1", "?offset=x"} {
		if rec := do(t, h, http.MethodGet, "/api/v1/pipelines"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", q, rec.Code)
		}
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

func TestJobResults(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})

	done := job.New(job.Spec{Name: "x"}, "")
	done.Status = job.StatusCompleted
	done.Progress = job.Progress{RecordsRead: 3, RecordsValid: 2, RecordsFailed: 1}
	done.Aggregates = []job.AggregateResult{{
		Name: "overall", GroupBy: []string{},
		Groups: []job.GroupResult{{Key: map[string]any{}, Metrics: map[string]float64{"count": 2, "sum_total": 4721.98}}},
	}}
	running := job.New(job.Spec{Name: "y"}, "")
	running.Status = job.StatusRunning
	for _, x := range []job.Job{done, running} {
		if err := store.Create(t.Context(), x); err != nil {
			t.Fatal(err)
		}
	}

	rec := do(t, h, http.MethodGet, "/api/v1/pipelines/"+done.ID+"/results", "")
	body := decode[resultsResponse](t, rec)
	if rec.Code != http.StatusOK || !body.Available || body.Progress.RecordsValid != 2 ||
		len(body.Aggregates) != 1 || body.Aggregates[0].Groups[0].Metrics["sum_total"] != 4721.98 {
		t.Errorf("completed: status %d body %s", rec.Code, rec.Body.String())
	}

	rec = do(t, h, http.MethodGet, "/api/v1/pipelines/"+running.ID+"/results", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"available":false`) ||
		!strings.Contains(rec.Body.String(), `"aggregates":[]`) {
		t.Errorf("running: status %d body %s", rec.Code, rec.Body.String())
	}

	if rec := do(t, h, http.MethodGet, "/api/v1/pipelines/nope/results", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown job: status %d", rec.Code)
	}
}

func TestJobProgress(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})

	started := time.Now().Add(-2 * time.Second).UTC()
	j := job.New(job.Spec{Name: "x"}, "")
	j.Status, j.StartedAt = job.StatusRunning, &started
	j.Progress = job.Progress{RecordsRead: 100, RecordsValid: 90, RecordsFailed: 10}
	j.Stages = []job.StageStat{{Stage: "validate", Records: 100, AvgMicros: 3.5, MaxMicros: 20}}
	if err := store.Create(t.Context(), j); err != nil {
		t.Fatal(err)
	}

	rec := do(t, h, http.MethodGet, "/api/v1/pipelines/"+j.ID+"/progress", "")
	body := decode[metrics.Progress](t, rec)
	if rec.Code != http.StatusOK || body.Counts.RecordsRead != 100 || body.PercentComplete != nil ||
		body.RecordsPerSecond < 40 || body.RecordsPerSecond > 51 || body.Stages[0].AvgMicros != 3.5 {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"percent_complete":null`) {
		t.Errorf("running job should report percent_complete null: %s", rec.Body.String())
	}

	if rec := do(t, h, http.MethodGet, "/api/v1/pipelines/nope/progress", ""); rec.Code != http.StatusNotFound {
		t.Errorf("unknown job: status %d", rec.Code)
	}
}

func TestMetrics(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})
	for _, st := range []job.Status{job.StatusCompleted, job.StatusCompleted, job.StatusFailed} {
		j := job.New(job.Spec{Name: "x"}, "")
		j.Status = st
		j.Progress = job.Progress{RecordsRead: 10, RecordsValid: 8, RecordsFailed: 2}
		if err := store.Create(t.Context(), j); err != nil {
			t.Fatal(err)
		}
	}

	rec := do(t, h, http.MethodGet, "/api/v1/metrics", "")
	var body struct {
		Jobs       map[string]int `json:"jobs"`
		Records    job.Progress   `json:"records"`
		ErrorRate  float64        `json:"error_rate"`
		Goroutines int            `json:"goroutines"`
		Uptime     *float64       `json:"uptime_seconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || body.Jobs["completed"] != 2 || body.Jobs["failed"] != 1 || body.Jobs["running"] != 0 ||
		body.Records.RecordsRead != 30 || body.ErrorRate != 0.2 || body.Goroutines < 1 || body.Uptime == nil {
		t.Errorf("status %d body %s", rec.Code, rec.Body.String())
	}
}

func TestJobResultsIncludeOutputs(t *testing.T) {
	h, store := newTestServer(t, nil, &fakeRunner{})
	j := job.New(job.Spec{Name: "x"}, "")
	j.Status = job.StatusCompleted
	j.Outputs = []job.Output{{Type: "csv", Content: "records", Location: "data/output/X/records.csv", Rows: 7}}
	if err := store.Create(t.Context(), j); err != nil {
		t.Fatal(err)
	}

	rec := do(t, h, http.MethodGet, "/api/v1/pipelines/"+j.ID+"/results", "")
	body := decode[resultsResponse](t, rec)
	if len(body.Outputs) != 1 || body.Outputs[0].Rows != 7 {
		t.Errorf("body %s", rec.Body.String())
	}
}
