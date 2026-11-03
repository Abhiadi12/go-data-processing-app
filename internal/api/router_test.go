package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
)

// fakeRunner records submitted jobs instead of running them.
type fakeRunner struct {
	mu        sync.Mutex
	submitted []string
	submitErr error
	cancel    func(ctx context.Context, id string) (job.Job, error)
	del       func(ctx context.Context, id string) error
}

func (f *fakeRunner) Delete(ctx context.Context, id string) error {
	return f.del(ctx, id)
}

func (f *fakeRunner) Submit(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = append(f.submitted, id)
	return nil
}

func (f *fakeRunner) Cancel(ctx context.Context, id string) (job.Job, error) {
	return f.cancel(ctx, id)
}

func newTestHandler(t *testing.T, logs io.Writer) http.Handler {
	t.Helper()
	h, _ := newTestServer(t, logs, &fakeRunner{})
	return h
}

func newTestServer(t *testing.T, logs io.Writer, r JobRunner) (http.Handler, *job.MemoryStore) {
	t.Helper()
	return newTestServerWith(t, logs, r, func(*Options) {})
}

func newTestServerWith(t *testing.T, logs io.Writer, r JobRunner, change func(*Options)) (http.Handler, *job.MemoryStore) {
	t.Helper()
	if logs == nil {
		logs = io.Discard
	}
	store := job.NewMemoryStore()
	opts := Options{
		Logger:       logging.New(logs, slog.LevelInfo, "json"),
		Store:        store,
		Runner:       r,
		Files:        fstest.MapFS{"orders.csv": {Data: []byte("order_id\n1\n")}},
		MaxBodyBytes: 4096,
	}
	change(&opts)
	return NewHandler(opts), store
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequestWithContext(t.Context(), method, path, r)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealth(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/healthz", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content-type = %q", got)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Errorf("body = %s", rec.Body.String())
	}
}

func TestHealthWrongMethod(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodPost, "/healthz", "")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	rec := do(t, newTestHandler(t, nil), http.MethodGet, "/healthz", "")

	want := map[string]string{
		"X-Content-Type-Options":  "nosniff",
		"X-Frame-Options":         "DENY",
		"Referrer-Policy":         "no-referrer",
		"Content-Security-Policy": "default-src 'none'",
		"Cache-Control":           "no-store",
	}
	for k, v := range want {
		if got := rec.Header().Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

func TestRequestID(t *testing.T) {
	h := newTestHandler(t, nil)

	tests := []struct {
		name     string
		incoming string
		keep     bool
	}{
		{"generated when missing", "", false},
		{"kept when valid", "abc-123_x.y", true},
		{"replaced when too long", strings.Repeat("a", 65), false},
		{"replaced when it has bad characters", "abc\"<script>", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
			if tc.incoming != "" {
				req.Header.Set("X-Request-ID", tc.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get("X-Request-ID")
			if tc.keep && got != tc.incoming {
				t.Errorf("request id = %q, want %q", got, tc.incoming)
			}
			if !tc.keep && (got == tc.incoming || !validRequestID.MatchString(got)) {
				t.Errorf("request id = %q, want a new valid id", got)
			}
		})
	}
}

func TestRequestLogHasRequestID(t *testing.T) {
	var logs bytes.Buffer
	h := newTestHandler(t, &logs)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "trace-me")
	h.ServeHTTP(httptest.NewRecorder(), req)

	line := logs.String()
	for _, want := range []string{`"msg":"request"`, `"status":200`, `"route":"GET /healthz"`, `"request_id":"trace-me"`} {
		if !strings.Contains(line, want) {
			t.Errorf("log %s does not contain %s", line, want)
		}
	}
}

func TestRecoverPanic(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	h := recoverPanic(logger)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "boom") {
		t.Errorf("panic value leaked to the client: %s", rec.Body.String())
	}
	if !strings.Contains(logs.String(), "boom") || !strings.Contains(logs.String(), "stack") {
		t.Errorf("panic not logged with stack: %s", logs.String())
	}
}

func TestAPIKey(t *testing.T) {
	const key = "s3cret-key-for-tests"
	h, _ := newTestServerWith(t, nil, &fakeRunner{}, func(o *Options) { o.APIKey = key })

	tests := []struct {
		name   string
		path   string
		header string
		want   int
	}{
		{"api without key", "/api/v1/pipelines", "", http.StatusUnauthorized},
		{"api with wrong key", "/api/v1/pipelines", "wrong", http.StatusUnauthorized},
		{"api with key prefix only", "/api/v1/pipelines", key[:5], http.StatusUnauthorized},
		{"api with key", "/api/v1/pipelines", key, http.StatusOK},
		{"metrics without key", "/api/v1/metrics", "", http.StatusUnauthorized},
		{"health is public", "/healthz", "", http.StatusOK},
		{"docs are public", "/docs", "", http.StatusOK},
		{"spec is public", "/openapi.json", "", http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tc.path, nil)
			if tc.header != "" {
				req.Header.Set("X-API-Key", tc.header)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
			if rec.Code == http.StatusUnauthorized && !strings.Contains(rec.Body.String(), "missing or invalid API key") {
				t.Errorf("body = %s", rec.Body.String())
			}
		})
	}
}

func TestNoAPIKeyMeansOpen(t *testing.T) {
	if rec := do(t, newTestHandler(t, nil), http.MethodGet, "/api/v1/pipelines", ""); rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when API_KEY is not set", rec.Code)
	}
}

func TestCORS(t *testing.T) {
	h, _ := newTestServerWith(t, nil, &fakeRunner{}, func(o *Options) {
		o.CORSOrigins = []string{"http://localhost:3000"}
		o.APIKey = "s3cret-key-for-tests"
	})

	send := func(method, origin string, preflight bool) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), method, "/api/v1/pipelines", nil)
		req.Header.Set("Origin", origin)
		if preflight {
			req.Header.Set("Access-Control-Request-Method", "POST")
			req.Header.Set("Access-Control-Request-Headers", "content-type, x-api-key")
		} else {
			req.Header.Set("X-API-Key", "s3cret-key-for-tests")
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	pre := send(http.MethodOptions, "http://localhost:3000", true)
	if pre.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want 204 (it must not need the API key)", pre.Code)
	}
	for k, want := range map[string]string{
		"Access-Control-Allow-Origin":  "http://localhost:3000",
		"Access-Control-Allow-Methods": "GET, POST, DELETE",
		"Access-Control-Allow-Headers": "Content-Type, X-API-Key, X-Request-ID",
	} {
		if got := pre.Header().Get(k); got != want {
			t.Errorf("preflight %s = %q, want %q", k, got, want)
		}
	}

	get := send(http.MethodGet, "http://localhost:3000", false)
	if get.Code != http.StatusOK || get.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("allowed origin: status %d, allow-origin %q", get.Code, get.Header().Get("Access-Control-Allow-Origin"))
	}
	if !strings.Contains(get.Header().Get("Access-Control-Expose-Headers"), "Location") {
		t.Error("Location header not exposed to the browser")
	}

	other := send(http.MethodGet, "https://evil.example", false)
	if got := other.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("other origin got Access-Control-Allow-Origin %q", got)
	}
	if other.Header().Get("Vary") != "Origin" {
		t.Error("responses must vary by Origin so caches keep them apart")
	}
}

func TestCORSAnyOrigin(t *testing.T) {
	h, _ := newTestServerWith(t, nil, &fakeRunner{}, func(o *Options) { o.CORSOrigins = []string{"*"} })
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "https://anything.example")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://anything.example" {
		t.Errorf("allow-origin = %q", got)
	}
}

func TestNoCORSByDefault(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rec := httptest.NewRecorder()
	newTestHandler(t, nil).ServeHTTP(rec, req)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" || rec.Header().Get("Vary") != "" {
		t.Error("CORS headers sent although CORS_ALLOWED_ORIGINS is empty")
	}
}

func TestChainOrder(t *testing.T) {
	var order []string
	mw := func(name string) middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	h := chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { order = append(order, "handler") }),
		mw("first"), mw("second"))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

	if strings.Join(order, ",") != "first,second,handler" {
		t.Errorf("order = %v", order)
	}
}
