package api

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
)

func newTestHandler(t *testing.T, logs io.Writer) http.Handler {
	t.Helper()
	if logs == nil {
		logs = io.Discard
	}
	return NewHandler(Options{
		Logger:       logging.New(logs, slog.LevelInfo, "json"),
		Store:        job.NewMemoryStore(),
		Files:        fstest.MapFS{"orders.csv": {Data: []byte("order_id\n1\n")}},
		MaxBodyBytes: 4096,
	})
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
