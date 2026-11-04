package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestLimiterTokenBucket(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	l := newLimiter(1, 2, clock.now) // burst of 2, then 1 per second

	for i := range 2 {
		if ok, _ := l.allow("a"); !ok {
			t.Fatalf("request %d of the burst was refused", i+1)
		}
	}
	if ok, wait := l.allow("a"); ok || wait != time.Second {
		t.Errorf("3rd request: allowed %v, wait %v; want refused, wait 1s", ok, wait)
	}
	if ok, _ := l.allow("b"); !ok {
		t.Error("another client was limited by a's requests")
	}

	clock.advance(500 * time.Millisecond)
	if ok, wait := l.allow("a"); ok || wait != 500*time.Millisecond {
		t.Errorf("after 0.5s: allowed %v, wait %v; want refused, wait 0.5s", ok, wait)
	}
	clock.advance(500 * time.Millisecond)
	if ok, _ := l.allow("a"); !ok {
		t.Error("after 1s a new token should be there")
	}
}

func TestLimiterForgetsIdleClients(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	l := newLimiter(10, 20, clock.now)
	for _, ip := range []string{"a", "b", "c"} {
		l.allow(ip)
	}

	clock.advance(sweepEvery)
	l.allow("d")
	if len(l.clients) != 1 {
		t.Errorf("%d clients kept, want only d: idle full buckets should be dropped", len(l.clients))
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	h, _ := newTestServerWith(t, nil, &fakeRunner{}, func(o *Options) {
		o.RateLimitPerSecond, o.RateLimitBurst = 1, 2
	})
	get := func(path, ip string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		req.RemoteAddr = ip + ":40000"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	for range 2 {
		if rec := get("/api/v1/pipelines", "10.0.0.1"); rec.Code != http.StatusOK {
			t.Fatalf("within the burst: status %d", rec.Code)
		}
	}
	rec := get("/api/v1/pipelines", "10.0.0.1")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "1" {
		t.Errorf("over the limit: status %d, Retry-After %q; want 429 and 1", rec.Code, rec.Header().Get("Retry-After"))
	}
	if body := decode[errorBody](t, rec); body.Error != "too many requests, try again later" {
		t.Errorf("body = %+v", body)
	}

	if rec := get("/api/v1/pipelines", "10.0.0.2"); rec.Code != http.StatusOK {
		t.Errorf("another IP: status %d, want 200", rec.Code)
	}
	for range 5 {
		if rec := get("/healthz", "10.0.0.1"); rec.Code != http.StatusOK {
			t.Fatalf("/healthz must not be limited: status %d", rec.Code)
		}
	}
}

func TestRateLimitOff(t *testing.T) {
	h, _ := newTestServer(t, nil, &fakeRunner{})
	for range 50 {
		if rec := do(t, h, http.MethodGet, "/api/v1/pipelines", ""); rec.Code != http.StatusOK {
			t.Fatalf("status %d with the limit off", rec.Code)
		}
	}
}
