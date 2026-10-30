package source

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func jsonBody(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}
}

func TestHTTPReadsArray(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("accept header = %q", r.Header.Get("Accept"))
		}
		jsonBody(`[{"id":1,"name":"Asha"},{"id":2},7]`)(w, r)
	})

	items, err := readAll(t, NewHTTP(HTTPConfig{Name: "api", URL: srv.URL}, NewHTTPClient(true)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
	if items[0].Record.Source != "api" || items[0].Record.Fields["name"] != "Asha" {
		t.Errorf("item 0 = %+v", items[0])
	}
	if !errors.Is(items[2].Err, ErrNotObject) {
		t.Errorf("item 2: want ErrNotObject row error, got %v", items[2].Err)
	}
}

func TestHTTPRecordsPath(t *testing.T) {
	srv := serve(t, jsonBody(`{"info":{"seed":"x"},"results":[{"id":1},{"id":2}]}`))

	cfg := HTTPConfig{Name: "api", URL: srv.URL, RecordsPath: "results"}
	items, err := readAll(t, NewHTTP(cfg, NewHTTPClient(true)))
	if err != nil || len(items) != 2 {
		t.Fatalf("got %d items, err %v; want 2 items", len(items), err)
	}
}

func TestHTTPFailures(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		cfg     HTTPConfig
		wantErr error
	}{
		{
			name:    "not found",
			handler: func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) },
			wantErr: ErrHTTPStatus,
		},
		{
			name:    "server error",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) },
			wantErr: ErrHTTPStatus,
		},
		{
			name:    "body over the limit",
			handler: jsonBody(`[` + strings.Repeat(`{"id":1},`, 100) + `{"id":1}]`),
			cfg:     HTTPConfig{MaxBytes: 64},
			wantErr: ErrResponseTooLarge,
		},
		{
			name: "server too slow",
			handler: func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(2 * time.Second):
				}
			},
			cfg:     HTTPConfig{Timeout: 50 * time.Millisecond},
			wantErr: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := serve(t, tc.handler)
			tc.cfg.Name, tc.cfg.URL = "api", srv.URL

			_, err := readAll(t, NewHTTP(tc.cfg, NewHTTPClient(true)))
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestHTTPBodyExactlyAtLimit(t *testing.T) {
	body := `[{"id":1}]`
	srv := serve(t, jsonBody(body))

	cfg := HTTPConfig{Name: "api", URL: srv.URL, MaxBytes: int64(len(body))}
	if _, err := readAll(t, NewHTTP(cfg, NewHTTPClient(true))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHTTPGuardBlocksLoopback(t *testing.T) {
	var called atomic.Bool
	srv := serve(t, func(http.ResponseWriter, *http.Request) { called.Store(true) })

	_, err := readAll(t, NewHTTP(HTTPConfig{Name: "api", URL: srv.URL}, NewHTTPClient(false)))
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("err = %v, want ErrBlockedAddress", err)
	}
	if called.Load() {
		t.Error("the blocked server received a request")
	}
}

func TestIsPublic(t *testing.T) {
	tests := map[string]bool{
		"8.8.8.8":          true,
		"1.1.1.1":          true,
		"2606:4700::1111":  true,
		"127.0.0.1":        false,
		"::1":              false,
		"10.1.2.3":         false,
		"172.16.0.1":       false,
		"192.168.1.10":     false,
		"169.254.169.254":  false,
		"100.64.0.1":       false,
		"0.0.0.0":          false,
		"::ffff:127.0.0.1": false,
		"fe80::1":          false,
		"fd00::1":          false,
		"224.0.0.1":        false,
	}
	for addr, want := range tests {
		if got := isPublic(netip.MustParseAddr(addr)); got != want {
			t.Errorf("isPublic(%s) = %v, want %v", addr, got, want)
		}
	}
}
