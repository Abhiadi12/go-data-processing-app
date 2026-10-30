package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestContextIDsAreLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := New(&buf, slog.LevelInfo, "json")

	ctx := WithJobID(WithRequestID(context.Background(), "req-1"), "job-1")
	logger.With("component", "test").InfoContext(ctx, "hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not json: %v\n%s", err, buf.String())
	}
	for key, want := range map[string]string{"msg": "hello", "request_id": "req-1", "job_id": "job-1", "component": "test"} {
		if line[key] != want {
			t.Errorf("%s = %v, want %q", key, line[key], want)
		}
	}
}

func TestNoIDsWithoutContext(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, slog.LevelInfo, "text").Info("hello")

	if strings.Contains(buf.String(), "request_id") || strings.Contains(buf.String(), "job_id") {
		t.Errorf("unexpected ids in %q", buf.String())
	}
}

func TestLevelFilters(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, slog.LevelWarn, "json").Info("hidden")

	if buf.Len() != 0 {
		t.Errorf("info line written at warn level: %s", buf.String())
	}
}
