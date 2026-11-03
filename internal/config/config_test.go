package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) func(string) string {
	return func(key string) string { return vars[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "9090" || cfg.LogLevel != slog.LevelInfo || cfg.LogFormat != "json" {
		t.Errorf("got port %q level %v format %q", cfg.Port, cfg.LogLevel, cfg.LogFormat)
	}
	if cfg.DataDir != "." || cfg.MaxBodyBytes != 1<<20 || cfg.AllowPrivateSources {
		t.Errorf("got data dir %q body %d private %v", cfg.DataDir, cfg.MaxBodyBytes, cfg.AllowPrivateSources)
	}
	if cfg.OutputDir != "data/output" || cfg.SQLitePath != "data/datapipe.db" {
		t.Errorf("got output dir %q sqlite path %q", cfg.OutputDir, cfg.SQLitePath)
	}
	if cfg.JobWorkers != 2 || cfg.JobQueueSize != 100 {
		t.Errorf("got job workers %d queue %d", cfg.JobWorkers, cfg.JobQueueSize)
	}
	if cfg.ReadTimeout != 15*time.Second || cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("got read %v shutdown %v", cfg.ReadTimeout, cfg.ShutdownTimeout)
	}
	if cfg.JobTTL != 24*time.Hour || cfg.APIKey != "" || cfg.CORSOrigins != nil {
		t.Errorf("got ttl %v api key %q cors %v", cfg.JobTTL, cfg.APIKey, cfg.CORSOrigins)
	}
}

func TestLoadSecuritySettings(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"API_KEY":              "0123456789abcdef",
		"CORS_ALLOWED_ORIGINS": " http://localhost:3000 , https://app.example.com,",
		"JOB_TTL":              "30m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.APIKey != "0123456789abcdef" || cfg.JobTTL != 30*time.Minute {
		t.Errorf("got api key %q ttl %v", cfg.APIKey, cfg.JobTTL)
	}
	if strings.Join(cfg.CORSOrigins, "|") != "http://localhost:3000|https://app.example.com" {
		t.Errorf("cors origins = %q", cfg.CORSOrigins)
	}

	if cfg, err := Load(env(map[string]string{"CORS_ALLOWED_ORIGINS": "*"})); err != nil || cfg.CORSOrigins[0] != "*" {
		t.Errorf("wildcard: %v, %v", cfg.CORSOrigins, err)
	}
}

func TestLoadRejectsBadSecuritySettings(t *testing.T) {
	_, err := Load(env(map[string]string{
		"API_KEY":              "short-secret",
		"CORS_ALLOWED_ORIGINS": "localhost:3000,https://ok.example,https://x.example/path",
		"JOB_TTL":              "forever",
	}))
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	msg := err.Error()
	for _, want := range []string{"API_KEY=", "at least 16", `CORS_ALLOWED_ORIGINS="localhost:3000"`, `CORS_ALLOWED_ORIGINS="https://x.example/path"`, "JOB_TTL="} {
		if !strings.Contains(msg, want) {
			t.Errorf("error does not mention %s:\n%v", want, msg)
		}
	}
	if strings.Contains(msg, "short-secret") {
		t.Error("the API key value was printed in the error")
	}
	if strings.Contains(msg, "https://ok.example") {
		t.Error("a valid origin was reported as invalid")
	}
}

func TestLoadOverrides(t *testing.T) {
	dir := t.TempDir()
	cfg, err := Load(env(map[string]string{
		"PORT":                  "8081",
		"LOG_LEVEL":             "DEBUG",
		"LOG_FORMAT":            "Text",
		"DATA_DIR":              dir,
		"MAX_BODY_BYTES":        "2048",
		"WRITE_TIMEOUT":         "1m",
		"JOB_WORKERS":           "8",
		"ALLOW_PRIVATE_SOURCES": "true",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "8081" || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != "text" {
		t.Errorf("got port %q level %v format %q", cfg.Port, cfg.LogLevel, cfg.LogFormat)
	}
	if cfg.DataDir != dir || cfg.MaxBodyBytes != 2048 || cfg.WriteTimeout != time.Minute || !cfg.AllowPrivateSources || cfg.JobWorkers != 8 {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := Load(env(map[string]string{
		"PORT":                  "99999",
		"LOG_LEVEL":             "loud",
		"LOG_FORMAT":            "xml",
		"DATA_DIR":              "does/not/exist",
		"MAX_BODY_BYTES":        "-1",
		"READ_TIMEOUT":          "soon",
		"ALLOW_PRIVATE_SOURCES": "maybe",
		"JOB_WORKERS":           "0",
		"JOB_QUEUE_SIZE":        "lots",
	}))
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, key := range []string{"PORT", "LOG_LEVEL", "LOG_FORMAT", "DATA_DIR", "MAX_BODY_BYTES", "READ_TIMEOUT", "ALLOW_PRIVATE_SOURCES", "JOB_WORKERS", "JOB_QUEUE_SIZE"} {
		if !strings.Contains(err.Error(), key+"=") {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}
