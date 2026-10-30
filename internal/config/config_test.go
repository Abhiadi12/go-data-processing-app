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
	if cfg.ReadTimeout != 15*time.Second || cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("got read %v shutdown %v", cfg.ReadTimeout, cfg.ShutdownTimeout)
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
		"ALLOW_PRIVATE_SOURCES": "true",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Port != "8081" || cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != "text" {
		t.Errorf("got port %q level %v format %q", cfg.Port, cfg.LogLevel, cfg.LogFormat)
	}
	if cfg.DataDir != dir || cfg.MaxBodyBytes != 2048 || cfg.WriteTimeout != time.Minute || !cfg.AllowPrivateSources {
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
	}))
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, key := range []string{"PORT", "LOG_LEVEL", "LOG_FORMAT", "DATA_DIR", "MAX_BODY_BYTES", "READ_TIMEOUT", "ALLOW_PRIVATE_SOURCES"} {
		if !strings.Contains(err.Error(), key+"=") {
			t.Errorf("error does not mention %s:\n%v", key, err)
		}
	}
}
