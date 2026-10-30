// Package config loads server settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port      string
	LogLevel  slog.Level
	LogFormat string

	DataDir      string
	MaxBodyBytes int64

	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration

	AllowPrivateSources bool
	HTTPSourceMaxBytes  int64
}

// Load reads the configuration through getenv (os.Getenv in production)
// and returns every invalid setting at once.
func Load(getenv func(string) string) (Config, error) {
	l := loader{getenv: getenv}

	cfg := Config{
		Port:      l.str("PORT", "9090"),
		LogLevel:  l.level("LOG_LEVEL", slog.LevelInfo),
		LogFormat: l.oneOf("LOG_FORMAT", "json", "json", "text"),

		DataDir:      l.str("DATA_DIR", "."),
		MaxBodyBytes: l.bytes("MAX_BODY_BYTES", 1<<20),

		ReadTimeout:     l.duration("READ_TIMEOUT", 15*time.Second),
		WriteTimeout:    l.duration("WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:     l.duration("IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout: l.duration("SHUTDOWN_TIMEOUT", 10*time.Second),

		AllowPrivateSources: l.boolean("ALLOW_PRIVATE_SOURCES", false),
		HTTPSourceMaxBytes:  l.bytes("HTTP_SOURCE_MAX_BYTES", 50<<20),
	}

	if p, err := strconv.Atoi(cfg.Port); err != nil || p < 1 || p > 65535 {
		l.fail("PORT", cfg.Port, "must be a number between 1 and 65535")
	}
	if info, err := os.Stat(cfg.DataDir); err != nil || !info.IsDir() {
		l.fail("DATA_DIR", cfg.DataDir, "must be an existing directory")
	}

	return cfg, errors.Join(l.errs...)
}

type loader struct {
	getenv func(string) string
	errs   []error
}

func (l *loader) fail(key, value, msg string) {
	l.errs = append(l.errs, fmt.Errorf("%s=%q: %s", key, value, msg))
}

func (l *loader) str(key, def string) string {
	if v := strings.TrimSpace(l.getenv(key)); v != "" {
		return v
	}
	return def
}

func (l *loader) oneOf(key, def string, allowed ...string) string {
	v := strings.ToLower(l.str(key, def))
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	l.fail(key, v, "must be one of "+strings.Join(allowed, ", "))
	return def
}

func (l *loader) level(key string, def slog.Level) slog.Level {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(v)); err != nil {
		l.fail(key, v, "must be debug, info, warn or error")
		return def
	}
	return lvl
}

func (l *loader) duration(key string, def time.Duration) time.Duration {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		l.fail(key, v, "must be a positive duration such as 30s or 2m")
		return def
	}
	return d
}

func (l *loader) bytes(key string, def int64) int64 {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		l.fail(key, v, "must be a positive number of bytes")
		return def
	}
	return n
}

func (l *loader) boolean(key string, def bool) bool {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.fail(key, v, "must be true or false")
		return def
	}
	return b
}
