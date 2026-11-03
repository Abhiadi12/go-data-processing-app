package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/api"
	"github.com/Abhiadi12/go-data-processing-app/internal/config"
	"github.com/Abhiadi12/go-data-processing-app/internal/export"
	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
	"github.com/Abhiadi12/go-data-processing-app/internal/runner"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

func main() {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration:\n%v\n", err)
		os.Exit(2)
	}

	logger := logging.New(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)

	if err := run(cfg, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, logger *slog.Logger) error {
	dataRoot, err := os.OpenRoot(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("open data dir: %w", err)
	}
	defer dataRoot.Close()

	db, err := export.OpenSQLite(context.Background(), cfg.SQLitePath)
	if err != nil {
		return err
	}
	defer db.Close() // deferred, so it runs after the runner has finished in the return below

	store := job.NewMemoryStore()
	jobs := runner.New(runner.Options{
		Store:  store,
		Logger: logger,
		Deps: job.SourceDeps{
			Files:        dataRoot.FS(),
			Client:       source.NewHTTPClient(cfg.AllowPrivateSources),
			HTTPMaxBytes: cfg.HTTPSourceMaxBytes,
		},
		Exports:   export.Deps{OutputDir: cfg.OutputDir, DB: db},
		Workers:   cfg.JobWorkers,
		QueueSize: cfg.JobQueueSize,
		JobTTL:    cfg.JobTTL,
	})
	jobs.Start()

	handler := api.NewHandler(api.Options{
		Logger:       logger,
		Store:        store,
		Runner:       jobs,
		Files:        dataRoot.FS(),
		MaxBodyBytes: cfg.MaxBodyBytes,
		APIKey:       cfg.APIKey,
		CORSOrigins:  cfg.CORSOrigins,
	})

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", srv.Addr, "data_dir", cfg.DataDir, "log_level", cfg.LogLevel.String(),
			"job_workers", cfg.JobWorkers, "job_queue_size", cfg.JobQueueSize,
			"output_dir", cfg.OutputDir, "sqlite_path", cfg.SQLitePath, "job_ttl", cfg.JobTTL.String(),
			"api_key_required", cfg.APIKey != "", "cors_origins", cfg.CORSOrigins)
		errCh <- srv.ListenAndServe()
	}()

	var serveErr error
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = err
		}
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	// Stop accepting requests first, so no new jobs are submitted,
	// then cancel running jobs and wait for them to record their status.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(serveErr, srv.Shutdown(shutdownCtx), jobs.Shutdown(shutdownCtx))
}
