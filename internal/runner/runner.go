// Package runner executes jobs in the background: a bounded queue of job IDs
// served by a fixed number of worker goroutines.
package runner

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/export"
	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/logging"
	"github.com/Abhiadi12/go-data-processing-app/internal/pipeline"
)

var (
	ErrQueueFull   = errors.New("job queue is full")
	ErrStopped     = errors.New("runner is shutting down")
	ErrJobFinished = errors.New("job has already finished")
	ErrJobActive   = errors.New("job is still pending or running")
	ErrBusy        = errors.New("the database is busy with another job")

	errCancelledByUser = errors.New("cancelled by user")
	errShutdown        = errors.New("server shutting down")
	errNotPending      = errors.New("job is no longer pending")
)

type Options struct {
	Store            job.Store
	Deps             job.SourceDeps
	Exports          export.Deps
	Logger           *slog.Logger
	Workers          int
	QueueSize        int
	ProgressInterval time.Duration

	JobTTL        time.Duration
	EvictInterval time.Duration
}

const deleteTimeout = 5 * time.Second

type Runner struct {
	store      job.Store
	deps       job.SourceDeps
	exports    export.Deps
	logger     *slog.Logger
	workers    int
	interval   time.Duration
	ttl        time.Duration
	evictEvery time.Duration

	queue chan string
	base  context.Context
	stop  context.CancelCauseFunc
	wg    sync.WaitGroup

	mu      sync.Mutex
	running map[string]context.CancelCauseFunc
}

func New(opts Options) *Runner {
	base, stop := context.WithCancelCause(context.Background())
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = 500 * time.Millisecond
	}
	if opts.EvictInterval <= 0 {
		opts.EvictInterval = time.Minute
	}
	return &Runner{
		store:      opts.Store,
		deps:       opts.Deps,
		exports:    opts.Exports,
		logger:     opts.Logger,
		workers:    opts.Workers,
		interval:   opts.ProgressInterval,
		ttl:        opts.JobTTL,
		evictEvery: opts.EvictInterval,
		queue:      make(chan string, opts.QueueSize),
		base:       base,
		stop:       stop,
		running:    make(map[string]context.CancelCauseFunc),
	}
}

// Start launches the worker goroutines and, when a TTL is set, the eviction
// loop. Call it once.
func (r *Runner) Start() {
	for range r.workers {
		r.wg.Add(1)
		go r.work()
	}
	if r.ttl > 0 {
		r.wg.Add(1)
		go r.evictLoop()
	}
}

// Submit queues a job without blocking. It returns ErrQueueFull when every
// worker is busy and the queue is full, so the API can answer 503.
func (r *Runner) Submit(id string) error {
	if r.base.Err() != nil {
		return ErrStopped
	}
	select {
	case r.queue <- id:
		return nil
	default:
		return ErrQueueFull
	}
}

// Cancel stops a job. A pending job is cancelled at once; a running job is
// signalled and reaches status cancelled when its pipeline has stopped.
func (r *Runner) Cancel(ctx context.Context, id string) (job.Job, error) {
	var wasRunning bool
	j, err := r.store.Update(ctx, id, func(j *job.Job) error {
		switch j.Status {
		case job.StatusPending:
			now := time.Now().UTC()
			j.Status, j.FinishedAt, j.Error = job.StatusCancelled, &now, errCancelledByUser.Error()
		case job.StatusRunning:
			wasRunning = true
		default:
			return ErrJobFinished
		}
		return nil
	})
	if err != nil {
		return job.Job{}, err
	}

	if wasRunning {
		r.mu.Lock()
		cancel := r.running[id]
		r.mu.Unlock()
		if cancel != nil {
			cancel(errCancelledByUser)
		}
	}
	return j, nil
}

// Shutdown stops taking jobs, cancels running ones and waits for the workers
// to finish recording their final status, or for ctx to expire.
// Jobs still in the queue stay pending; nothing new is started.
func (r *Runner) Shutdown(ctx context.Context) error {
	r.stop(errShutdown)

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("runner shutdown: %w", ctx.Err())
	}
}

func (r *Runner) work() {
	defer r.wg.Done()
	for {
		select {
		case <-r.base.Done():
			return
		case id := <-r.queue:
			if r.base.Err() != nil {
				return
			}
			r.run(id)
		}
	}
}

func (r *Runner) Delete(ctx context.Context, id string) error {
	j, err := r.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if !j.Finished() {
		return ErrJobActive
	}

	removeCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()
	if err := export.Remove(removeCtx, j, r.exports); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrBusy
		}
		return fmt.Errorf("remove outputs: %w", err)
	}
	return r.store.Delete(ctx, id)
}

func (r *Runner) evictLoop() {
	defer r.wg.Done()
	t := time.NewTicker(r.evictEvery)
	defer t.Stop()
	for {
		select {
		case <-r.base.Done():
			return
		case now := <-t.C:
			r.evict(now)
		}
	}
}
func (r *Runner) evict(now time.Time) {
	jobs, err := r.store.List(r.base)
	if err != nil {
		r.logger.Warn("list jobs for eviction", "err", err)
		return
	}
	evicted := 0
	for _, j := range jobs {
		if !j.Finished() || j.FinishedAt == nil || now.Sub(*j.FinishedAt) < r.ttl {
			continue
		}
		if err := r.store.Delete(r.base, j.ID); err != nil && !errors.Is(err, job.ErrNotFound) {
			r.logger.Warn("evict job", "job_id", j.ID, "err", err)
			continue
		}
		evicted++
	}
	if evicted > 0 {
		r.logger.Info("evicted finished jobs", "count", evicted, "ttl", r.ttl.String())
	}
}

func (r *Runner) run(id string) {
	ctx, cancel := context.WithCancelCause(r.base)
	defer cancel(nil)

	// Register the cancel func before the job is marked running, so a Cancel
	// that sees "running" always finds it.
	r.mu.Lock()
	r.running[id] = cancel
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.running, id)
		r.mu.Unlock()
	}()

	j, err := r.store.Update(ctx, id, func(j *job.Job) error {
		if j.Status != job.StatusPending {
			return errNotPending
		}
		now := time.Now().UTC()
		j.Status, j.StartedAt = job.StatusRunning, &now
		return nil
	})
	if err != nil {
		if !errors.Is(err, errNotPending) {
			r.logger.Error("start job", "job_id", id, "err", err)
		}
		return
	}

	ctx = logging.WithJobID(logging.WithRequestID(ctx, j.RequestID), j.ID)
	r.logger.InfoContext(ctx, "job started", "name", j.Name)

	timeout := time.Duration(j.Spec.Options.TimeoutSeconds) * time.Second
	runCtx, cancelTimeout := context.WithTimeout(ctx, timeout)
	defer cancelTimeout()

	counters := &pipeline.Counters{}
	errs := pipeline.NewErrors(j.Spec.Options.MaxStoredErrors)

	stopProgress := r.reportProgress(ctx, id, counters)
	res, runErr := r.execute(runCtx, j, counters, errs)
	stopProgress()

	status, msg := outcome(runErr, timeout)
	var pe *pipeline.PanicError
	if errors.As(runErr, &pe) {
		r.logger.ErrorContext(ctx, "job panicked", "stage", pe.Stage, "panic", pe.Value, "stack", string(pe.Stack))
	}
	failed, _ := errs.Snapshot()
	slices.SortFunc(failed, func(a, b job.RecordError) int {
		return cmp.Or(cmp.Compare(a.Source, b.Source), cmp.Compare(a.Index, b.Index))
	})

	final, err := r.store.Update(context.WithoutCancel(ctx), id, func(j *job.Job) error {
		now := time.Now().UTC()
		j.Status, j.Error, j.FinishedAt = status, msg, &now
		j.Progress = counters.Snapshot()
		j.Stages = counters.Stages()
		j.FailedRecords = failed
		if status == job.StatusCompleted {
			j.Aggregates = res.Aggregates
			j.Outputs = res.Outputs
		}
		return nil
	})
	if err != nil {
		r.logger.ErrorContext(ctx, "save job result", "err", err)
		return
	}

	r.logger.InfoContext(ctx, "job finished",
		"status", final.Status,
		"error", final.Error,
		"duration_ms", final.FinishedAt.Sub(*final.StartedAt).Milliseconds(),
		"records_read", final.Progress.RecordsRead,
		"records_valid", final.Progress.RecordsValid,
		"records_failed", final.Progress.RecordsFailed,
	)
}

type result struct {
	pipeline.Result
	Outputs []job.Output
}

// execute runs the pipeline with the job's exports open, then commits them.
// Any failure (or cancel) aborts the exports, so no partial output is left.
// A panic here (outside the pipeline's own goroutines, which recover their
// own) fails this job instead of crashing the server.
func (r *Runner) execute(ctx context.Context, j job.Job, c *pipeline.Counters, errs *pipeline.Errors) (res result, err error) {
	var set *export.Set
	defer func() {
		if v := recover(); v != nil {
			if set != nil {
				set.Abort()
			}
			res, err = result{}, pipeline.NewPanicError("job", v)
		}
	}()

	srcs, err := job.BuildSources(j.Spec.Sources, r.deps)
	if err != nil {
		return result{}, err
	}

	set, err = export.Open(ctx, j, r.exports)
	if err != nil {
		return result{}, fmt.Errorf("%w: %w", pipeline.ErrExportFailed, err)
	}

	out, err := pipeline.Run(ctx, j.Spec, srcs, set, c, errs)
	if err != nil {
		set.Abort()
		return result{}, err
	}

	outputs, err := set.Commit(ctx, out.Aggregates)
	if err != nil {
		return result{}, fmt.Errorf("%w: %w", pipeline.ErrExportFailed, err)
	}
	return result{Result: out, Outputs: outputs}, nil
}

func (r *Runner) reportProgress(ctx context.Context, id string, c *pipeline.Counters) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		defer close(stopped)
		t := time.NewTicker(r.interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_, err := r.store.Update(ctx, id, func(j *job.Job) error {
					j.Progress = c.Snapshot()
					j.Stages = c.Stages()
					return nil
				})
				if err != nil {
					r.logger.WarnContext(ctx, "update progress", "err", err)
				}
			}
		}
	}()

	return func() {
		close(done)
		<-stopped
	}
}

func outcome(err error, timeout time.Duration) (job.Status, string) {
	switch {
	case err == nil:
		return job.StatusCompleted, ""
	case errors.Is(err, errCancelledByUser), errors.Is(err, errShutdown):
		return job.StatusCancelled, err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return job.StatusFailed, fmt.Sprintf("timed out after %s", timeout)
	default:
		return job.StatusFailed, err.Error()
	}
}
