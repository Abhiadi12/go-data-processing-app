// Package pipeline runs a job's stages concurrently over channels:
// sources → ingest (fan-in) → validate (worker pool) → transform (worker pool) → sink (aggregate).
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

var (
	ErrSourceFailed = errors.New("source failed")
	ErrExportFailed = errors.New("export failed")
)

type StageFunc func(record.Record) (record.Record, []string)

// RecordWriter receives every processed record, from the sink goroutine only
// (export.Set implements it). It may be nil when nothing is exported.
type RecordWriter interface {
	WriteRecord(record.Record) error
}

type Result struct {
	Aggregates []job.AggregateResult
}

func Run(ctx context.Context, spec job.Spec, sources []source.Source, w RecordWriter, c *Counters, errs *Errors) (Result, error) {
	v, err := NewValidator(spec.Schema)
	if err != nil {
		return Result{}, err
	}
	t := NewTransformer(spec.Transforms)
	agg := NewAggregator(spec.Aggregations)

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	o := spec.Options
	records := ingest(ctx, cancel, sources, o.BufferSize, o.ContinueOnSourceError, c, errs)
	valid := runStage(ctx, cancel, "validate", o.ValidateWorkers, o.BufferSize, records, v.Validate, &c.Validate, c, errs)
	transformed := runStage(ctx, cancel, "transform", o.TransformWorkers, o.BufferSize, valid, t.Transform, &c.Transform, c, errs)

	sink(cancel, transformed, w, agg, c)

	if err := context.Cause(ctx); err != nil {
		return Result{}, err
	}
	return Result{Aggregates: agg.Results()}, nil
}

// sink runs in Run's own goroutine: it alone owns the aggregator and the
// writers, so neither needs a lock. A write error or a panic stops the run.
func sink(fail context.CancelCauseFunc, in <-chan record.Record, w RecordWriter, agg *Aggregator, c *Counters) {
	defer recoverTo(fail, "sink")
	for rec := range in {
		start := time.Now()
		if w != nil {
			if err := w.WriteRecord(rec); err != nil {
				fail(fmt.Errorf("%w: %w", ErrExportFailed, err))
				return
			}
		}
		agg.Add(rec)
		c.Sink.Observe(time.Since(start))
		c.Valid.Add(1)
	}
}

func ingest(ctx context.Context, fail context.CancelCauseFunc, sources []source.Source,
	bufSize int, continueOnErr bool, c *Counters, errs *Errors,
) <-chan record.Record {
	out := make(chan record.Record, bufSize)

	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer recoverTo(fail, "source "+src.Name())

			err := src.Read(ctx, func(it source.Item) error {
				c.Read.Add(1)
				if it.Err != nil {
					c.Failed.Add(1)
					errs.Add(job.RecordError{Source: it.Err.Source, Index: it.Err.Index, Stage: "ingest", Reasons: []string{it.Err.Err.Error()}})
					return nil
				}
				select {
				case out <- it.Record:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err == nil || ctx.Err() != nil {
				return
			}

			c.SourceErrors.Add(1)
			errs.Add(job.RecordError{Source: src.Name(), Stage: "source", Reasons: []string{err.Error()}})
			if !continueOnErr {
				fail(fmt.Errorf("%w: %w", ErrSourceFailed, err))
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func runStage(ctx context.Context, fail context.CancelCauseFunc, name string, workers, bufSize int, in <-chan record.Record,
	fn StageFunc, timer *StageTimer, c *Counters, errs *Errors,
) <-chan record.Record {
	out := make(chan record.Record, bufSize)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer recoverTo(fail, name)
			for {
				var rec record.Record
				var ok bool
				select {
				case <-ctx.Done():
					return
				case rec, ok = <-in:
					if !ok {
						return
					}
				}

				start := time.Now()
				res, reasons := fn(rec)
				timer.Observe(time.Since(start))
				if len(reasons) > 0 {
					c.Failed.Add(1)
					errs.Add(job.RecordError{Source: rec.Source, Index: rec.Index, Stage: name, Reasons: reasons, Record: rec.Fields})
					continue
				}

				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
