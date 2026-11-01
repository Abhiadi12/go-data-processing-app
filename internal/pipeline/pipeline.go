// Package pipeline runs a job's stages concurrently over channels:
// sources → ingest (fan-in) → validate (worker pool) → sink.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Abhiadi12/go-data-processing-app/internal/job"
	"github.com/Abhiadi12/go-data-processing-app/internal/record"
	"github.com/Abhiadi12/go-data-processing-app/internal/source"
)

var ErrSourceFailed = errors.New("source failed")

// StageFunc processes one record. It returns the new record, or the reasons
// the record is rejected. It must be safe to call from many goroutines.
type StageFunc func(record.Record) (record.Record, []string)

// Run reads every source, validates each record and counts the results.
// It returns nil when all input was processed, or why it stopped early:
// the context's cause (cancel, timeout) or a wrapped ErrSourceFailed.
func Run(ctx context.Context, spec job.Spec, sources []source.Source, c *Counters, errs *Errors) error {
	v, err := NewValidator(spec.Schema)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	o := spec.Options
	records := ingest(ctx, cancel, sources, o.BufferSize, o.ContinueOnSourceError, c, errs)
	valid := runStage(ctx, "validate", o.ValidateWorkers, o.BufferSize, records, v.Validate, c, errs)

	for range valid {
		c.Valid.Add(1)
	}

	return context.Cause(ctx)
}

// ingest starts one goroutine per source and merges their records into one
// channel (fan-in). Row errors are counted and stored, not sent on.
// A failing source cancels the whole run unless continueOnErr is set.
// ingest owns the returned channel and closes it after every source returns.
func ingest(ctx context.Context, fail context.CancelCauseFunc, sources []source.Source,
	bufSize int, continueOnErr bool, c *Counters, errs *Errors,
) <-chan record.Record {
	out := make(chan record.Record, bufSize)

	var wg sync.WaitGroup
	for _, src := range sources {
		wg.Add(1)
		go func() {
			defer wg.Done()

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

// runStage starts workers goroutines that all read from in (fan-out) and apply
// fn. Rejected records are counted and stored; the rest go to the returned
// channel, which runStage owns and closes once every worker has stopped.
func runStage(ctx context.Context, name string, workers, bufSize int, in <-chan record.Record,
	fn StageFunc, c *Counters, errs *Errors,
) <-chan record.Record {
	out := make(chan record.Record, bufSize)

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
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

				res, reasons := fn(rec)
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
