package pipeline

import (
	"context"
	"fmt"
	"runtime/debug"
)

// PanicError is a panic recovered while a job ran. The job fails with it
// instead of the panic crashing the whole server. Stack is for the logs;
// Error() is what the API shows.
type PanicError struct {
	Stage string
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("internal error in %s: %v", e.Stage, e.Value)
}

// NewPanicError records a recovered panic value with the current stack.
// Call it from the deferred function that called recover.
func NewPanicError(stage string, v any) *PanicError {
	return &PanicError{Stage: stage, Value: v, Stack: debug.Stack()}
}

// recoverTo is deferred at the top of every pipeline goroutine. A panic there
// would end the whole process, because recover only works in the goroutine
// that panicked; here it cancels the run instead, and every other goroutine
// stops through ctx.Done like on any other failure.
func recoverTo(fail context.CancelCauseFunc, stage string) {
	if v := recover(); v != nil {
		fail(NewPanicError(stage, v))
	}
}
