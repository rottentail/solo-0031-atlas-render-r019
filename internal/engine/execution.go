package engine

import (
	"context"
	"sync"
)

// execution owns the transient resources of one render attempt. Durable facts
// (pinned snapshot, scale, attempt number, state) live in model.Job; the
// context and its cancel handle exist only while this process executes the
// attempt, and are never shared with another attempt of the same job.
type execution struct {
	jobID   string
	attempt int
	ctx     context.Context
	cancel  context.CancelFunc
}

type executionKey struct {
	jobID   string
	attempt int
}

// executionSet is the executor's handle registry. Entries are keyed by attempt
// identity so releasing or cancelling one attempt can never touch a newer one.
type executionSet struct {
	mu   sync.Mutex
	live map[executionKey]*execution
}

func newExecutionSet() *executionSet {
	return &executionSet{live: map[executionKey]*execution{}}
}

// begin creates the cancellable context for a claimed attempt and records it
// under that attempt's identity. The caller becomes the sole owner and must
// call end exactly once.
func (s *executionSet) begin(parent context.Context, jobID string, attempt int) *execution {
	ctx, cancel := context.WithCancel(parent)
	e := &execution{jobID: jobID, attempt: attempt, ctx: ctx, cancel: cancel}
	s.mu.Lock()
	s.live[executionKey{jobID: jobID, attempt: attempt}] = e
	s.mu.Unlock()
	return e
}

// end releases exactly the handle created by begin. If a retry already
// registered a newer attempt of the same job, that attempt keeps its own
// context and cancel handle.
func (s *executionSet) end(e *execution) {
	s.mu.Lock()
	key := executionKey{jobID: e.jobID, attempt: e.attempt}
	if s.live[key] == e {
		delete(s.live, key)
	}
	s.mu.Unlock()
	e.cancel()
}

// cancel aborts the execution registered for this exact attempt identity.
// A notification for an attempt that already ended finds no entry and cannot
// reach a newer attempt of the same job.
func (s *executionSet) cancel(jobID string, attempt int) {
	s.mu.Lock()
	e := s.live[executionKey{jobID: jobID, attempt: attempt}]
	s.mu.Unlock()
	if e != nil {
		e.cancel()
	}
}

// all snapshots the live attempts without holding the registry lock while
// durable state is consulted.
func (s *executionSet) all() []*execution {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*execution, 0, len(s.live))
	for _, e := range s.live {
		out = append(out, e)
	}
	return out
}

// cancelAll aborts every execution this process owns. Handles stay registered
// until their owner calls end, so cancellation remains idempotent.
func (s *executionSet) cancelAll() {
	for _, e := range s.all() {
		e.cancel()
	}
}

func (s *executionSet) size() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.live)
}
