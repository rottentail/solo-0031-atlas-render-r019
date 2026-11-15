package engine

import (
	"atlas-render/internal/codec"
	"atlas-render/internal/model"
	"atlas-render/internal/persist"
	"atlas-render/internal/raster"
	"context"
	"errors"
	"log"
	"sort"
	"time"
)

// Worker drains the durable queue. It owns every transient resource of a
// running attempt: the attempt context and cancel handle, the compiled plan
// and the pixel buffers. None of those enter the persisted state; the durable
// Job only carries identity, the pinned snapshot reference, scale, attempt
// number and terminal bookkeeping.
type Worker struct {
	store *persist.Store
	execs *executionSet
}

func New(st *persist.Store) *Worker {
	return &Worker{store: st, execs: newExecutionSet()}
}

// Recover requeues attempts that were running when the previous process
// stopped. Recovery is separate from the consumption loop so that startup can
// finish validating durable facts before any attempt is claimed.
func (w *Worker) Recover() error {
	return w.store.Update(func(s *model.State) error {
		for id, j := range s.Jobs {
			if j.State == "running" {
				j.State = "queued"
				j.UpdatedAt = time.Now().UTC()
				s.Jobs[id] = j
				s.Emit("job.recovered", id)
			}
		}
		return nil
	})
}

// CancelAttempt aborts the execution registered for exactly this attempt
// identity. A stale notification for an attempt that already finished is a
// no-op, and it can never reach a newer attempt of the same job.
func (w *Worker) CancelAttempt(jobID string, attempt int) {
	w.execs.cancel(jobID, attempt)
}

// ActiveExecutions reports how many attempt handles this process currently
// owns. It is a diagnostic for resource ownership.
func (w *Worker) ActiveExecutions() int { return w.execs.size() }

func (w *Worker) Run(ctx context.Context) {
	defer w.execs.cancelAll()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if e := w.step(ctx); e != nil {
				log.Printf("render worker: %v", e)
			}
		}
	}
}

// attempt is the executor's view of one claimed try. It pins the snapshot
// reference resolved during the claim transaction so that a later retry or
// snapshot state change cannot alter what this attempt renders.
type attempt struct {
	jobID  string
	number int
	scale  int
	input  model.Snapshot
}

// current reports whether the durable job still names this attempt as the one
// allowed to commit. This is the single guard against stale results.
func (a attempt) current(j model.Job) bool {
	return j.State == "running" && j.Attempt == a.number
}

func (w *Worker) claim() (attempt, bool, error) {
	var out attempt
	found := false
	state := w.store.Read()
	ids := make([]string, 0, len(state.Jobs))
	for id, j := range state.Jobs {
		if j.State == "queued" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return out, false, nil
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := state.Jobs[ids[i]], state.Jobs[ids[j]]
		if a.CreatedAt.Equal(b.CreatedAt) {
			return a.ID < b.ID
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	e := w.store.Update(func(s *model.State) error {
		j := s.Jobs[ids[0]]
		if j.State != "queued" {
			return nil
		}
		input, ok := s.Snapshots[j.SnapshotID]
		if !ok {
			// Unreachable through validated state; fail the promise rather
			// than spinning on a queue entry that can never render.
			j.State = "failed"
			j.Error = "pinned snapshot is missing"
			j.UpdatedAt = time.Now().UTC()
			s.Jobs[j.ID] = j
			s.Emit("job.failed", j.ID)
			return nil
		}
		j.State = "running"
		j.UpdatedAt = time.Now().UTC()
		s.Jobs[j.ID] = j
		s.Emit("job.running", j.ID)
		out = attempt{jobID: j.ID, number: j.Attempt, scale: j.Scale, input: input}
		found = true
		return nil
	})
	return out, found, e
}

// step claims at most one queued attempt, executes it with a context owned by
// this attempt, and commits the outcome only while the attempt is still the
// current one.
func (w *Worker) step(parent context.Context) error {
	a, ok, e := w.claim()
	if e != nil || !ok {
		return e
	}
	ex := w.execs.begin(parent, a.jobID, a.number)
	defer w.execs.end(ex)

	// A cancel request can commit between the claim and handle registration.
	// Such a request found no handle to abort, so re-check the durable job
	// now that this attempt is registered; the abort is otherwise lost. The
	// attempt can never commit regardless, because finish re-checks identity.
	if !a.current(w.store.Read().Jobs[a.jobID]) {
		ex.cancel()
	}

	data, width, height, runErr := execute(ex.ctx, a)

	// A process shutdown cancels the attempt context through its parent. The
	// durable job keeps its running state so the next process requeues it;
	// this attempt no longer owns any commit right.
	if parent.Err() != nil {
		return nil
	}
	return w.finish(a, data, width, height, runErr)
}

func execute(ctx context.Context, a attempt) ([]byte, int, int, error) {
	plan, e := raster.Compile(a.input, a.scale)
	if e != nil {
		return nil, 0, 0, e
	}
	img, e := plan.Render(ctx)
	if e != nil {
		return nil, 0, 0, e
	}
	data, e := codec.Encode(img)
	if e != nil {
		return nil, 0, 0, e
	}
	return data, plan.Width, plan.Height, nil
}

// finish writes the outcome of one attempt. The attempt identity check happens
// inside the same transaction as the write, so an execution that lost its
// attempt (through cancel and retry) cannot publish a result or a terminal
// state over the new attempt.
func (w *Worker) finish(a attempt, data []byte, width, height int, runErr error) error {
	return w.store.Update(func(s *model.State) error {
		j := s.Jobs[a.jobID]
		if !a.current(j) {
			return nil
		}
		j.UpdatedAt = time.Now().UTC()
		if runErr != nil {
			j.Error = runErr.Error()
			if errors.Is(runErr, context.Canceled) {
				j.State = "cancelled"
			} else {
				j.State = "failed"
			}
			s.Jobs[j.ID] = j
			s.Emit("job."+j.State, j.ID)
			return nil
		}
		artifact := model.Artifact{ID: model.ID("png", s.Sequence+1), JobID: j.ID, State: "live", Digest: model.Digest(data), Width: width, Height: height, PNG: data}
		s.Artifacts[artifact.ID] = artifact
		j.State = "succeeded"
		j.ArtifactID = artifact.ID
		s.Jobs[j.ID] = j
		s.Emit("job.succeeded", j.ID)
		return nil
	})
}
