package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/raster"
	"atlas-render/internal/scene"
	"fmt"
	"time"
)

// Submit records a durable render promise. It resolves the pinned snapshot
// only to validate the request and to check the work budget; the snapshot
// record itself stays the single input source, so the job carries no copy of
// the document.
func (s *Service) Submit(snapshotID string, scale int, key string) (model.Job, error) {
	var out model.Job
	if !scene.ValidID(key) {
		return out, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	fingerprint := model.CanonicalDigest([]any{snapshotID, scale})
	e := s.Store.Update(func(st *model.State) error {
		if r, ok := st.Receipts[key]; ok {
			if r.Fingerprint != fingerprint {
				return model.Conflict("idempotency key used for different request")
			}
			out = st.Jobs[r.JobID]
			return nil
		}
		snap, ok := st.Snapshots[snapshotID]
		if !ok {
			return model.Missing("snapshot not found")
		}
		if snap.State != "active" {
			return model.Conflict("snapshot is revoked")
		}
		if _, e := raster.Compile(snap, scale); e != nil {
			return e
		}
		if pending(st) >= 64 {
			return model.Busy("render queue is full")
		}
		now := time.Now().UTC()
		out = model.Job{ID: model.ID("job", st.Sequence+1), SnapshotID: snapshotID, State: "queued", Scale: scale, Attempt: 1, CreatedAt: now, UpdatedAt: now}
		st.Jobs[out.ID] = out
		st.Receipts[key] = model.Receipt{Key: key, Fingerprint: fingerprint, JobID: out.ID}
		st.Emit("job.queued", out.ID)
		return nil
	})
	return out, e
}

func pending(st *model.State) int {
	n := 0
	for _, j := range st.Jobs {
		if j.State == "queued" || j.State == "running" {
			n++
		}
	}
	return n
}

// Cancel commits the cancelled terminal state and returns the job with the
// attempt number that was running, so the executor can target exactly that
// attempt's handle. The returned job is a stable fact; the executor-owned
// context is not part of it.
func (s *Service) Cancel(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State == "cancelled" {
			out = j
			return nil
		}
		if j.State != "queued" && j.State != "running" {
			return model.Conflict("job has already finished")
		}
		j.State = "cancelled"
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.cancelled", id)
		out = j
		return nil
	})
	return out, e
}

// Retry reopens a failed or cancelled job as a new attempt. The new attempt
// reuses the pinned snapshot reference and scale already stored on the job;
// the previous attempt's executor resources were released when it ended.
func (s *Service) Retry(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State != "failed" && j.State != "cancelled" {
			return model.Conflict("only failed or cancelled jobs can retry")
		}
		if pending(st) >= 64 {
			return model.Busy("render queue is full")
		}
		j.State = "queued"
		j.Error = ""
		j.Attempt++
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.retried", id)
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Purge(id string) error {
	return s.Store.Update(func(st *model.State) error {
		a, ok := st.Artifacts[id]
		if !ok {
			return model.Missing("artifact not found")
		}
		if a.State == "purged" {
			return nil
		}
		a.State = "purged"
		a.PNG = nil
		st.Artifacts[id] = a
		st.Emit("artifact.purged", id)
		return nil
	})
}
func (s *Service) Artifact(id string) (model.Artifact, error) {
	a, ok := s.Store.Read().Artifacts[id]
	if !ok {
		return a, model.Missing("artifact not found")
	}
	if a.State == "purged" {
		return a, model.Gone(fmt.Sprintf("artifact %s was purged", id))
	}
	return a, nil
}
