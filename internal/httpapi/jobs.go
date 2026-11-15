package httpapi

import (
	"atlas-render/internal/model"
	"atlas-render/internal/observe"
	"net/http"
)

// jobView preserves the public job detail shape. Tasks no longer persist the
// pinned input copy, but the documented response still carries an empty
// placeholder payload field; input details remain readable through the
// snapshot endpoint.
type jobView struct {
	model.Job
	Payload model.Snapshot `json:"payload"`
}

func present(j model.Job) jobView { return jobView{Job: j} }

func (a API) submit(w http.ResponseWriter, r *http.Request) {
	var b struct {
		SnapshotID string `json:"snapshotId"`
		Scale      int    `json:"scale"`
	}
	if e := decode(w, r, &b); e != nil {
		fail(w, e)
		return
	}
	v, e := a.Service.Submit(b.SnapshotID, b.Scale, r.Header.Get("Idempotency-Key"))
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 202, present(v))
}
func (a API) getJob(w http.ResponseWriter, r *http.Request) {
	v, ok := a.Service.Store.Read().Jobs[r.PathValue("id")]
	if !ok {
		fail(w, model.Missing("job not found"))
		return
	}
	write(w, 200, present(v))
}
func (a API) cancel(w http.ResponseWriter, r *http.Request) {
	v, e := a.Service.Cancel(r.PathValue("id"))
	if e != nil {
		fail(w, e)
		return
	}
	// The durable cancel names the attempt that was current when it committed.
	// The executor maps that identity onto its own handle, so a newer retry is
	// never touched by this notification.
	a.Worker.CancelAttempt(v.ID, v.Attempt)
	write(w, 200, present(v))
}
func (a API) retry(w http.ResponseWriter, r *http.Request) {
	v, e := a.Service.Retry(r.PathValue("id"))
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 202, present(v))
}
func (a API) download(w http.ResponseWriter, r *http.Request) {
	v, e := a.Service.Artifact(r.PathValue("id"))
	if e != nil {
		fail(w, e)
		return
	}
	etag := `"` + v.Digest + `"`
	w.Header().Set("ETag", etag)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(304)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "private, no-cache")
	_, _ = w.Write(v.PNG)
}
func (a API) purge(w http.ResponseWriter, r *http.Request) {
	if e := a.Service.Purge(r.PathValue("id")); e != nil {
		fail(w, e)
		return
	}
	write(w, 200, map[string]string{"state": "purged"})
}
func (a API) compare(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Left  string `json:"left"`
		Right string `json:"right"`
	}
	if e := decode(w, r, &b); e != nil {
		fail(w, e)
		return
	}
	left, e := a.Service.Artifact(b.Left)
	if e != nil {
		fail(w, e)
		return
	}
	right, e := a.Service.Artifact(b.Right)
	if e != nil {
		fail(w, e)
		return
	}
	v, e := observe.Compare(left, right)
	if e != nil {
		fail(w, e)
		return
	}
	write(w, 200, v)
}
func (a API) status(w http.ResponseWriter, r *http.Request) {
	summary := observe.Summary(a.Service.Store.Read())
	// Execution handles belong to this process, not to durable state, so the
	// count is added by the entry point rather than derived from state.
	summary["activeExecutions"] = a.Worker.ActiveExecutions()
	write(w, 200, summary)
}
func (a API) events(w http.ResponseWriter, r *http.Request) {
	write(w, 200, a.Service.Store.Read().Events)
}
