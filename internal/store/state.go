package store

import (
	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// state is the in-memory, fully-deterministic content of one journal
// record. The whole state is journaled on every commit, so recovery is
// simply "take the last good record".
type state struct {
	Seq      int64                `json:"seq"`
	GroupIDs []string             `json:"group_ids"`
	JobIDs   []string             `json:"job_ids"`
	Groups   map[string]job.Group `json:"groups"`
	Jobs     map[string]job.Job   `json:"jobs"`
	Events   []job.Event          `json:"events"`
}

func newState() *state {
	return &state{
		Groups: map[string]job.Group{},
		Jobs:   map[string]job.Job{},
	}
}

func (s *state) clone() *state {
	c := &state{
		Seq:      s.Seq,
		GroupIDs: append([]string(nil), s.GroupIDs...),
		JobIDs:   append([]string(nil), s.JobIDs...),
		Groups:   make(map[string]job.Group, len(s.Groups)),
		Jobs:     make(map[string]job.Job, len(s.Jobs)),
		Events:   append([]job.Event(nil), s.Events...),
	}
	for k, v := range s.Groups {
		c.Groups[k] = v
	}
	for k, v := range s.Jobs {
		// Defensive copy of slices inside specs.
		v.Spec.Args = append([]string(nil), v.Spec.Args...)
		v.Spec.Env = append([]string(nil), v.Spec.Env...)
		c.Jobs[k] = v
	}
	return c
}

// viewImpl implements View over a state snapshot.
type viewImpl struct {
	st *state
}

func (v viewImpl) Groups() []job.Group {
	out := make([]job.Group, 0, len(v.st.GroupIDs))
	for _, id := range v.st.GroupIDs {
		out = append(out, v.st.Groups[id])
	}
	return out
}

func (v viewImpl) Jobs() []job.Job {
	out := make([]job.Job, 0, len(v.st.JobIDs))
	for _, id := range v.st.JobIDs {
		out = append(out, v.st.Jobs[id])
	}
	return out
}

func (v viewImpl) Group(id string) (job.Group, bool) {
	g, ok := v.st.Groups[id]
	return g, ok
}

func (v viewImpl) Job(id string) (job.Job, bool) {
	j, ok := v.st.Jobs[id]
	return j, ok
}

func (v viewImpl) JobsOfGroup(groupID string) []job.Job {
	out := make([]job.Job, 0)
	for _, id := range v.st.JobIDs {
		if j := v.st.Jobs[id]; j.GroupID == groupID {
			out = append(out, j)
		}
	}
	return out
}

func (v viewImpl) Release() {}

// txImpl implements Tx: mutations land on a working clone; the caller
// swaps it in only when the function succeeds (all-or-nothing semantics).
type txImpl struct {
	viewImpl
	pending []job.Event
}

func (t *txImpl) InsertGroup(g job.Group) {
	if _, ok := t.st.Groups[g.ID]; ok {
		panic("store: duplicate group insert: " + g.ID)
	}
	t.st.Groups[g.ID] = g
	t.st.GroupIDs = append(t.st.GroupIDs, g.ID)
}

func (t *txImpl) InsertJob(j job.Job) {
	if _, ok := t.st.Jobs[j.ID]; ok {
		panic("store: duplicate job insert: " + j.ID)
	}
	t.st.Jobs[j.ID] = j
	t.st.JobIDs = append(t.st.JobIDs, j.ID)
}

func (t *txImpl) UpdateGroup(g job.Group) {
	if _, ok := t.st.Groups[g.ID]; !ok {
		panic("store: update of unknown group: " + g.ID)
	}
	t.st.Groups[g.ID] = g
}

func (t *txImpl) UpdateJob(j job.Job) {
	if _, ok := t.st.Jobs[j.ID]; !ok {
		panic("store: update of unknown job: " + j.ID)
	}
	t.st.Jobs[j.ID] = j
}

func (t *txImpl) Emit(e job.Event) {
	t.pending = append(t.pending, e)
}
