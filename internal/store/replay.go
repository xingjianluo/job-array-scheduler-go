package store

import (
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// applyEvent replays one event on top of the in-memory snapshot.
//
// The snapshot is atomically rewritten after every structural state
// transition, so at restart almost everything is already present. The only
// events that may be newer than the snapshot are:
//
//   - job_heartbeat / job_cancel_requested: emitted without snapshot rewrite
//     on the hot path; replay patches heartbeat time / cancel flag;
//   - job_lease_expired: only when the crash happened in the tiny window
//     between log append and snapshot rewrite.
//
// Replay is idempotent: snapshot rewriting after each transition guarantees
// the same prefix and these patches converge.
func (s *FileStore) applyEvent(ev domain.Event) {
	switch ev.Type {
	case domain.EvJobHeartbeat:
		if j := s.snap.Jobs[ev.JobID]; j != nil {
			if a := j.InFlightAttempt(); a != nil && a.Token == ev.Token {
				a.LastHeartbeat = ev.Time
			}
		}
	case domain.EvJobCancelReq:
		if j := s.snap.Jobs[ev.JobID]; j != nil {
			j.CancelRequested = true
		}
	case domain.EvJobLeaseExpired:
		s.replayLeaseExpired(ev)
	}
}

// replayLeaseExpired applies the requeue of a dead attempt during log replay.
func (s *FileStore) replayLeaseExpired(ev domain.Event) {
	j := s.snap.Jobs[ev.JobID]
	if j == nil {
		return
	}
	a := j.InFlightAttempt()
	if a == nil || a.Token != ev.Token {
		return // already superseded by a newer transition
	}
	a.EndedAt = ev.Time
	a.State = domain.StateFailed
	a.Reason = "lease_expired"
	a.Message = "worker heartbeat timed out; attempt requeued"
	j.State = domain.StateReady
	j.NextAttemptAt = ev.Time
	j.CancelRequested = false
	j.CancelReason = ""
}

var _ = time.Time{}
