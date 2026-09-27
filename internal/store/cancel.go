package store

import (
	"fmt"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// CancelJob requests cancellation of one job.
//
//   - PENDING/READY: terminal immediately (CANCELED + reason), no worker will
//     ever run it;
//   - RUNNING: CancelRequested is set durably and the owning worker observes
//     it on its next poll; the terminal CANCELED state is committed by the
//     worker when the attempt actually stops (cooperative cancellation). The
//     job can therefore never be observed both "running" and "canceled";
//   - terminal: ErrConflict, so repeated cancels are idempotent-conflicting
//     and clearly reported.
func (s *FileStore) CancelJob(jobID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.snap.Jobs[jobID]
	if !ok {
		return domain.ErrNotFound
	}
	if j.State.Terminal() {
		return fmt.Errorf("%w: job %s already %s", domain.ErrConflict, jobID, j.State)
	}
	if reason == "" {
		reason = domain.ReasonUserCanceled
	}
	if j.State == domain.StateRunning {
		return s.requestCancelRunningLocked(j, reason, "user cancel requested")
	}
	s.cancelJobLocked(j, reason, "canceled before start", nowTime(), true)
	s.finalizeGroupIfDoneLocked(j.GroupID)
	return nil
}

// CancelGroup cancels every non-terminal member:
//
//   - if the group is still PENDING (barrier closed) the whole batch is
//     rejected-style and the group becomes CANCELED at once — no member ever
//     started, which cannot leave a half-running batch;
//   - if RUNNING, waiting members terminate immediately and running members
//     are cooperatively signaled; the group becomes terminal (FAILED if a
//     member failed, otherwise CANCELED) once all members settle.
func (s *FileStore) CancelGroup(groupID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.snap.Groups[groupID]
	if !ok {
		return domain.ErrNotFound
	}
	if g.State == domain.GroupSucceeded || g.State == domain.GroupFailed || g.State == domain.GroupCanceled {
		return fmt.Errorf("%w: group %s already %s", domain.ErrConflict, groupID, g.State)
	}
	if reason == "" {
		reason = domain.ReasonUserCanceled
	}
	now := nowTime()

	if g.State == domain.GroupPending {
		s.emitLocked(domain.Event{
			Type: domain.EvGroupCanceled, GroupID: groupID,
			Detail: fmt.Sprintf("canceled behind barrier: %s", reason),
		})
		for _, id := range g.JobIDs {
			if j := s.snap.Jobs[id]; j != nil && !j.State.Terminal() {
				s.cancelJobLocked(j, reason, "group canceled before admission", now, true)
			}
		}
		g.State = domain.GroupCanceled
		g.Reason = reason
		g.FinishedAt = now
		s.persistLocked()
		return nil
	}

	// RUNNING group.
	s.emitLocked(domain.Event{
		Type: domain.EvGroupCanceled, GroupID: groupID,
		Detail: fmt.Sprintf("cancel requested: %s", reason),
	})
	for _, id := range g.JobIDs {
		j := s.snap.Jobs[id]
		if j == nil || j.State.Terminal() {
			continue
		}
		if j.State == domain.StateRunning {
			_ = s.requestCancelRunningLocked(j, reason, "group cancel requested")
		} else {
			s.cancelJobLocked(j, reason, "group canceled", now, true)
		}
	}
	s.finalizeGroupIfDoneLocked(groupID)
	return nil
}

// requestCancelRunningLocked durably flags a running job and emits a
// cancel-request event. Caller holds mu.
func (s *FileStore) requestCancelRunningLocked(j *domain.Job, reason, detail string) error {
	j.CancelRequested = true
	j.CancelReason = reason
	s.emitLocked(domain.Event{
		Type:     domain.EvJobCancelReq,
		GroupID:  j.GroupID,
		JobID:    j.ID,
		WorkerID: workerOf(j),
		Token:    tokenOf(j),
		Detail:   fmt.Sprintf("reason=%s: %s", reason, detail),
	})
	// The cancel request is durable in the log; force a snapshot too so the
	// flag survives even a process restart mid-attempt.
	if err := s.persistLocked(); err != nil {
		return err
	}
	return nil
}

// cancelJobLocked moves a non-running job straight to CANCELED. Caller holds
// mu. If emitJobEvent is false no per-job event is emitted (unused currently
// beyond call-site clarity).
func (s *FileStore) cancelJobLocked(j *domain.Job, reason, message string, at time.Time, emitJobEvent bool) {
	j.State = domain.StateCanceled
	j.Reason = reason
	j.Message = message
	j.FinishedAt = at
	j.CancelRequested = false
	j.CancelReason = ""
	s.emitLocked(domain.Event{
		Type:    domain.EvJobCanceled,
		GroupID: j.GroupID,
		JobID:   j.ID,
		Detail:  fmt.Sprintf("reason=%s: %s", reason, message),
	})
}

func workerOf(j *domain.Job) string {
	if a := j.InFlightAttempt(); a != nil {
		return a.WorkerID
	}
	return ""
}

func tokenOf(j *domain.Job) int64 {
	if a := j.InFlightAttempt(); a != nil {
		return a.Token
	}
	return 0
}
