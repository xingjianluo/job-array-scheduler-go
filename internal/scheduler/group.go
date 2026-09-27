package scheduler

import (
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// abortUnfinished moves every non-terminal member of the group to
// CANCELED with the given reason, emitting events. It returns the IDs of
// members that were RUNNING so the caller can interrupt their processes.
// It runs inside the caller's transaction, so the failure decision and
// the sibling aborts commit together (never "half a group").
func abortUnfinished(tx store.Tx, groupID, kind string, reason job.FailureReason, now time.Time) []string {
	var running []string
	for _, j := range tx.JobsOfGroup(groupID) {
		if j.Status.Terminal() {
			continue
		}
		from := j.Status
		j.Status = job.StatusCanceled
		j.Reason = reason
		j.UpdatedAt = now
		j.FinishedAt = now
		j.ExitCode = -1
		j.LeaseOwner = ""
		j.LeaseUntil = now
		tx.UpdateJob(j)
		tx.Emit(job.Event{
			Time: now, GroupID: groupID, JobID: j.ID,
			Kind: kind, From: from, To: job.StatusCanceled,
			Reason: reason, Attempt: j.Attempts,
		})
		if from == job.StatusRunning {
			running = append(running, j.ID)
		}
	}
	return running
}

// aggregateGroupStatus computes the terminal group status from terminal
// members under a fixed precedence rule: explicit cancellation of the
// group aside, any FAILED/TIMED_OUT member (or siblings aborted because of
// one) makes the group FAILED; pure success makes it SUCCEEDED; success
// mixed only with user cancellations makes it CANCELED.
func aggregateGroupStatus(g job.Group, jobs []job.Job) job.GroupStatus {
	if g.Status == job.GroupCanceled {
		return job.GroupCanceled
	}
	var sawFail, sawTimeout, sawAbort, sawUserCancel bool
	for _, j := range jobs {
		switch j.Status {
		case job.StatusFailed:
			sawFail = true
		case job.StatusTimedOut:
			sawTimeout = true
		case job.StatusCanceled:
			switch j.Reason {
			case job.ReasonGroupAborted:
				sawAbort = true
			case job.ReasonUserCanceled:
				sawUserCancel = true
			}
		}
	}
	if sawFail || sawTimeout || sawAbort {
		return job.GroupFailed
	}
	if sawUserCancel {
		return job.GroupCanceled
	}
	return job.GroupSucceeded
}

// finalizeGroupIfDone advances the group once all members are terminal.
// Safe to call on every member transition: it no-ops while any member is
// still running or while the group is already terminal.
func (s *Scheduler) finalizeGroupIfDone(tx store.Tx, groupID string, now time.Time) {
	g, ok := tx.Group(groupID)
	if !ok || isGroupTerminal(g.Status) {
		return
	}
	jobs := tx.JobsOfGroup(groupID)
	for _, j := range jobs {
		if !j.Status.Terminal() {
			return
		}
	}
	from := g.Status
	g.Status = aggregateGroupStatus(g, jobs)
	g.UpdatedAt = now
	tx.UpdateGroup(g)
	tx.Emit(job.Event{
		Time: now, GroupID: groupID, Kind: "GROUP_FINALIZED",
		Detail: string(from) + "->" + string(g.Status),
	})
}
