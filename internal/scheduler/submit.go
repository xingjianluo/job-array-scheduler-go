package scheduler

import (
	"context"
	"fmt"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// SubmitGroup persists a batch as one group and, when startNow is true,
// atomically opens the barrier so all members become READY together.
// With startNow false the group waits for an explicit OpenGroup call.
//
// The whole submission is one store transaction: either every member and
// the group record exist afterward, or none of them do. A batch can never
// be half-submitted.
func (s *Scheduler) SubmitGroup(ctx context.Context, policy job.GroupPolicy, items []SubmitItem, startNow bool) (SubmitResult, error) {
	if policy != job.PolicyFailFast && policy != job.PolicyContinue {
		return SubmitResult{}, fmt.Errorf("scheduler: unknown group policy %q", policy)
	}
	if len(items) == 0 {
		return SubmitResult{}, fmt.Errorf("scheduler: empty submission")
	}
	groupID := job.NewID("grp")
	now := s.now()
	g := job.Group{
		ID:        groupID,
		Policy:    policy,
		Status:    job.GroupPending,
		Size:      len(items),
		Open:      startNow,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if startNow {
		g.Status = job.GroupRunning
	}
	res := SubmitResult{GroupID: groupID, JobIDs: make([]string, 0, len(items))}

	err := s.st.Mutate(ctx, func(tx store.Tx) error {
		tx.InsertGroup(g)
		for _, it := range items {
			jid := job.NewID("job")
			j := job.Job{
				ID:        jid,
				GroupID:   groupID,
				Name:      it.Name,
				Spec:      it.Spec,
				Status:    job.StatusPending,
				ExitCode:  -1,
				CreatedAt: now,
				UpdatedAt: now,
			}
			if startNow {
				j.Status = job.StatusReady
			}
			tx.InsertJob(j)
			tx.Emit(job.Event{
				Time:    now,
				GroupID: groupID,
				JobID:   jid,
				Kind:    "SUBMITTED",
				To:      j.Status,
				Detail:  it.Name,
			})
			res.JobIDs = append(res.JobIDs, jid)
		}
		if startNow {
			tx.Emit(job.Event{
				Time:    now,
				GroupID: groupID,
				Kind:    "BARRIER_OPENED",
				Detail:  string(policy),
			})
		}
		return nil
	})
	if err != nil {
		return SubmitResult{}, err
	}
	return res, nil
}

// OpenGroup atomically opens a held group's barrier: PENDING members move
// to READY in the same transaction that flips Group.Open. Members already
// terminal (e.g. canceled while held) are skipped.
func (s *Scheduler) OpenGroup(ctx context.Context, groupID string) error {
	now := s.now()
	return s.st.Mutate(ctx, func(tx store.Tx) error {
		g, ok := tx.Group(groupID)
		if !ok {
			return fmt.Errorf("%w: group %s", store.ErrNotFound, groupID)
		}
		if g.Open {
			return nil // idempotent
		}
		g.Open = true
		if g.Status == job.GroupPending {
			g.Status = job.GroupRunning
		}
		g.UpdatedAt = now
		tx.UpdateGroup(g)
		for _, j := range tx.JobsOfGroup(groupID) {
			if j.Status == job.StatusPending {
				from := j.Status
				j.Status = job.StatusReady
				j.UpdatedAt = now
				tx.UpdateJob(j)
				tx.Emit(job.Event{Time: now, GroupID: groupID, JobID: j.ID, Kind: "READY", From: from, To: job.StatusReady})
			}
		}
		tx.Emit(job.Event{Time: now, GroupID: groupID, Kind: "BARRIER_OPENED", Detail: string(g.Policy)})
		return nil
	})
}

// CancelJob requests cancellation of a single job. READY/PENDING jobs
// become terminal CANCELED synchronously; a RUNNING job is interrupted
// through its attempt context. Terminal jobs are left untouched.
func (s *Scheduler) CancelJob(ctx context.Context, jobID string) error {
	now := s.now()
	var running bool
	err := s.st.Mutate(ctx, func(tx store.Tx) error {
		j, ok := tx.Job(jobID)
		if !ok {
			return fmt.Errorf("%w: job %s", store.ErrNotFound, jobID)
		}
		if j.Status.Terminal() {
			return nil
		}
		running = j.Status == job.StatusRunning
		from := j.Status
		// Terminal immediately in all cases. For a RUNNING job the live
		// attempt is interrupted after commit; its terminal write is then
		// rejected by the worker's terminal guard, so the cancel result
		// can never be overwritten by a late completion. Clearing the
		// lease in the same breath means the recoverer cannot requeue a
		// job that is already terminal.
		j.Status = job.StatusCanceled
		j.Reason = job.ReasonUserCanceled
		j.UpdatedAt = now
		j.FinishedAt = now
		j.ExitCode = -1
		j.LeaseOwner = ""
		j.LeaseUntil = now
		tx.UpdateJob(j)
		tx.Emit(job.Event{Time: now, GroupID: j.GroupID, JobID: j.ID, Kind: "CANCELED", From: from, To: job.StatusCanceled, Reason: job.ReasonUserCanceled, Attempt: j.Attempts})
		s.finalizeGroupIfDone(tx, j.GroupID, now)
		return nil
	})
	if err != nil {
		return err
	}
	if running {
		// Interrupt the live attempt's process.
		s.interrupt(jobID)
	}
	return nil
}

// CancelGroup cancels the group and all unfinished members atomically:
// there is no moment where the group is "canceled" while some members are
// still PENDING/READY.
func (s *Scheduler) CancelGroup(ctx context.Context, groupID string) error {
	now := s.now()
	var runningIDs []string
	err := s.st.Mutate(ctx, func(tx store.Tx) error {
		g, ok := tx.Group(groupID)
		if !ok {
			return fmt.Errorf("%w: group %s", store.ErrNotFound, groupID)
		}
		if isGroupTerminal(g.Status) {
			return nil
		}
		g.Status = job.GroupCanceled
		g.UpdatedAt = now
		tx.UpdateGroup(g)
		for _, j := range tx.JobsOfGroup(groupID) {
			if j.Status.Terminal() {
				continue
			}
			from := j.Status
			j.Status = job.StatusCanceled
			j.Reason = job.ReasonUserCanceled
			j.UpdatedAt = now
			j.FinishedAt = now
			j.ExitCode = -1
			j.LeaseOwner = ""
			j.LeaseUntil = now
			tx.UpdateJob(j)
			tx.Emit(job.Event{Time: now, GroupID: groupID, JobID: j.ID, Kind: "CANCELED", From: from, To: job.StatusCanceled, Reason: job.ReasonUserCanceled, Attempt: j.Attempts})
			if from == job.StatusRunning {
				runningIDs = append(runningIDs, j.ID)
			}
		}
		tx.Emit(job.Event{Time: now, GroupID: groupID, Kind: "GROUP_CANCELED"})
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range runningIDs {
		s.interrupt(id)
	}
	return nil
}

func isGroupTerminal(st job.GroupStatus) bool {
	return st == job.GroupSucceeded || st == job.GroupFailed || st == job.GroupCanceled
}

// interrupt signals a running attempt's context. Absence of a registered
// cancel func means the worker already finished; the terminal guard in
// the worker handles that race.
func (s *Scheduler) interrupt(jobID string) {
	s.reqMu.Lock()
	cf := s.cancelFuncs[jobID]
	s.reqMu.Unlock()
	if cf != nil {
		cf()
	}
}

func (s *Scheduler) registerCancel(jobID string, cf context.CancelFunc) {
	s.reqMu.Lock()
	s.cancelFuncs[jobID] = cf
	s.reqMu.Unlock()
}

func (s *Scheduler) unregisterCancel(jobID string) {
	s.reqMu.Lock()
	delete(s.cancelFuncs, jobID)
	s.reqMu.Unlock()
}
