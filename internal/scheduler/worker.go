package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/executor"
	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// claimResult is what one claim transaction returns.
type claimResult struct {
	job   job.Job
	owner string
	ok    bool // false when nothing is currently claimable
}

// claimNext performs one atomic claim: find the next READY job of an OPEN
// group (FIFO by timestamp-prefixed group/job ID) and flip it to RUNNING
// under a fresh lease, all in one store transaction. Two concurrent
// workers therefore can never claim the same job, and a duplicate claim
// simply finds the job non-READY.
func (s *Scheduler) claimNext(ctx context.Context) (claimResult, error) {
	now := s.now()
	owner := s.newOwner()
	res := claimResult{}
	err := s.st.Mutate(ctx, func(tx store.Tx) error {
		var chosen string
	groupLoop:
		for _, gid := range orderedGroups(tx) {
			g, _ := tx.Group(gid)
			if !g.Open || isGroupTerminal(g.Status) {
				continue
			}
			for _, j := range tx.JobsOfGroup(gid) {
				if j.Status == job.StatusReady {
					chosen = j.ID
					break groupLoop
				}
			}
		}
		if chosen == "" {
			return nil
		}
		j, _ := tx.Job(chosen)
		from := j.Status
		j.Status = job.StatusRunning
		j.Attempts = 1
		j.LeaseOwner = owner
		j.LeaseUntil = now.Add(s.cfg.LeaseTTL)
		j.UpdatedAt = now
		if j.StartedAt.IsZero() {
			j.StartedAt = now
		}
		tx.UpdateJob(j)
		tx.Emit(job.Event{
			Time: now, GroupID: j.GroupID, JobID: j.ID, Kind: "CLAIMED",
			From: from, To: job.StatusRunning, Attempt: 1, Owner: owner,
		})
		res.job = j
		res.owner = owner
		res.ok = true
		return nil
	})
	return res, err
}

// runOne drives an already claimed job to a terminal state (or until the
// lease is lost / the scheduler shuts down). The caller owns and releases
// one capacity slot for the whole call; the job stays RUNNING across
// retry backoffs while heartbeats keep the lease alive. Attempts are
// incremented durably before each process start, so the retry budget
// survives restarts (a restarted worker re-fetches it from the requeued
// job).
func (s *Scheduler) runOne(ctx context.Context, claimed job.Job, owner string) {
	// User cancel (CancelJob/CancelGroup) interrupts attempt and backoff.
	userCtx, userCancel := context.WithCancel(ctx)
	defer userCancel()
	s.registerCancel(claimed.ID, userCancel)
	defer s.unregisterCancel(claimed.ID)

	// Heartbeat keeps the lease alive across attempts and backoffs. The
	// worker owns the job until it commits a terminal state, loses the
	// lease, or the scheduler shuts down (in which case the lease simply
	// expires and recovery requeues the job).
	hbCtx, hbStop := context.WithCancel(ctx)
	hbErr := make(chan error, 1)
	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		s.heartbeat(hbCtx, claimed.ID, owner, hbErr)
	}()
	defer func() {
		hbStop()
		hbWg.Wait()
	}()

	j := claimed
	// attemptNo is this worker's monotonic attempt counter. It starts
	// from the durable value (1 on a fresh claim, possibly higher after a
	// crash-recovery requeue) and advances locally, so refreshing j from
	// the store each loop can never reset the retry budget.
	attemptNo := j.Attempts
	if attemptNo < 1 {
		attemptNo = 1
	}
	for {
		if ctx.Err() != nil {
			return // scheduler shutdown: lease will expire -> requeue
		}
		select {
		case lost := <-hbErr:
			if lost != nil {
				return // lease lost: another owner handles the job now
			}
		default:
		}
		cur, ok := s.getJob(j.ID)
		if !ok || cur.Status.Terminal() {
			return // canceled while in backoff (or removed)
		}
		if cur.Status != job.StatusRunning || cur.LeaseOwner != owner {
			return
		}
		j = cur

		// Persist the attempt counter when it advances (every retry and
		// after recovery), before the process starts.
		if attemptNo != j.Attempts {
			if err := s.beginAttempt(ctx, &j, owner, attemptNo); err != nil {
				return // terminal (j updated) or lease lost
			}
		}

		out := s.runAttempt(ctx, userCtx, &j)
		if ctx.Err() != nil {
			return // shutdown while running: do not commit; lease expires
		}

		switch out.Kind {
		case executor.OutcomeSucceeded:
			s.commitTerminal(j.ID, owner, terminalDecision{
				status: job.StatusSucceeded, reason: job.ReasonNone, exit: 0,
				kind: "SUCCEEDED",
			}, out, nil)
			return
		case executor.OutcomeTimeout:
			done := s.commitTerminal(j.ID, owner, terminalDecision{
				status: job.StatusTimedOut, reason: job.ReasonTimeout, exit: -1,
				kind: "TIMED_OUT", abortSiblings: true,
			}, out, nil)
			if done.abortedSiblings {
				s.interruptGroup(done.groupID)
			}
			return
		case executor.OutcomeCanceled:
			done := s.commitTerminal(j.ID, owner, terminalDecision{
				status: job.StatusCanceled, reason: job.ReasonUserCanceled, exit: -1,
				kind: "CANCELED", abortSiblings: false,
			}, out, nil)
			if done.abortedSiblings {
				s.interruptGroup(done.groupID)
			}
			return
		default: // FAILED (non-zero exit) or SPAWN
			if attemptNo <= j.Spec.MaxRetries {
				if err := s.sleep(userCtx, j.Spec.RetryBackoff); err != nil {
					// Scheduler shutdown -> lease expires and requeues;
					// user cancel -> CANCELED is already committed.
					return
				}
				attemptNo++
				continue
			}
			lastReason := out.Reason
			done := s.commitTerminal(j.ID, owner, terminalDecision{
				status: job.StatusFailed, reason: job.ReasonRetriesExhausted,
				exit: out.ExitCode, kind: "FAILED", abortSiblings: true,
				detailExtra: fmt.Sprintf("last_error=%s exit=%d", lastReason, out.ExitCode),
			}, out, nil)
			if done.abortedSiblings {
				s.interruptGroup(done.groupID)
			}
			return
		}
	}
}

// runAttempt executes the process under a per-attempt timeout derived
// from the parent + user-cancel context.
func (s *Scheduler) runAttempt(parent, user context.Context, j *job.Job) executor.Outcome {
	attemptCtx, attemptCancel := context.WithCancel(user)
	defer attemptCancel()
	if j.Spec.Timeout > 0 {
		var tc context.CancelFunc
		attemptCtx, tc = context.WithTimeout(attemptCtx, j.Spec.Timeout)
		defer tc()
	}
	return s.runner.Run(attemptCtx, j.Spec)
}

// beginAttempt durably increments the attempt counter for a retry under
// the owner guard.
func (s *Scheduler) beginAttempt(ctx context.Context, j *job.Job, owner string, attemptNo int) error {
	now := s.now()
	return s.st.Mutate(ctx, func(tx store.Tx) error {
		cur, ok := tx.Job(j.ID)
		if !ok {
			return fmt.Errorf("job %s vanished", j.ID)
		}
		if cur.Status.Terminal() {
			*j = cur
			return nil
		}
		if cur.Status != job.StatusRunning || cur.LeaseOwner != owner {
			return errors.New("lease lost")
		}
		cur.Attempts = attemptNo
		cur.UpdatedAt = now
		tx.UpdateJob(cur)
		tx.Emit(job.Event{
			Time: now, GroupID: cur.GroupID, JobID: cur.ID, Kind: "ATTEMPT_START",
			From: job.StatusRunning, To: job.StatusRunning, Attempt: attemptNo, Owner: owner,
			Detail: fmt.Sprintf("retry after backoff"),
		})
		*j = cur
		return nil
	})
}

// heartbeat renews the lease until ctx is canceled. It reports at most one
// fatal error on errCh.
func (s *Scheduler) heartbeat(ctx context.Context, jobID, owner string, errCh chan<- error) {
	t := time.NewTicker(s.cfg.LeaseRenew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.renewLease(ctx, jobID, owner); err != nil {
				if ctx.Err() == nil {
					select {
					case errCh <- err:
					default:
					}
				}
				return
			}
		}
	}
}

// terminalDecision is the guarded outcome of one finished attempt.
type terminalDecision struct {
	status        job.Status
	reason        job.FailureReason
	exit          int
	kind          string
	abortSiblings bool
	detailExtra   string
}

// commitTerminal applies a guarded terminal transition: it only writes if
// the job is still RUNNING and still owned by this worker. Under
// FAIL_FAST a failure/timeout aborts all unfinished siblings in the very
// same transaction, and the group is finalized if that completes it.
func (s *Scheduler) commitTerminal(jobID, owner string, d terminalDecision, out executor.Outcome, _ func()) terminalResult {
	now := s.now()
	res := terminalResult{}
	_ = s.st.Mutate(context.Background(), func(tx store.Tx) error {
		j, ok := tx.Job(jobID)
		if !ok {
			return nil
		}
		res.groupID = j.GroupID
		if j.Status.Terminal() {
			return nil // cancel/timeout/another owner already decided
		}
		if j.Status != job.StatusRunning || j.LeaseOwner != owner {
			return nil // lost the lease: never overwrite the new owner
		}
		from := j.Status
		j.Status = d.status
		j.Reason = d.reason
		j.ExitCode = d.exit
		j.UpdatedAt = now
		j.FinishedAt = now
		j.LeaseOwner = ""
		j.LeaseUntil = now
		tx.UpdateJob(j)
		detail := fmt.Sprintf("attempts=%d dur=%s", j.Attempts, out.Duration)
		if d.detailExtra != "" {
			detail += " " + d.detailExtra
		}
		tx.Emit(job.Event{
			Time: now, GroupID: j.GroupID, JobID: j.ID, Kind: d.kind,
			From: from, To: d.status, Reason: d.reason, Attempt: j.Attempts, Owner: owner,
			Detail: detail,
		})
		g, _ := tx.Group(j.GroupID)
		if d.abortSiblings && g.Policy == job.PolicyFailFast {
			if running := abortUnfinished(tx, j.GroupID, "GROUP_ABORTED", job.ReasonGroupAborted, now); len(running) > 0 {
				res.abortedSiblings = true
			}
		}
		s.finalizeGroupIfDone(tx, j.GroupID, now)
		return nil
	})
	return res
}

// terminalResult carries side information from a terminal commit.
type terminalResult struct {
	groupID         string
	abortedSiblings bool
}

// renewLease extends LeaseUntil if (and only if) this worker still owns
// the RUNNING job.
func (s *Scheduler) renewLease(ctx context.Context, jobID, owner string) error {
	now := s.now()
	return s.st.Mutate(ctx, func(tx store.Tx) error {
		j, ok := tx.Job(jobID)
		if !ok {
			return errors.New("job gone")
		}
		if j.Status != job.StatusRunning || j.LeaseOwner != owner {
			return errors.New("lease lost")
		}
		j.LeaseUntil = now.Add(s.cfg.LeaseTTL)
		j.UpdatedAt = now
		tx.UpdateJob(j)
		return nil
	})
}

func (s *Scheduler) getJob(id string) (job.Job, bool) {
	var out job.Job
	found := false
	_ = s.st.Read(func(v store.View) error {
		out, found = v.Job(id)
		return nil
	})
	return out, found
}

func (s *Scheduler) interruptGroup(groupID string) {
	_ = s.st.Read(func(v store.View) error {
		for _, j := range v.JobsOfGroup(groupID) {
			if j.Status == job.StatusCanceled {
				s.interrupt(j.ID)
			}
		}
		return nil
	})
}

func (s *Scheduler) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func orderedGroups(v store.View) []string {
	groups := v.Groups()
	ids := make([]string, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, g.ID)
	}
	// IDs are timestamp-prefixed, so lexicographic order is FIFO submit.
	return ids
}
