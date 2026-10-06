package store

import (
	"fmt"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/policy"
)

// ClaimNext atomically leases one claimable job to workerID.
//
// Claimable = group RUNNING, job READY, backoff elapsed. The method enforces
// the global parallelism cap and the single-writer invariant: claiming is one
// mutex-protected compare-and-set (READY+elapsed -> RUNNING with a fresh,
// globally monotonic fencing token), so concurrent workers can never receive
// the same job, and every later completion/heartbeat is checked against the
// token it was leased with — stale or duplicate completions are rejected with
// ErrConflict instead of double-applying.
func (s *FileStore) ClaimNext(workerID string, now time.Time) (*domain.Job, *domain.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cap := s.cfg.MaxParallel
	if cap <= 0 {
		cap = s.cfg.NumWorkers
	}
	if cap > 0 && s.runningCountLocked() >= cap {
		return nil, nil, nil
	}

	for _, id := range s.snap.JobOrder {
		j := s.snap.Jobs[id]
		if j == nil || j.State != domain.StateReady {
			continue
		}
		g := s.snap.Groups[j.GroupID]
		if g == nil || g.State != domain.GroupRunning {
			continue
		}
		if !j.NextAttemptAt.IsZero() && now.Before(j.NextAttemptAt) {
			continue
		}
		// Per-group concurrency cap: a saturated group is skipped over (not
		// blocking the scan), so its members can never starve other groups,
		// and the effective limit is min(global cap, group cap).
		if gcap := g.Spec.MaxParallel; gcap > 0 && s.runningInGroupLocked(g.ID) >= gcap {
			continue
		}
		// Lease it.
		token := s.snap.NextToken
		s.snap.NextToken++
		attemptNo := j.CurrentAttempt()
		a := &domain.Attempt{
			Attempt:       attemptNo,
			WorkerID:      workerID,
			Token:         token,
			StartedAt:     now,
			LastHeartbeat: now,
		}
		j.Attempts = append(j.Attempts, a)
		j.State = domain.StateRunning
		j.CancelRequested = false
		j.CancelReason = ""
		detail := fmt.Sprintf("lease granted (running=%d/%d, attempt=%d/%d)",
			s.runningCountLocked(), cap, attemptNo, j.Retry.MaxAttempts)
		if gcap := g.Spec.MaxParallel; gcap > 0 {
			detail += fmt.Sprintf(" group_running=%d/%d", s.runningInGroupLocked(g.ID), gcap)
		}
		s.emitLocked(domain.Event{
			Type:     domain.EvJobClaimed,
			GroupID:  j.GroupID,
			JobID:    j.ID,
			WorkerID: workerID,
			Attempt:  attemptNo,
			Token:    token,
			Detail:   detail,
		})
		return cloneJob(j), cloneAttempt(a), nil
	}
	return nil, nil, nil
}

func cloneAttempt(a *domain.Attempt) *domain.Attempt {
	if a == nil {
		return nil
	}
	c := *a
	return &c
}

func (s *FileStore) runningCountLocked() int {
	n := 0
	for _, j := range s.snap.Jobs {
		if j.State == domain.StateRunning {
			n++
		}
	}
	return n
}

// runningInGroupLocked counts RUNNING members of one group (per-group cap).
func (s *FileStore) runningInGroupLocked(groupID string) int {
	g := s.snap.Groups[groupID]
	if g == nil {
		return 0
	}
	n := 0
	for _, id := range g.JobIDs {
		if j := s.snap.Jobs[id]; j != nil && j.State == domain.StateRunning {
			n++
		}
	}
	return n
}

// Heartbeat renews a lease. It refuses stale workers (wrong token) so a
// zombie worker whose job was already recovered and re-leased cannot extend
// someone else's lease. Heartbeats are audit-logged but don't rewrite the
// snapshot; Scheduler.Flush persists them periodically.
func (s *FileStore) Heartbeat(jobID, workerID string, token int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.snap.Jobs[jobID]
	if !ok {
		return domain.ErrNotFound
	}
	a := j.InFlightAttempt()
	if a == nil || a.Token != token || a.WorkerID != workerID {
		return fmt.Errorf("%w: lease stale for %s token=%d", domain.ErrConflict, jobID, token)
	}
	a.LastHeartbeat = nowTime()
	s.emitNoSnapLocked(domain.Event{
		Type:     domain.EvJobHeartbeat,
		GroupID:  j.GroupID,
		JobID:    jobID,
		WorkerID: workerID,
		Token:    token,
	})
	return nil
}

// CancelReason returns the pending cancellation reason for a leased job.
func (s *FileStore) CancelReason(jobID, workerID string, token int64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.snap.Jobs[jobID]
	if !ok {
		return "", false
	}
	a := j.InFlightAttempt()
	if a == nil || a.Token != token || a.WorkerID != workerID {
		return "", false
	}
	return j.CancelReason, j.CancelRequested
}

// CancelRequested is the cooperative cancel check polled by the worker. It
// returns false for a stale lease, after which a well-behaved worker stops
// touching the job entirely (fencing).
func (s *FileStore) CancelRequested(jobID, workerID string, token int64) bool {
	_, ok := s.CancelReason(jobID, workerID, token)
	return ok
}

// FinishAttempt applies the outcome of a leased attempt. The token check is
// the idempotency boundary:
//
//   - a second FinishAttempt for the same lease (retry on the wire, confused
//     worker) finds the attempt already closed -> ErrConflict, no second
//     state transition;
//   - a worker reporting after its lease expired and the job was re-leased
//     fails the token comparison -> ErrConflict, its result is discarded.
func (s *FileStore) FinishAttempt(jobID, workerID string, token int64, out Outcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.snap.Jobs[jobID]
	if !ok {
		return domain.ErrNotFound
	}
	a := j.InFlightAttempt()
	if a == nil || a.Token != token || a.WorkerID != workerID {
		return fmt.Errorf("%w: finish rejected for %s token=%d (stale lease)",
			domain.ErrConflict, jobID, token)
	}

	now := nowTime()
	a.EndedAt = now
	a.State = out.State
	a.Reason = out.Reason
	a.Message = out.Message
	a.Output = out.Output
	out.WorkerID = workerID

	switch out.State {
	case domain.StateSucceeded:
		j.State = domain.StateSucceeded
		j.Reason = ""
		j.Message = ""
		j.Output = out.Output
		j.FinishedAt = now
		s.emitLocked(domain.Event{
			Type: domain.EvJobSucceeded, GroupID: j.GroupID, JobID: j.ID,
			WorkerID: workerID, Attempt: a.Attempt, Token: token,
			Detail: fmt.Sprintf("output=%q", out.Output),
		})

	case domain.StateFailed:
		// Ordinary task error: retryable up to MaxAttempts.
		if a.Attempt < j.Retry.MaxAttempts {
			delay := j.Retry.Delay(a.Attempt)
			j.State = domain.StateReady
			j.NextAttemptAt = now.Add(delay)
			j.CancelRequested = false
			j.CancelReason = ""
			s.emitLocked(domain.Event{
				Type: domain.EvJobFailed, GroupID: j.GroupID, JobID: j.ID,
				WorkerID: workerID, Attempt: a.Attempt, Token: token,
				Detail: fmt.Sprintf("reason=%s: %s — will retry (attempt %d/%d, wait %s)",
					out.Reason, out.Message, a.Attempt+1, j.Retry.MaxAttempts, delay),
			})
			s.emitLocked(domain.Event{
				Type: domain.EvJobRetryWait, GroupID: j.GroupID, JobID: j.ID,
				Attempt: a.Attempt + 1,
				Detail:  fmt.Sprintf("backoff=%s until attempt %d", delay, a.Attempt+1),
			})
		} else {
			s.failJobLocked(j, now, out,
				fmt.Sprintf("attempts exhausted (%d/%d): %s",
					a.Attempt, j.Retry.MaxAttempts, out.Message))
		}

	case domain.StateTimedOut:
		// Timeout is terminal by design: a runaway task is not retried, so
		// resource cleanup and the final classification are unambiguous.
		s.timeoutJobLocked(j, now, out)

	case domain.StateCanceled:
		j.State = domain.StateCanceled
		j.Reason = out.Reason
		j.Message = out.Message
		j.FinishedAt = now
		j.CancelRequested = false
		j.CancelReason = ""
		s.emitLocked(domain.Event{
			Type: domain.EvJobCanceled, GroupID: j.GroupID, JobID: j.ID,
			WorkerID: workerID, Attempt: a.Attempt, Token: token,
			Detail: fmt.Sprintf("reason=%s: %s", out.Reason, out.Message),
		})

	default:
		return fmt.Errorf("%w: unknown outcome state %s", domain.ErrConflict, out.State)
	}

	s.afterMemberSettledLocked(j)
	return nil
}

// failJobLocked commits a permanently failed job after retry exhaustion.
func (s *FileStore) failJobLocked(j *domain.Job, now time.Time, out Outcome, msg string) {
	j.State = domain.StateFailed
	j.Reason = out.Reason
	j.Message = msg
	j.FinishedAt = now
	j.CancelRequested = false
	j.CancelReason = ""
	s.emitLocked(domain.Event{
		Type: domain.EvJobFailed, GroupID: j.GroupID, JobID: j.ID,
		WorkerID: out.WorkerID,
		Detail:   fmt.Sprintf("reason=%s: %s", out.Reason, msg),
	})
}

func (s *FileStore) timeoutJobLocked(j *domain.Job, now time.Time, out Outcome) {
	j.State = domain.StateTimedOut
	if out.Reason == "" {
		out.Reason = domain.ReasonTimeout
	}
	j.Reason = domain.ReasonTimeout
	j.Message = out.Message
	j.FinishedAt = now
	j.CancelRequested = false
	j.CancelReason = ""
	s.emitLocked(domain.Event{
		Type: domain.EvJobTimedOut, GroupID: j.GroupID, JobID: j.ID,
		Detail: fmt.Sprintf("reason=%s: %s", domain.ReasonTimeout, out.Message),
	})
}

// afterMemberSettledLocked handles group-wide consequences of a member
// reaching (or leaving) a settled state: fail-fast propagation and group
// finalization. Caller holds mu.
func (s *FileStore) afterMemberSettledLocked(j *domain.Job) {
	g := s.snap.Groups[j.GroupID]
	if g == nil {
		return
	}
	// Fail-fast: a member that terminally fails or times out causes every
	// other member that can still stop to stop. The triggering member keeps
	// its own failure reason; peers are CANCELED+group_failfast, so the two
	// categories remain separately countable.
	if g.State == domain.GroupRunning && g.Spec.FailurePolicy == domain.FailureFailFast &&
		(j.State == domain.StateFailed || j.State == domain.StateTimedOut) {
		s.emitLocked(domain.Event{
			Type: domain.EvJobCancelReq, GroupID: g.ID, JobID: j.ID,
			Detail: fmt.Sprintf("fail-fast trigger: member %s is %s (%s)", j.ID, j.State, j.Reason),
		})
		for _, pid := range g.JobIDs {
			if pid == j.ID {
				continue
			}
			peer := s.snap.Jobs[pid]
			if peer == nil || peer.State.Terminal() {
				continue
			}
			if peer.State == domain.StateRunning {
				_ = s.requestCancelRunningLocked(peer, domain.ReasonGroupFailFast,
					"peer failure under fail-fast policy")
			} else {
				s.cancelJobLocked(peer, domain.ReasonGroupFailFast,
					"peer failed under fail-fast policy", nowTime(), true)
			}
		}
	}
	s.finalizeGroupIfDoneLocked(g.ID)
}

// propagateDepsLocked reconciles every BLOCKED member of the group with the
// current state of its dependencies, iterating to a fixpoint so cascades
// (a skip unblocking further skips) settle within this one locked section:
//
//   - all dependencies SUCCEEDED          -> READY (claimable);
//   - any dependency at a non-success     -> SKIPPED + dependency_failed
//     terminal state (FAILED / TIMED_OUT /   (terminal; the member never
//     CANCELED / SKIPPED)                    runs and is classified apart
//     from real failures, timeouts and cancels).
//
// Deterministic: members are scanned in submission (JobIDs) order and the
// loop only stops when a full pass changes nothing, so the same member
// states always produce the same propagation result.
func (s *FileStore) propagateDepsLocked(g *domain.Group) {
	for changed := true; changed; {
		changed = false
		for _, id := range g.JobIDs {
			j := s.snap.Jobs[id]
			if j == nil || j.State != domain.StateBlocked {
				continue
			}
			allOK := true
			var badDep *domain.Job
			for _, depID := range j.DependsOn {
				d := s.snap.Jobs[depID]
				if d == nil || d.State == domain.StateSucceeded {
					continue
				}
				allOK = false
				if d.State.Terminal() {
					badDep = d
				}
				break
			}
			switch {
			case badDep != nil:
				now := nowTime()
				j.State = domain.StateSkipped
				j.Reason = domain.ReasonDependencyFailed
				j.Message = fmt.Sprintf("dependency %s (%q) reached %s",
					badDep.ID, badDep.Name, badDep.State)
				j.FinishedAt = now
				s.emitLocked(domain.Event{
					Type:    domain.EvJobSkipped,
					GroupID: g.ID,
					JobID:   j.ID,
					Detail: fmt.Sprintf("reason=%s: %s",
						domain.ReasonDependencyFailed, j.Message),
				})
				changed = true
			case allOK:
				j.State = domain.StateReady
				j.ReadyAt = nowTime()
				s.emitLocked(domain.Event{
					Type:    domain.EvJobReady,
					GroupID: g.ID,
					JobID:   j.ID,
					Detail:  "all dependencies succeeded",
				})
				changed = true
			}
		}
	}
}

// finalizeGroupIfDoneLocked closes the group when all members are terminal.
// Success/failure/cancel classification follows policy.GroupTerminal, giving
// a single deterministic group result independent of completion ordering.
func (s *FileStore) finalizeGroupIfDoneLocked(groupID string) {
	g := s.snap.Groups[groupID]
	if g == nil || g.State != domain.GroupRunning {
		return
	}
	// Settle dependency-driven transitions first: a member that just became
	// terminal may unblock (READY) or cascade-skip (SKIPPED) its dependents,
	// and only the fixpoint of that propagation decides whether the group is
	// really done.
	s.propagateDepsLocked(g)
	jobs := make([]*domain.Job, 0, len(g.JobIDs))
	for _, id := range g.JobIDs {
		j := s.snap.Jobs[id]
		if j == nil || !j.State.Terminal() {
			return
		}
		jobs = append(jobs, j)
	}
	final := policy.GroupTerminal(jobs)
	now := nowTime()
	g.State = final
	g.FinishedAt = now
	evType := domain.EvGroupSucceeded
	switch final {
	case domain.GroupFailed:
		evType = domain.EvGroupFailed
	case domain.GroupCanceled:
		evType = domain.EvGroupCanceled
	}
	counts := map[domain.JobState]int{}
	for _, j := range jobs {
		counts[j.State]++
	}
	s.emitLocked(domain.Event{
		Type:    evType,
		GroupID: groupID,
		Detail: fmt.Sprintf("group settled %s (success=%d failed=%d timeout=%d canceled=%d skipped=%d)",
			final, counts[domain.StateSucceeded], counts[domain.StateFailed],
			counts[domain.StateTimedOut], counts[domain.StateCanceled],
			counts[domain.StateSkipped]),
	})
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

// RecoverExpiredLeases finds RUNNING attempts whose latest heartbeat is older
// than the configured lease TTL and requeues them: the attempt is closed as
// lease_expired and the job returns to READY (subject to the same retry
// budget). The fencing token already issued is never reused, so when the dead
// worker eventually reports back it is fenced off, and the task's idempotency
// ledger prevents double execution of the replayed attempt.
func (s *FileStore) RecoverExpiredLeases(now time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ttl := s.cfg.LeaseTimeout
	var recovered []string
	for _, j := range s.snap.Jobs {
		if j.State != domain.StateRunning {
			continue
		}
		a := j.InFlightAttempt()
		if a == nil {
			continue
		}
		if now.Sub(a.LastHeartbeat) <= ttl {
			continue
		}
		// Honor a cancel request that landed before the crash: the worker is
		// dead, so settle as CANCELED instead of re-running.
		if j.CancelRequested {
			a.EndedAt = now
			a.State = domain.StateCanceled
			a.Reason = domain.ReasonGroupFailFast
			a.Message = "worker died after cancel requested"
			j.State = domain.StateCanceled
			j.Reason = domain.ReasonGroupFailFast
			j.Message = a.Message
			j.FinishedAt = now
			j.CancelRequested = false
			j.CancelReason = ""
			s.emitLocked(domain.Event{
				Type: domain.EvJobCanceled, GroupID: j.GroupID, JobID: j.ID,
				WorkerID: a.WorkerID, Token: a.Token,
				Detail: "lease expired while cancel pending; settled CANCELED",
			})
			s.afterMemberSettledLocked(j)
			recovered = append(recovered, j.ID)
			continue
		}
		a.EndedAt = now
		a.State = domain.StateFailed
		a.Reason = "lease_expired"
		a.Message = fmt.Sprintf("worker %s heartbeat timed out after %s", a.WorkerID, ttl)
		s.emitLocked(domain.Event{
			Type: domain.EvJobLeaseExpired, GroupID: j.GroupID, JobID: j.ID,
			WorkerID: a.WorkerID, Token: a.Token,
			Detail: a.Message,
		})
		if a.Attempt < j.Retry.MaxAttempts {
			j.State = domain.StateReady
			j.NextAttemptAt = now
			s.emitLocked(domain.Event{
				Type: domain.EvJobRetryWait, GroupID: j.GroupID, JobID: j.ID,
				Attempt: a.Attempt + 1, Detail: "requeue after lease expiry",
			})
		} else {
			s.failJobLocked(j, now,
				Outcome{State: domain.StateFailed, Reason: "lease_expired", Message: a.Message},
				fmt.Sprintf("attempts exhausted after lease expiry (%d/%d)", a.Attempt, j.Retry.MaxAttempts))
			s.afterMemberSettledLocked(j)
		}
		recovered = append(recovered, j.ID)
	}
	if len(recovered) > 0 {
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	}
	return recovered, nil
}
