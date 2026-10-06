// Package scheduler wires submission, barrier admission, fail-fast reaction,
// workers, lease recovery and group finalization into one local engine.
//
// The scheduler itself is deliberately thin: every decision is persisted by
// the store as a state transition + event, while the scheduler only (1) polls
// pending groups for barrier release, (2) reaps expired leases to simulate
// crash recovery and (3) periodically flushes heartbeat-only state. Workers
// (internal/executor) do all task execution against the same store, so the
// engine can be restarted without losing or duplicating work.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/executor"
	"github.com/xingjianluo/job-array-scheduler-go/internal/policy"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
	"github.com/xingjianluo/job-array-scheduler-go/internal/task"
)

// Scheduler is the local job-array engine.
type Scheduler struct {
	cfg domain.Config
	st  *store.FileStore
	reg *task.Registry

	workers []*executor.Executor

	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once
	logger   func(format string, args ...any)
}

// New constructs a scheduler over an opened store.
func New(cfg domain.Config, st *store.FileStore, reg *task.Registry) *Scheduler {
	return &Scheduler{
		cfg:    cfg,
		st:     st,
		reg:    reg,
		stopCh: make(chan struct{}),
		logger: func(string, ...any) {},
	}
}

// SetLogger installs a printf-style trace logger.
func (s *Scheduler) SetLogger(f func(format string, args ...any)) { s.logger = f }

// Start launches admission/recovery/flush loops and all workers.
func (s *Scheduler) Start() {
	for i := 0; i < s.cfg.NumWorkers; i++ {
		id := fmt.Sprintf("worker-%d", i+1)
		w := executor.New(s.cfg, s.st, s.reg, id)
		w.SetLogger(s.logger)
		s.workers = append(s.workers, w)
		w.Start()
	}
	s.wg.Add(3)
	go s.admissionLoop()
	go s.recoveryLoop()
	go s.flushLoop()
}

func (s *Scheduler) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *Scheduler) tick(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.stopCh:
		return false
	}
}

// admissionLoop releases groups whose barrier is satisfied. Immediate groups
// are typically admitted inside AddJobs already; this loop covers batches
// whose members are submitted in separate AddJobs calls.
func (s *Scheduler) admissionLoop() {
	defer s.wg.Done()
	for {
		if !s.tick(s.cfg.PollInterval) {
			return
		}
		g, admitted, err := s.st.TryAdmit()
		if err != nil {
			s.logger("scheduler: admit error: %v", err)
			continue
		}
		if admitted {
			s.logger("scheduler: group %s admitted (%s, members=%d)",
				g.ID, g.Spec.Admission, len(g.JobIDs))
		}
	}
}

// recoveryLoop reaps leases whose heartbeats stopped (worker goroutine/process
// crash). The lease TTL is deliberately much larger than the heartbeat
// interval, so a healthy worker is never mistaken for a dead one.
func (s *Scheduler) recoveryLoop() {
	defer s.wg.Done()
	interval := s.cfg.LeaseTimeout / 3
	if interval < s.cfg.PollInterval {
		interval = s.cfg.PollInterval
	}
	for {
		if !s.tick(interval) {
			return
		}
		ids, err := s.st.RecoverExpiredLeases(time.Now())
		if err != nil {
			s.logger("scheduler: recovery error: %v", err)
			continue
		}
		for _, id := range ids {
			s.logger("scheduler: recovered expired lease of %s; requeued", id)
		}
	}
}

// flushLoop periodically persists heartbeat-only updates into the snapshot.
func (s *Scheduler) flushLoop() {
	defer s.wg.Done()
	interval := s.cfg.HeartbeatInterval * 5
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	for {
		if !s.tick(interval) {
			return
		}
		if err := s.st.Flush(); err != nil {
			s.logger("scheduler: flush error: %v", err)
		}
	}
}

// Shutdown stops claiming new work, lets loops exit and waits. Workers finish
// their current attempt first (call with a generous ctx if tasks are long).
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })
	done := make(chan struct{})
	go func() {
		for _, w := range s.workers {
			w.Stop()
		}
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return s.st.Flush()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SubmitGroup creates a group and submits its member jobs in one batch. The
// batch is atomic at the API boundary: either every member is accepted or an
// error is returned and the caller gets no half-registered group. Dependency
// declarations are validated BEFORE anything is created, so an unknown
// reference or a cycle rejects the submission with no group and no member
// left behind at all.
func (s *Scheduler) SubmitGroup(spec domain.GroupSpec, jobs []domain.JobSpec) (*domain.Group, []*domain.Job, error) {
	if err := validateSpec(spec, jobs); err != nil {
		return nil, nil, err
	}
	g, err := s.st.CreateGroup(spec)
	if err != nil {
		return nil, nil, err
	}
	created, err := s.st.AddJobs(g.ID, jobs)
	if err != nil {
		// Roll the unusable header into a terminal rejected state so nothing
		// remains PENDING after a failed batch.
		_ = s.st.RejectGroup(g.ID, domain.ReasonGroupRejected,
			"batch failed: "+err.Error())
		return nil, nil, err
	}
	return g, created, nil
}

// validateSpec catches submissions whose barrier could never be satisfied,
// malformed dependency declarations and other invalid input up front, with
// an explicit reject result rather than an eternally pending group.
func validateSpec(spec domain.GroupSpec, jobs []domain.JobSpec) error {
	n := len(jobs)
	if n == 0 {
		return fmt.Errorf("%w: empty job batch", domain.ErrConflict)
	}
	switch spec.Admission {
	case domain.AdmissionImmediate, domain.AdmissionAll, domain.AdmissionQuorum:
	default:
		return fmt.Errorf("%w: unknown admission %q", domain.ErrConflict, spec.Admission)
	}
	if spec.Admission == domain.AdmissionAll || spec.Admission == domain.AdmissionQuorum {
		if spec.ExpectedMembers <= 0 {
			return fmt.Errorf("%w: %s admission requires ExpectedMembers>0",
				domain.ErrConflict, spec.Admission)
		}
		if n > spec.ExpectedMembers {
			return fmt.Errorf("%w: batch %d exceeds ExpectedMembers %d",
				domain.ErrConflict, n, spec.ExpectedMembers)
		}
		if spec.Admission == domain.AdmissionAll && n < spec.ExpectedMembers {
			// A single-call SubmitGroup with "all" MUST carry the complete
			// set, otherwise the barrier could never open. Extra members can
			// still be added later via AddJobs (e.g. partial submitters).
			return fmt.Errorf("%w: all-admission batch has %d/%d members",
				domain.ErrConflict, n, spec.ExpectedMembers)
		}
	}
	switch spec.FailurePolicy {
	case "", domain.FailureContinue, domain.FailureFailFast:
	default:
		return fmt.Errorf("%w: unknown failure policy %q", domain.ErrConflict, spec.FailurePolicy)
	}
	if spec.MaxParallel < 0 {
		return fmt.Errorf("%w: MaxParallel must be >= 0", domain.ErrConflict)
	}
	// Dependency rules: unknown member, duplicate name, self-dependency or
	// a cycle all reject the whole batch before anything is persisted.
	if err := policy.ValidateDeps(nil, jobs); err != nil {
		return err
	}
	return nil
}

// CancelJob cancels a single job.
func (s *Scheduler) CancelJob(jobID string) error {
	return s.st.CancelJob(jobID, domain.ReasonUserCanceled)
}

// CancelGroup cancels every non-terminal member of a group.
func (s *Scheduler) CancelGroup(groupID string) error {
	return s.st.CancelGroup(groupID, domain.ReasonUserCanceled)
}

// WaitGroup blocks until the group reaches a terminal state.
func (s *Scheduler) WaitGroup(ctx context.Context, groupID string) (*domain.Group, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	events := s.st.Watch(ctx2)
	t := time.NewTicker(s.cfg.PollInterval * 2)
	defer t.Stop()
	for {
		g, err := s.st.GetGroup(groupID)
		if err != nil {
			return nil, err
		}
		if g.State == domain.GroupSucceeded || g.State == domain.GroupFailed || g.State == domain.GroupCanceled {
			return g, nil
		}
		select {
		case <-events:
		case <-t.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// GetGroup / GetJob are read-through accessors.
func (s *Scheduler) GetGroup(id string) (*domain.Group, error) { return s.st.GetGroup(id) }
func (s *Scheduler) GetJob(id string) (*domain.Job, error)     { return s.st.GetJob(id) }
