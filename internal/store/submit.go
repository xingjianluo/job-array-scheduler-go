package store

import (
	"fmt"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/policy"
)

// CreateGroup registers a batch header. No member is runnable until the
// admission barrier is released. Spec validation lives in the scheduler; the
// store enforces only structural rules (positive expected size for barriers).
func (s *FileStore) CreateGroup(spec domain.GroupSpec) (*domain.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch spec.Admission {
	case domain.AdmissionAll, domain.AdmissionQuorum:
		if spec.ExpectedMembers <= 0 {
			return nil, fmt.Errorf("%w: %s admission needs ExpectedMembers>0",
				domain.ErrConflict, spec.Admission)
		}
	case domain.AdmissionImmediate:
		// expected members is meaningless for immediate admission
	default:
		return nil, fmt.Errorf("%w: unknown admission policy %q",
			domain.ErrConflict, spec.Admission)
	}
	if spec.FailurePolicy == "" {
		spec.FailurePolicy = domain.FailureContinue
	}
	if spec.MaxParallel < 0 {
		return nil, fmt.Errorf("%w: group MaxParallel must be >= 0",
			domain.ErrConflict)
	}

	id := fmt.Sprintf("grp-%04d", s.snap.NextGroupNum)
	s.snap.NextGroupNum++
	g := &domain.Group{
		ID:     id,
		Spec:   spec,
		State:  domain.GroupPending,
		JobIDs: []string{},
	}
	s.snap.Groups[id] = g
	s.emitLocked(domain.Event{
		Type:    domain.EvGroupSubmitted,
		GroupID: id,
		Detail:  fmt.Sprintf("name=%q admission=%s failure_policy=%s expected=%d retry=%+v", spec.Name, spec.Admission, spec.FailurePolicy, spec.ExpectedMembers, spec.Retry),
	})
	return cloneGroup(g), nil
}

// AddJobs appends member jobs to a group. Jobs are born PENDING: they are not
// claimable until the barrier opens. If the barrier is already open (e.g. a
// late member of a quorum group), the new members bypass it immediately and
// are admitted to READY, which is exactly why partial batches can never be
// stuck in an ambiguous "half-started" state — the barrier is the one gate.
func (s *FileStore) AddJobs(groupID string, specs []domain.JobSpec) ([]*domain.Job, error) {
	if len(specs) == 0 {
		return nil, fmt.Errorf("%w: no jobs submitted", domain.ErrConflict)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	g, ok := s.snap.Groups[groupID]
	if !ok {
		return nil, domain.ErrNotFound
	}
	if g.State == domain.GroupSucceeded || g.State == domain.GroupFailed || g.State == domain.GroupCanceled {
		return nil, fmt.Errorf("%w: group %s is %s", domain.ErrGroupClosed, groupID, g.State)
	}
	if g.Spec.Admission != domain.AdmissionImmediate &&
		len(g.JobIDs)+len(specs) > g.Spec.ExpectedMembers {
		return nil, fmt.Errorf("%w: group %s capacity %d exceeded (+%d)",
			domain.ErrGroupClosed, groupID, g.Spec.ExpectedMembers, len(specs))
	}

	retry := g.Spec.Retry
	if retry.MaxAttempts <= 0 {
		retry = s.defaultRetryLocked()
	}
	timeout := s.defaultJobTimeoutLocked()

	// Dependency validation happens BEFORE any job is created, so an
	// invalid batch (unknown reference, duplicate name, cycle) fails the
	// whole AddJobs call atomically and cannot leave half-registered
	// members behind.
	existingNames := make([]string, 0, len(g.JobIDs))
	nameToID := map[string]string{}
	for _, id := range g.JobIDs {
		if ej := s.snap.Jobs[id]; ej != nil {
			existingNames = append(existingNames, ej.Name)
			nameToID[ej.Name] = id
		}
	}
	if err := policy.ValidateDeps(existingNames, specs); err != nil {
		return nil, err
	}

	out := make([]*domain.Job, 0, len(specs))
	for _, spec := range specs {
		id := fmt.Sprintf("job-%06d", s.snap.NextJobNum)
		s.snap.NextJobNum++
		t := timeout
		if spec.Timeout > 0 {
			t = spec.Timeout
		}
		j := &domain.Job{
			ID:        id,
			GroupID:   groupID,
			Name:      spec.Name,
			Task:      spec.Task,
			Retry:     retry,
			Timeout:   t,
			State:     domain.StatePending,
			Attempts:  []*domain.Attempt{},
			EnqueueAt: nowTime(),
		}
		s.snap.Jobs[id] = j
		s.snap.JobOrder = append(s.snap.JobOrder, id)
		g.JobIDs = append(g.JobIDs, id)
		nameToID[spec.Name] = id
		out = append(out, j)

		s.emitLocked(domain.Event{
			Type:    domain.EvJobSubmitted,
			GroupID: groupID,
			JobID:   id,
			Detail:  fmt.Sprintf("name=%q task=%s/%q timeout=%s max_attempts=%d depends_on=%v", spec.Name, spec.Task.Kind, spec.Task.Payload, t, retry.MaxAttempts, spec.DependsOn),
		})
	}
	// Resolve dependency names to durable job IDs now that every member of
	// the batch has an ID.
	for i, spec := range specs {
		if len(spec.DependsOn) == 0 {
			continue
		}
		deps := make([]string, 0, len(spec.DependsOn))
		for _, dn := range spec.DependsOn {
			deps = append(deps, nameToID[dn])
		}
		s.snap.Jobs[out[i].ID].DependsOn = deps
	}

	// Barrier may release with this batch. For an "all" group this is the
	// expected point; for quorum it may have been open already — in that case
	// admit just the new members below.
	if g.State == domain.GroupPending && policy.ShouldAdmit(g) {
		s.admitLocked(g, "barrier satisfied on submission")
	} else if g.State == domain.GroupRunning {
		// Already-admitted group: late members skip the barrier.
		for _, j := range out {
			s.releaseJobLocked(s.snap.Jobs[j.ID], "late member bypasses open barrier")
		}
		s.propagateDepsLocked(g)
	}
	return out, nil
}

// releaseJobLocked moves an admitted PENDING job to its initial runnable
// state: READY when it has no dependencies, BLOCKED when it must wait for
// them. Caller holds mu.
func (s *FileStore) releaseJobLocked(j *domain.Job, detail string) {
	if j == nil || j.State != domain.StatePending {
		return
	}
	now := nowTime()
	if len(j.DependsOn) == 0 {
		j.State = domain.StateReady
		j.ReadyAt = now
		s.emitLocked(domain.Event{
			Type:    domain.EvJobReady,
			GroupID: j.GroupID,
			JobID:   j.ID,
			Detail:  detail,
		})
		return
	}
	j.State = domain.StateBlocked
	s.emitLocked(domain.Event{
		Type:    domain.EvJobBlocked,
		GroupID: j.GroupID,
		JobID:   j.ID,
		Detail:  fmt.Sprintf("waiting for %d dependencies (%s)", len(j.DependsOn), detail),
	})
}

// admitLocked transitions a pending group to RUNNING and releases every
// member. The whole gate opens atomically under the store mutex: no member
// can be claimed while another is still waiting behind the gate. Members
// with dependencies are released as BLOCKED and immediately reconciled by
// propagateDepsLocked (a late member's dependencies may already be settled).
func (s *FileStore) admitLocked(g *domain.Group, detail string) {
	now := nowTime()
	g.State = domain.GroupRunning
	g.AdmittedAt = now
	s.emitLocked(domain.Event{
		Type:    domain.EvGroupAdmitted,
		GroupID: g.ID,
		Detail:  detail,
	})
	for _, id := range g.JobIDs {
		s.releaseJobLocked(s.snap.Jobs[id], "")
	}
	s.propagateDepsLocked(g)
}

// TryAdmit is driven by the scheduler loop: it finds one still-pending group
// whose barrier is satisfied and releases it atomically. Returns the admitted
// group (clone) when one was admitted.
func (s *FileStore) TryAdmit() (*domain.Group, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.snap.Groups {
		if g.State != domain.GroupPending {
			continue
		}
		if policy.ShouldAdmit(g) {
			s.admitLocked(g, "barrier satisfied")
			return cloneGroup(g), true, nil
		}
	}
	return nil, false, nil
}

// RejectGroup marks a group as not runnable (validation found it can never
// satisfy its barrier). Members become CANCELED+group_rejected: the outcome
// is distinguishable from an executed failure and the group is terminal —
// nothing can ever be left half-pending.
func (s *FileStore) RejectGroup(groupID, reason, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.snap.Groups[groupID]
	if !ok {
		return domain.ErrNotFound
	}
	if g.State != domain.GroupPending {
		return fmt.Errorf("%w: group %s already %s", domain.ErrConflict, groupID, g.State)
	}
	now := nowTime()
	s.emitLocked(domain.Event{
		Type:    domain.EvGroupRejected,
		GroupID: groupID,
		Detail:  fmt.Sprintf("reason=%s: %s", reason, message),
	})
	for _, id := range g.JobIDs {
		j := s.snap.Jobs[id]
		if j != nil && !j.State.Terminal() {
			s.cancelJobLocked(j, domain.ReasonGroupRejected, message, now, false)
		}
	}
	g.State = domain.GroupCanceled
	g.Reason = reason
	g.FinishedAt = now
	s.emitLocked(domain.Event{
		Type:    domain.EvGroupCanceled,
		GroupID: groupID,
		Detail:  fmt.Sprintf("rejected: %s", message),
	})
	return nil
}
