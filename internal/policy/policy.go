// Package policy contains pure scheduling-policy helpers: barrier admission
// rules, quorum arithmetic and terminal group-state derivation. It depends
// only on domain types and has no side effects, so every rule here is easy
// to unit test and reason about.
package policy

import "github.com/xingjianluo/job-array-scheduler-go/internal/domain"

// Quorum returns the majority threshold for n members: n/2+1.
//
//	n=0 -> 1, n=1 -> 1, n=2 -> 2, n=3 -> 2, n=4 -> 3 ...
func Quorum(n int) int {
	if n < 1 {
		n = 1
	}
	return n/2 + 1
}

// ShouldAdmit reports whether a pending group's barrier may be released given
// its current member count:
//
//   - immediate: one member is enough (the barrier never really blocks);
//   - all:       every ExpectedMembers member must be present;
//   - quorum:    a majority (ExpectedMembers/2+1) must be present. Late
//     members after admission bypass the barrier (see AddJobs).
//
// A group whose admission condition can never be met (expected <= 0 for a
// barrier policy) is NOT admitted here; the submitter validates specs.
func ShouldAdmit(g *domain.Group) bool {
	n := len(g.JobIDs)
	switch g.Spec.Admission {
	case domain.AdmissionImmediate:
		return n >= 1
	case domain.AdmissionAll:
		return g.Spec.ExpectedMembers > 0 && n >= g.Spec.ExpectedMembers
	case domain.AdmissionQuorum:
		return g.Spec.ExpectedMembers > 0 && n >= Quorum(g.Spec.ExpectedMembers)
	default:
		return false
	}
}

// GroupTerminal derives the terminal group state from its final members.
// Precondition: every job is in a terminal state.
//
//   - at least one SUCCEEDED                         -> SUCCEEDED;
//   - otherwise any FAILED / TIMED_OUT member        -> FAILED
//     (the non-success is an execution failure, not a cancellation);
//   - otherwise (every member CANCELED)              -> CANCELED.
//
// Fail-fast groups with one failed member therefore finish FAILED with the
// remaining members individually CANCELED+group_failfast, keeping the two
// outcomes explicitly distinguishable.
func GroupTerminal(jobs []*domain.Job) domain.GroupState {
	anySuccess := false
	anyFailure := false
	for _, j := range jobs {
		switch j.State {
		case domain.StateSucceeded:
			anySuccess = true
		case domain.StateFailed, domain.StateTimedOut:
			anyFailure = true
		}
	}
	switch {
	case anySuccess:
		return domain.GroupSucceeded
	case anyFailure:
		return domain.GroupFailed
	default:
		return domain.GroupCanceled
	}
}
