package policy

import (
	"testing"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

func TestQuorum(t *testing.T) {
	cases := map[int]int{0: 1, 1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4, 7: 4}
	for n, want := range cases {
		if got := Quorum(n); got != want {
			t.Fatalf("Quorum(%d)=%d want %d", n, got, want)
		}
	}
}

func g(adm domain.AdmissionPolicy, expected, members int) *domain.Group {
	g := &domain.Group{Spec: domain.GroupSpec{Admission: adm, ExpectedMembers: expected}, State: domain.GroupPending}
	for i := 0; i < members; i++ {
		g.JobIDs = append(g.JobIDs, "j")
	}
	return g
}

func TestShouldAdmit(t *testing.T) {
	// immediate: first member opens.
	if !ShouldAdmit(g(domain.AdmissionImmediate, 0, 1)) {
		t.Fatal("immediate with 1 member should admit")
	}
	// all: only the complete set opens.
	if ShouldAdmit(g(domain.AdmissionAll, 3, 2)) {
		t.Fatal("all with 2/3 must NOT admit")
	}
	if !ShouldAdmit(g(domain.AdmissionAll, 3, 3)) {
		t.Fatal("all with 3/3 must admit")
	}
	// quorum(4)=3.
	if ShouldAdmit(g(domain.AdmissionQuorum, 4, 2)) {
		t.Fatal("quorum 4 at 2 must NOT admit")
	}
	if !ShouldAdmit(g(domain.AdmissionQuorum, 4, 3)) {
		t.Fatal("quorum 4 at 3 must admit")
	}
	t.Log("JUDGMENT: admission table correct for immediate/all/quorum")
}

func jb(id string, st domain.JobState) *domain.Job { return &domain.Job{ID: id, State: st} }

func TestGroupTerminal(t *testing.T) {
	cases := []struct {
		name string
		jobs []*domain.Job
		want domain.GroupState
	}{
		{"all-success", []*domain.Job{jb("1", domain.StateSucceeded), jb("2", domain.StateSucceeded)}, domain.GroupSucceeded},
		{"mixed-success-fail", []*domain.Job{jb("1", domain.StateSucceeded), jb("2", domain.StateFailed), jb("3", domain.StateCanceled)}, domain.GroupSucceeded},
		{"fail-plus-cancel", []*domain.Job{jb("1", domain.StateFailed), jb("2", domain.StateCanceled)}, domain.GroupFailed},
		{"timeout-plus-cancel", []*domain.Job{jb("1", domain.StateTimedOut), jb("2", domain.StateCanceled)}, domain.GroupFailed},
		{"all-cancel", []*domain.Job{jb("1", domain.StateCanceled), jb("2", domain.StateCanceled)}, domain.GroupCanceled},
	}
	for _, c := range cases {
		if got := GroupTerminal(c.jobs); got != c.want {
			t.Fatalf("%s: got %s want %s", c.name, got, c.want)
		}
	}
	t.Log("JUDGMENT: group terminal classification table correct (success/fail/cancel distinguishable)")
}
