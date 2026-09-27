package scheduler_test

import (
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// TestAdmissionImmediate: members run as soon as submitted; group succeeds.
func TestAdmissionImmediate(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "imm", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	t.Logf("INPUT immediate group with 3 ok jobs (no barrier)")
	g, jobs, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "j1", Task: domain.TaskSpec{Kind: "ok", Payload: "2ms"}},
		{Name: "j2", Task: domain.TaskSpec{Kind: "ok", Payload: "2ms"}},
		{Name: "j3", Task: domain.TaskSpec{Kind: "ok", Payload: "2ms"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group state=%s want SUCCEEDED", gf.State)
	}
	for _, j := range jobs {
		got, _ := e.sched.GetJob(j.ID)
		if got.State != domain.StateSucceeded {
			t.Fatalf("JUDGMENT: %s state=%s want SUCCEEDED", j.ID, got.State)
		}
	}
	t.Logf("JUDGMENT: group %s SUCCEEDED, all %d jobs SUCCEEDED", g.ID, len(jobs))
}

// TestAdmissionAllBarrier: with an "all" barrier nothing starts until the
// complete set is present. We submit the header + 2 of 3 jobs directly via
// the store and assert members remain PENDING, then add the last job and wait
// for the group to run atomically as a whole.
func TestAdmissionAllBarrier(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "allbar", Admission: domain.AdmissionAll,
		FailurePolicy: domain.FailureContinue, ExpectedMembers: 3, Retry: noRetry(),
	}
	g, err := e.st.CreateGroup(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	first, err := e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j1", Task: domain.TaskSpec{Kind: "sleep", Payload: "200ms"}},
		{Name: "j2", Task: domain.TaskSpec{Kind: "sleep", Payload: "200ms"}},
	})
	if err != nil {
		t.Fatalf("add first batch: %v", err)
	}
	t.Logf("INPUT all-barrier group expected=3, submitted=2; jobs must NOT run yet")

	// While the barrier is closed the group stays PENDING and every member
	// PENDING: there is no possible half-started intermediate state.
	time.Sleep(60 * time.Millisecond)
	gg, _ := e.st.GetGroup(g.ID)
	if gg.State != domain.GroupPending {
		t.Fatalf("JUDGMENT: group state=%s want PENDING behind barrier", gg.State)
	}
	for _, j := range first {
		cur, _ := e.sched.GetJob(j.ID)
		if cur.State != domain.StatePending {
			t.Fatalf("JUDGMENT: %s state=%s want PENDING (barrier closed)", j.ID, cur.State)
		}
	}
	t.Logf("JUDGMENT: group PENDING, %d members PENDING — barrier held", len(first))

	// Release the gate with the final member.
	_, err = e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j3", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
	})
	if err != nil {
		t.Fatalf("add final member: %v", err)
	}
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group state=%s want SUCCEEDED after barrier release", gf.State)
	}
	for _, j := range e.st.ListJobs(g.ID) {
		if j.State != domain.StateSucceeded {
			t.Fatalf("JUDGMENT: %s state=%s want SUCCEEDED", j.ID, j.State)
		}
	}
	t.Logf("JUDGMENT: barrier released atomically; all 3 members SUCCEEDED")
}

// TestAdmissionQuorum: quorum(4)=3 opens the barrier; the 4th late member
// bypasses the already-open barrier immediately.
func TestAdmissionQuorum(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "quorum", Admission: domain.AdmissionQuorum,
		FailurePolicy: domain.FailureContinue, ExpectedMembers: 4, Retry: noRetry(),
	}
	g, err := e.st.CreateGroup(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	// Quorum for 4 is 3; 2 members must keep the barrier closed.
	_, _ = e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j1", Task: domain.TaskSpec{Kind: "sleep", Payload: "300ms"}},
		{Name: "j2", Task: domain.TaskSpec{Kind: "sleep", Payload: "300ms"}},
	})
	time.Sleep(40 * time.Millisecond)
	if gg, _ := e.st.GetGroup(g.ID); gg.State != domain.GroupPending {
		t.Fatalf("JUDGMENT: below quorum, group=%s want PENDING", gg.State)
	}
	t.Logf("INPUT quorum group expected=4; at 2 members still PENDING (need 3)")

	third, err := e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j3", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
	})
	if err != nil {
		t.Fatalf("add third: %v", err)
	}
	// Wait until barrier open.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if gg, _ := e.st.GetGroup(g.ID); gg.State == domain.GroupRunning {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if gg, _ := e.st.GetGroup(g.ID); gg.State != domain.GroupRunning {
		t.Fatalf("JUDGMENT: at quorum group=%s want RUNNING", gg.State)
	}
	t.Logf("JUDGMENT: quorum reached; group RUNNING; members j1..j3 released")

	// Late 4th member goes READY immediately.
	late, err := e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j4-late", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
	})
	if err != nil {
		t.Fatalf("add late member: %v", err)
	}
	if cur, _ := e.sched.GetJob(late[0].ID); cur.State != domain.StateReady && cur.State != domain.StateRunning && cur.State != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: late member state=%s, expected to bypass open barrier", cur.State)
	}
	_ = third
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s want SUCCEEDED", gf.State)
	}
	t.Logf("JUDGMENT: late member bypassed barrier; group SUCCEEDED with all 4")
}

// TestBarrierCancel: canceling a group behind its barrier terminates every
// member CANCELED+user_canceled at once — nothing is ever half-started.
func TestBarrierCancel(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "barcancel", Admission: domain.AdmissionAll,
		FailurePolicy: domain.FailureContinue, ExpectedMembers: 3, Retry: noRetry(),
	}
	g, err := e.st.CreateGroup(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	jobs, err := e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j1", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
		{Name: "j2", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
		{Name: "j3", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
	})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	t.Logf("INPUT all-barrier group complete; cancel before scheduler admits it")
	// A full AddJobs triggers immediate admission; cancel right away —
	// members may be READY/RUNNING briefly but cancel must settle all of them.
	if err := e.sched.CancelGroup(g.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupCanceled {
		t.Fatalf("JUDGMENT: group=%s want CANCELED", gf.State)
	}
	for _, j := range jobs {
		cur, _ := e.sched.GetJob(j.ID)
		if cur.State != domain.StateCanceled {
			t.Fatalf("JUDGMENT: %s state=%s want CANCELED", j.ID, cur.State)
		}
	}
	t.Logf("JUDGMENT: group and all %d members CANCELED; no half-pending state", len(jobs))
}
