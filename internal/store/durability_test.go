package store

import (
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

func testCfg(dir string) domain.Config {
	c := domain.DefaultConfig()
	c.DataDir = dir
	c.LeaseTimeout = 100 * time.Millisecond
	c.HeartbeatInterval = 10 * time.Millisecond
	return c
}

// TestSnapshotDurabilityAcrossReopen: committed state survives close/reopen
// with identical job/group state, counters and event sequence.
func TestSnapshotDurabilityAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(dir)

	st, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := st.CreateGroup(domain.GroupSpec{Name: "g", Admission: domain.AdmissionImmediate, Retry: RetryPolicy1()})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "j1", Task: domain.TaskSpec{Kind: "ok"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	j, a, err := st.ClaimNext("w1", time.Now())
	if err != nil || j == nil {
		t.Fatalf("claim: %v %v", j, err)
	}
	if err := st.FinishAttempt(j.ID, "w1", a.Token, Outcome{State: domain.StateSucceeded, Output: "out"}); err != nil {
		t.Fatal(err)
	}
	eventsBefore := len(st.Events())
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: same state, same history.
	st2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()

	g2, err := st2.GetGroup(g.ID)
	if err != nil {
		t.Fatalf("group gone: %v", err)
	}
	if g2.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: reopened group=%s want SUCCEEDED", g2.State)
	}
	j2, err := st2.GetJob(jobs[0].ID)
	if err != nil || j2.State != domain.StateSucceeded || j2.Output != "out" {
		t.Fatalf("JUDGMENT: reopened job state=%v output=%q err=%v", j2, j2.Output, err)
	}
	if eventsAfter := len(st2.Events()); eventsAfter != eventsBefore {
		t.Fatalf("JUDGMENT: events after reopen=%d want %d", eventsAfter, eventsBefore)
	}
	t.Logf("JUDGMENT: state.json + events.log survived reopen: group/job terminal, %d events re-read", eventsBefore)
}

// RetryPolicy1 returns a no-retry policy (helper kept here to avoid an import
// cycle with scheduler tests).
func RetryPolicy1() domain.RetryPolicy {
	return domain.RetryPolicy{MaxAttempts: 1}
}

// TestRecoverAfterReopen: a lease held when the "process dies" is requeued by
// the reopened store purely from persisted heartbeat state.
func TestRecoverAfterReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(dir)

	st, _ := Open(dir, cfg)
	g, _ := st.CreateGroup(domain.GroupSpec{Name: "g", Admission: domain.AdmissionImmediate, Retry: domain.RetryPolicy{MaxAttempts: 2}})
	jobs, _ := st.AddJobs(g.ID, []domain.JobSpec{{Name: "j", Task: domain.TaskSpec{Kind: "ok"}}})
	j, a, _ := st.ClaimNext("w1", time.Now())
	_ = a
	_ = jobs
	// Flush heartbeat/claim state to snapshot, then "crash" without finishing.
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	st2, _ := Open(dir, cfg)
	defer st2.Close()
	ids, err := st2.RecoverExpiredLeases(time.Now().Add(cfg.LeaseTimeout + 5*time.Millisecond))
	if err != nil || len(ids) != 1 || ids[0] != j.ID {
		t.Fatalf("JUDGMENT: recovery ids=%v err=%v", ids, err)
	}
	j2, _ := st2.GetJob(j.ID)
	if j2.State != domain.StateReady {
		t.Fatalf("JUDGMENT: after reopen+recovery state=%s want READY", j2.State)
	}
	t.Log("JUDGMENT: lease held at crash time requeued after reopen from durable heartbeat")
}

// TestRecoverAfterReopen: a lease held when the "process dies" is requeued by
// the reopened store purely from persisted heartbeat state.
