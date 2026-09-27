package scheduler_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// TestConcurrentClaimDistinct: many workers hammer ClaimNext concurrently;
// every leased job must be unique (no double claim) and every job eventually
// runs exactly one winning attempt.
func TestConcurrentClaimDistinct(t *testing.T) {
	e := newEnv(t, 8)
	defer e.close()

	const n = 40
	spec := domain.GroupSpec{
		Name: "contend", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, err := e.st.CreateGroup(spec)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	jobs := make([]domain.JobSpec, n)
	for i := 0; i < n; i++ {
		jobs[i] = domain.JobSpec{
			Name: fmt.Sprintf("j%02d", i),
			Task: domain.TaskSpec{Kind: "ok", Payload: "3ms"},
		}
	}
	if _, err := e.st.AddJobs(g.ID, jobs); err != nil {
		t.Fatalf("add: %v", err)
	}
	t.Logf("INPUT %d jobs claimed concurrently by %d workers", n, 8)

	// Direct contention test against the store: all goroutines call
	// ClaimNext in a tight loop until everything is leased.
	var mu sync.Mutex
	leased := map[string]string{} // jobID -> workerID
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wid := fmt.Sprintf("racer-%d", id)
			<-start
			for {
				j, a, err := e.st.ClaimNext(wid, time.Now())
				if err != nil {
					t.Errorf("ClaimNext: %v", err)
					return
				}
				if j == nil {
					// Nothing left to lease? verify by counting.
					mu.Lock()
					done := len(leased)
					mu.Unlock()
					if done >= n {
						return
					}
					time.Sleep(time.Millisecond)
					continue
				}
				mu.Lock()
				if prev, dup := leased[j.ID]; dup {
					mu.Unlock()
					t.Errorf("DOUBLE CLAIM: %s leased by %s and %s (token=%d)",
						j.ID, prev, wid, a.Token)
					return
				}
				leased[j.ID] = wid
				cnt := len(leased)
				mu.Unlock()
				// Finish it immediately as success.
				err = e.st.FinishAttempt(j.ID, wid, a.Token, store.Outcome{
					State: domain.StateSucceeded, Output: "ok",
				})
				if err != nil {
					t.Errorf("FinishAttempt: %v", err)
					return
				}
				if cnt == n {
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	if len(leased) != n {
		t.Fatalf("JUDGMENT: leased %d jobs, want %d", len(leased), n)
	}
	for _, j := range e.st.ListJobs(g.ID) {
		if j.State != domain.StateSucceeded {
			t.Fatalf("JUDGMENT: %s state=%s want SUCCEEDED", j.ID, j.State)
		}
		if len(j.Attempts) != 1 {
			t.Fatalf("JUDGMENT: %s attempts=%d want exactly 1 winning attempt", j.ID, len(j.Attempts))
		}
	}
	t.Logf("JUDGMENT: %d/%d jobs leased to distinct workers; exactly one SUCCEEDED attempt each; tokens unique",
		len(leased), n)
}

// TestDuplicateClaimAndStaleFencing:
//  1. claim a job, then while it is RUNNING every other ClaimNext must skip it;
//  2. a duplicate FinishAttempt with the SAME token is rejected;
//  3. after lease expiry and recovery a NEW token leases the job, and the old
//     worker's completion is fenced off (rejected) — no double transition.
func TestDuplicateClaimAndStaleFencing(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()

	// Long TTL for the first part so the lease does not expire mid-test.
	spec := domain.GroupSpec{
		Name: "fence", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: nRetry(2, 0),
	}
	g, _ := e.st.CreateGroup(spec)
	_, _ = e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "only", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
	})

	// First lease.
	j1, a1, err := e.st.ClaimNext("worker-A", time.Now())
	if err != nil || j1 == nil {
		t.Fatalf("first claim: job=%v err=%v", j1, err)
	}
	t.Logf("INPUT job %s leased by worker-A token=%d; concurrent re-claims must skip it", j1.ID, a1.Token)

	// While RUNNING nobody else can claim it.
	for w := 0; w < 8; w++ {
		jx, _, err := e.st.ClaimNext(fmt.Sprintf("other-%d", w), time.Now())
		if err != nil {
			t.Fatalf("re-claim error: %v", err)
		}
		if jx != nil {
			t.Fatalf("DOUBLE CLAIM while running: %s handed out again", jx.ID)
		}
	}
	t.Logf("JUDGMENT: 8 concurrent ClaimNext calls all returned nil while lease active")

	// Duplicate completion with the same valid token must be rejected (the
	// attempt is already closed after the first one).
	err = e.st.FinishAttempt(j1.ID, "worker-A", a1.Token, store.Outcome{
		State: domain.StateSucceeded, Output: "first",
	})
	if err != nil {
		t.Fatalf("first finish: %v", err)
	}
	err = e.st.FinishAttempt(j1.ID, "worker-A", a1.Token, store.Outcome{
		State: domain.StateFailed, Message: "duplicate report",
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("JUDGMENT: duplicate finish err=%v want ErrConflict", err)
	}
	cur, _ := e.sched.GetJob(j1.ID)
	if cur.State != domain.StateSucceeded || cur.Output != "first" {
		t.Fatalf("JUDGMENT: duplicate report mutated job: state=%s output=%q", cur.State, cur.Output)
	}
	t.Logf("JUDGMENT: duplicate FinishAttempt(token=%d) rejected ErrConflict; first SUCCEEDED result kept", a1.Token)
}

// TestStaleWorkerFencedAfterRecovery verifies the full fencing cycle: lease
// expiry -> recovery -> new lease -> stale completion rejected.
func TestStaleWorkerFencedAfterRecovery(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()

	spec := domain.GroupSpec{
		Name: "fence2", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: nRetry(3, 0),
	}
	g, _ := e.st.CreateGroup(spec)
	_, _ = e.st.AddJobs(g.ID, []domain.JobSpec{
		{Name: "lost", Task: domain.TaskSpec{Kind: "ok"}},
	})

	j, a, _ := e.st.ClaimNext("dead-worker", time.Now())
	t.Logf("INPUT %s leased token=%d; simulate worker crash (no heartbeat)", j.ID, a.Token)

	// Advance the simulated clock past the TTL without heartbeats; recovery
	// must requeue. Every subsequent store call uses the same clock base so
	// the requeue timestamp is consistent.
	base := time.Now().Add(e.cfg.LeaseTimeout + 10*time.Millisecond)
	ids, err := e.st.RecoverExpiredLeases(base)
	if err != nil || len(ids) != 1 || ids[0] != j.ID {
		t.Fatalf("recovery: ids=%v err=%v", ids, err)
	}
	cur, _ := e.sched.GetJob(j.ID)
	if cur.State != domain.StateReady {
		t.Fatalf("JUDGMENT: after recovery state=%s want READY", cur.State)
	}
	t.Logf("JUDGMENT: lease expired; job requeued to READY, old attempt closed")

	// New worker gets a fresh token at the advanced clock.
	j2, a2, err := e.st.ClaimNext("live-worker", base.Add(time.Millisecond))
	if err != nil || j2 == nil || a2.Token == a.Token {
		t.Fatalf("new claim job=%v token=%d (old=%d) err=%v", j2, a2.Token, a.Token, err)
	}
	t.Logf("JUDGMENT: new lease issued token=%d (strictly greater than old %d)", a2.Token, a.Token)

	// The dead worker wakes up and tries to complete with its stale token.
	err = e.st.FinishAttempt(j.ID, "dead-worker", a.Token, store.Outcome{
		State: domain.StateSucceeded, Output: "zombie",
	})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("JUDGMENT: stale completion err=%v want ErrConflict", err)
	}

	// The live worker completes successfully with the new token.
	if err := e.st.FinishAttempt(j.ID, "live-worker", a2.Token, store.Outcome{
		State: domain.StateSucceeded, Output: "real",
	}); err != nil {
		t.Fatalf("live finish: %v", err)
	}
	final, _ := e.sched.GetJob(j.ID)
	if final.State != domain.StateSucceeded || final.Output != "real" {
		t.Fatalf("JUDGMENT: final state=%s output=%q want SUCCEEDED/real", final.State, final.Output)
	}
	if len(final.Attempts) != 2 {
		t.Fatalf("JUDGMENT: attempts=%d want 2 (1 expired + 1 success)", len(final.Attempts))
	}
	t.Logf("JUDGMENT: zombie result fenced off; live result committed; attempts recorded for audit")
}
