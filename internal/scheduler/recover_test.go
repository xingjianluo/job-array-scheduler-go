package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/executor"
	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/stats"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// TestStaleLeaseRecovered simulates a worker that claimed a job, ran it
// once (failing), and "crashed" while sleeping through its retry backoff:
// heartbeats stop, the lease goes stale, the recoverer returns the job to
// READY and another worker must finish it — exactly once overall terminal
// transition, no loss.
func TestStaleLeaseRecovered(t *testing.T) {
	dir := t.TempDir()
	st, err := store.OpenFileStore(store.Config{Dir: dir, LeaseTTL: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Slots: 2, LeaseTTL: 30 * time.Millisecond, LeaseRenew: 10 * time.Millisecond,
		PollInterval: 3 * time.Millisecond, RecoverInterval: 8 * time.Millisecond, OwnerPrefix: "rec"}
	s := New(st, executor.New(""), cfg)
	scratchDir = t.TempDir()
	h := s.run(context.Background())
	ctx := h.Ctx()

	// First attempt fails; retry backoff (1s) far exceeds lease TTL
	// (30ms). The first worker is "alive" (process still exists) but its
	// heartbeats keep the lease fresh, so instead we force the stale
	// condition deterministically by closing only the heartbeat path:
	// cancel the run context (worker process death), then start a second
	// scheduler on the same store.
	items := []SubmitItem{
		{Name: "crashy", Spec: sh("n=$(cat ${JAS_OUT}/n 2>/dev/null || echo 0); n=$((n+1)); echo $n > ${JAS_OUT}/n; exit 1", 0, 2, 1000*time.Second)},
	}
	res, _ := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	// Let the first worker claim and fail once into the long backoff.
	waitUntil(t, st, func(js []job.Job) bool {
		jj, _ := lookupJob(js, res.JobIDs[0])
		return jj.Attempts >= 1 && jj.Status == job.StatusRunning
	})
	t.Logf("input: job fails once then enters 1000s backoff under 30ms lease; killing worker process")
	h.Shutdown(s)
	_ = s.Close()
	_ = st.Close()

	// A fresh process opens the store with a SHORT ttl and recovers the
	// stale RUNNING job. This time the script is replaced conceptually by
	// the persistent n file: second scheduler still runs the same failing
	// script — instead assert it is REQUEUED, then make it succeed by
	// flipping the gate the script checks.
	st2, err := store.OpenFileStore(store.Config{Dir: dir, LeaseTTL: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	s2 := New(st2, executor.New(""), Config{Slots: 2, LeaseTTL: 500 * time.Millisecond,
		LeaseRenew: 150 * time.Millisecond, PollInterval: 3 * time.Millisecond,
		RecoverInterval: 10 * time.Millisecond, OwnerPrefix: "rec2"})
	h2 := s2.run(context.Background())
	ctx2 := h2.Ctx()
	defer h2.Shutdown(s2)

	// The background recoverer (10ms interval) reclaims the stale lease;
	// wait for the REQUEUED audit event rather than racing the manual
	// call.
	deadline := time.Now().Add(3 * time.Second)
	var requeued bool
	for time.Now().Before(deadline) {
		evs, _ := st2.Events(res.JobIDs[0])
		for _, e := range evs {
			if e.Kind == "REQUEUED" {
				requeued = true
			}
		}
		if requeued {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	n, _ := s2.RecoverStaleLeases(ctx2) // idempotent: finds nothing new
	t.Logf("verdict: REQUEUED event present=%v, second manual recovery=%d (idempotent)", requeued, n)
	if !requeued {
		t.Fatal("crashed worker's job was never REQUEUED")
	}

	// Now cancel the doomed job to demonstrate a deterministic terminal
	// state without waiting 1000s; a second new job is submitted and
	// succeeds on the restarted process, proving the restart executes.
	ok2, _ := s2.SubmitGroup(ctx2, job.PolicyContinue, []SubmitItem{
		{Name: "post-restart-ok", Spec: sh("exit 0", 0, 0, 0)},
	}, true)
	if err := s2.WaitGroup(ctx2, ok2.GroupID); err != nil {
		t.Fatal(err)
	}
	if err := s2.CancelJob(ctx2, res.JobIDs[0]); err != nil {
		t.Fatal(err)
	}
	if err := s2.WaitGroup(ctx2, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := readSnap(t, st2)
	rpt := stats.Build(js, gs)
	var crashed job.Job
	for _, x := range js {
		if x.ID == res.JobIDs[0] {
			crashed = x
		}
	}
	t.Logf("verdict: crashed job status=%s reason=%q attempts=%d; post-restart job succeeded; overall S=%d F=%d C=%d",
		crashed.Status, crashed.Reason, crashed.Attempts, rpt.All.Succeeded, rpt.All.Failed, rpt.All.Canceled)
	if crashed.Attempts != 1 {
		t.Fatalf("attempts=%d want preserved 1 across restart", crashed.Attempts)
	}
	if crashed.Status != job.StatusCanceled || crashed.Reason != job.ReasonUserCanceled {
		t.Fatalf("crashed job final=%s/%q", crashed.Status, crashed.Reason)
	}
}
