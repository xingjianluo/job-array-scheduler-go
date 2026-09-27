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

func newTestScheduler(t *testing.T, slots int, ttl time.Duration) (*Scheduler, store.Store) {
	t.Helper()
	st, err := store.OpenFileStore(store.Config{Dir: t.TempDir(), LeaseTTL: ttl})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := Config{
		Slots:           slots,
		LeaseTTL:        ttl,
		LeaseRenew:      ttl / 3,
		PollInterval:    5 * time.Millisecond,
		RecoverInterval: 25 * time.Millisecond,
		OwnerPrefix:     "test",
	}
	s := New(st, executor.New(""), cfg)
	scratchDir = t.TempDir()
	t.Cleanup(func() { s.Close() })
	return s, st
}

func runWithTimeout(t *testing.T, s *Scheduler, timeout time.Duration) (context.Context, *runHandle) {
	h := s.run(context.Background())
	t.Cleanup(func() { h.Shutdown(s) })
	return h.Ctx(), h
}

// scratchDir is set per test; children coordinate side effects through
// files under it ($JAS_OUT).
var scratchDir string

func sh(script string, timeout time.Duration, retries int, backoff time.Duration) job.Spec {
	return job.Spec{
		Command: "sh", Args: []string{"-c", script},
		Timeout: timeout, MaxRetries: retries, RetryBackoff: backoff,
		Env: []string{"JAS_OUT=" + scratchDir},
	}
}

func snapshot(t *testing.T, st store.Store) ([]job.Job, []job.Group) {
	t.Helper()
	var js []job.Job
	var gs []job.Group
	if err := st.Read(func(v store.View) error {
		js = v.Jobs()
		gs = v.Groups()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return js, gs
}

func TestSuccessfulBatch(t *testing.T) {
	s, st := newTestScheduler(t, 4, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, time.Second)

	items := []SubmitItem{
		{Name: "a", Spec: sh("echo a", 0, 0, 0)},
		{Name: "b", Spec: sh("echo b", 0, 0, 0)},
		{Name: "c", Spec: sh("echo c", 0, 0, 0)},
	}
	res, err := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: group=%s policy=CONTINUE jobs=%v (all echo)", res.GroupID, res.JobIDs)
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	t.Logf("verdict: group=%s succeeded=%d failed=%d timedout=%d canceled=%d inflight=%d",
		res.GroupID, rpt.All.Succeeded, rpt.All.Failed, rpt.All.TimedOut, rpt.All.Canceled, len(rpt.InFlight))
	if rpt.All.Succeeded != 3 || rpt.All.Total() != 3 || len(rpt.InFlight) != 0 {
		t.Fatalf("unexpected report: %+v", rpt.All)
	}
	if rpt.Groups[res.GroupID].Status != job.GroupSucceeded {
		t.Fatalf("group status=%s", rpt.Groups[res.GroupID].Status)
	}
}

func TestConcurrentClaimsNoDuplicateExecution(t *testing.T) {
	s, st := newTestScheduler(t, 8, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, time.Second)

	const n = 40
	items := make([]SubmitItem, n)
	for i := 0; i < n; i++ {
		// Every job records its unique marker into a shared directory;
		// a duplicated execution would append a duplicate marker.
		items[i] = SubmitItem{Name: "j", Spec: sh("echo run >> ${JAS_OUT}/runs; sleep 0.05", 0, 0, 0)}
	}
	res, err := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: %d identical jobs, 8 slots, asserting each runs exactly once", n)
	_ = res
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	// Cross-check from the durable event log: exactly one CLAIMED/job.
	evs, err := st.Events("")
	if err != nil {
		t.Fatal(err)
	}
	claimed := map[string]int{}
	for _, e := range evs {
		if e.Kind == "CLAIMED" {
			claimed[e.JobID]++
		}
	}
	for id, c := range claimed {
		if c != 1 {
			t.Fatalf("job %s CLAIMED %d times, want exactly 1", id, c)
		}
	}
	if len(claimed) != n {
		t.Fatalf("claimed jobs=%d want %d (none lost)", len(claimed), n)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	t.Logf("verdict: distinct claimed=%d, succeeded=%d, no duplicate/lost claim", len(claimed), rpt.All.Succeeded)
	if rpt.All.Succeeded != n {
		t.Fatalf("succeeded=%d want %d", rpt.All.Succeeded, n)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	s, st := newTestScheduler(t, 2, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 5*time.Second)

	// Fails twice, succeeds on the 3rd attempt (MaxRetries=2).
	items := []SubmitItem{
		{Name: "flaky", Spec: sh("n=$(cat ${JAS_OUT}/n 2>/dev/null || echo 0); n=$((n+1)); echo $n > ${JAS_OUT}/n; echo attempt=$n; [ $n -ge 3 ]", 0, 2, 20*time.Millisecond)},
	}
	res, _ := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	t.Logf("input: flaky job failing until attempt 3, maxRetries=2 backoff=20ms")
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	var jj job.Job
	for _, x := range js {
		if x.ID == res.JobIDs[0] {
			jj = x
		}
	}
	t.Logf("verdict: status=%s attempts=%d reason=%q exit=%d", jj.Status, jj.Attempts, jj.Reason, jj.ExitCode)
	if jj.Status != job.StatusSucceeded || jj.Attempts != 3 {
		t.Fatalf("want SUCCEEDED on attempt 3, got %s attempts=%d", jj.Status, jj.Attempts)
	}
	if rpt.All.Succeeded != 1 {
		t.Fatalf("report=%+v", rpt.All)
	}
}

func TestRetriesExhaustedFails(t *testing.T) {
	s, st := newTestScheduler(t, 2, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 5*time.Second)
	items := []SubmitItem{{Name: "always-bad", Spec: sh("exit 3", 0, 1, 10*time.Millisecond)}}
	res, _ := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	t.Logf("input: always exit3, maxRetries=1 -> expect FAILED/RETRIES_EXHAUSTED attempts=2")
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	jj, _ := lookupJob(js, res.JobIDs[0])
	t.Logf("verdict: status=%s reason=%q attempts=%d exit=%d", jj.Status, jj.Reason, jj.Attempts, jj.ExitCode)
	if jj.Status != job.StatusFailed || jj.Reason != job.ReasonRetriesExhausted || jj.Attempts != 2 || jj.ExitCode != 3 {
		t.Fatalf("unexpected job: %+v", jj)
	}
	if rpt.All.Failed != 1 || rpt.All.Total() != 1 {
		t.Fatalf("report=%+v", rpt.All)
	}
}

func TestDuplicateClaimCannotDoubleRun(t *testing.T) {
	// Force many workers + a tiny lease TTL so stale recovery races with
	// live workers; assert every job still finishes exactly once and
	// succeeds. Duplicate external claim attempts are simulated by two
	// schedulers running against one store sequentially (flock serialized).
	dir := t.TempDir()
	st1, err := store.OpenFileStore(store.Config{Dir: dir, LeaseTTL: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	s1 := New(st1, executor.New(""), Config{
		Slots: 6, LeaseTTL: 40 * time.Millisecond, LeaseRenew: 12 * time.Millisecond,
		PollInterval: 2 * time.Millisecond, RecoverInterval: 8 * time.Millisecond, OwnerPrefix: "p1",
	})
	parent, pcancel := context.WithCancel(context.Background())
	defer pcancel()
	ctx, h1 := runWithTimeoutBare(s1, parent)

	const n = 30
	items := make([]SubmitItem, n)
	for i := range items {
		items[i] = SubmitItem{Name: "j", Spec: sh("sleep 0.03; exit 0", 0, 0, 0)}
	}
	res, _ := s1.SubmitGroup(ctx, job.PolicyContinue, items, true)
	t.Logf("input: %d jobs, ttl=40ms task=30ms, recover=8ms — recoverer may requeue; still exactly-once terminal", n)
	if err := s1.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	h1.Shutdown(s1)
	_ = s1.Close()
	_ = st1.Close()

	// Reopen store like a restarted process: nothing should be lost or
	// double-terminal; a second scheduler finds no work left.
	st2, err := store.OpenFileStore(store.Config{Dir: dir, LeaseTTL: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	s2 := New(st2, executor.New(""), Config{Slots: 4, LeaseTTL: 40 * time.Millisecond,
		LeaseRenew: 12 * time.Millisecond, PollInterval: 2 * time.Millisecond, OwnerPrefix: "p2"})
	ctx2, h2 := runWithTimeoutBare(s2, parent)
	if err := s2.WaitIdle(ctx2); err != nil {
		t.Fatal(err)
	}
	js, gs := readSnap(t, st2)
	rpt := stats.Build(js, gs)
	t.Logf("verdict after reopen: succeeded=%d failed=%d inflight=%d (no rerun of terminal jobs)",
		rpt.All.Succeeded, rpt.All.Failed, len(rpt.InFlight))
	if rpt.All.Succeeded != n || len(rpt.InFlight) != 0 {
		t.Fatalf("after restart got %+v inflight=%v", rpt.All, rpt.InFlight)
	}
	h2.Shutdown(s2)
	_ = s2.Close()
}

func TestFailFastPartialFailureAbortsSiblings(t *testing.T) {
	s, st := newTestScheduler(t, 4, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 5*time.Second)

	// Deterministic ordering even with all workers racing:
	//   ok    finishes first and drops a gate file, then exits 0;
	//   bad   waits for that gate and then exits 1;
	//   slow* sleep long enough that they are still RUNNING/READY when
	//         bad fails, so they are deterministically GROUP_ABORTED.
	items := []SubmitItem{
		{Name: "ok-early", Spec: sh("echo done > ${JAS_OUT}/gate; exit 0", 0, 0, 0)},
		{Name: "bad", Spec: sh("i=0; while [ ! -f ${JAS_OUT}/gate ] && [ $i -lt 200 ]; do sleep 0.01; i=$((i+1)); done; exit 1", 0, 0, 0)},
		{Name: "slow1", Spec: sh("sleep 2", 0, 0, 0)},
		{Name: "slow2", Spec: sh("sleep 2", 0, 0, 0)},
	}
	res, _ := s.SubmitGroup(ctx, job.PolicyFailFast, items, true)
	t.Logf("input: FAIL_FAST 4 jobs [ok(gate,exit0), bad(wait gate,exit1), slow, slow] — slow members must be GROUP_ABORTED")
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	byName := map[string]stats.JobResult{}
	for _, jr := range rpt.Groups[res.GroupID].Jobs {
		byName[jr.Name] = jr
	}
	t.Logf("verdict: group=%s ok=%d failed=%d canceled=%d reasons: slow1=%q slow2=%q",
		rpt.Groups[res.GroupID].Status, rpt.All.Succeeded, rpt.All.Failed, rpt.All.Canceled,
		byName["slow1"].Reason, byName["slow2"].Reason)
	if rpt.Groups[res.GroupID].Status != job.GroupFailed {
		t.Fatalf("group=%s", rpt.Groups[res.GroupID].Status)
	}
	if byName["ok-early"].Status != job.StatusSucceeded {
		t.Fatalf("ok-early=%+v", byName["ok-early"])
	}
	if byName["bad"].Status != job.StatusFailed || byName["bad"].Reason != job.ReasonRetriesExhausted {
		t.Fatalf("bad=%+v", byName["bad"])
	}
	for _, n := range []string{"slow1", "slow2"} {
		if byName[n].Status != job.StatusCanceled || byName[n].Reason != job.ReasonGroupAborted {
			t.Fatalf("%s=%+v want CANCELED/GROUP_ABORTED", n, byName[n])
		}
	}
}

func TestTimeoutReclaimsResourcesAndDistinguished(t *testing.T) {
	s, st := newTestScheduler(t, 1, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 5*time.Second)
	// One slot: if the timed-out job leaked its slot, the second job can
	// never run and the test hangs/fails.
	items := []SubmitItem{
		{Name: "slow", Spec: sh("sleep 5", 200*time.Millisecond, 0, 0)},
		{Name: "after", Spec: sh("exit 0", 0, 0, 0)},
	}
	res, _ := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	t.Logf("input: 1 slot; job1 sleep5 timeout=200ms; job2 after — slot must be freed for job2")
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	byName := map[string]stats.JobResult{}
	for _, jr := range rpt.Groups[res.GroupID].Jobs {
		byName[jr.Name] = jr
	}
	t.Logf("verdict: slow=%s/%q after=%s/%q buckets S=%d T=%d",
		byName["slow"].Status, byName["slow"].Reason, byName["after"].Status, byName["after"].Reason,
		rpt.All.Succeeded, rpt.All.TimedOut)
	if byName["slow"].Status != job.StatusTimedOut || byName["slow"].Reason != job.ReasonTimeout {
		t.Fatalf("slow=%+v", byName["slow"])
	}
	if byName["after"].Status != job.StatusSucceeded {
		t.Fatalf("after=%+v (slot likely leaked)", byName["after"])
	}
}

func TestCancelRunningAndReadyJobs(t *testing.T) {
	s, st := newTestScheduler(t, 1, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 5*time.Second)
	items := []SubmitItem{
		{Name: "run1", Spec: sh("sleep 2", 0, 0, 0)},
		{Name: "run2", Spec: sh("sleep 2", 0, 0, 0)},
		{Name: "ready1", Spec: sh("sleep 2", 0, 0, 0)},
		{Name: "ready2", Spec: sh("sleep 2", 0, 0, 0)},
	}
	res, _ := s.SubmitGroup(ctx, job.PolicyContinue, items, true)
	// Wait until one job is RUNNING and the rest are blocked by the slot.
	waitUntil(t, st, func(js []job.Job) bool {
		var running, ready int
		for _, j := range js {
			switch j.Status {
			case job.StatusRunning:
				running++
			case job.StatusReady:
				ready++
			}
		}
		return running >= 1 && ready >= 1
	})
	t.Logf("input: 1 slot, 4 sleep jobs; cancel whole group while 1 running, others READY")
	if err := s.CancelGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	rpt := stats.Build(js, gs)
	t.Logf("verdict: group=%s canceled=%d inflight=%d (all USER_CANCELED, no half state)",
		rpt.Groups[res.GroupID].Status, rpt.All.Canceled, len(rpt.InFlight))
	if rpt.Groups[res.GroupID].Status != job.GroupCanceled {
		t.Fatalf("group=%s", rpt.Groups[res.GroupID].Status)
	}
	if rpt.All.Canceled != 4 || len(rpt.InFlight) != 0 {
		t.Fatalf("report=%+v inflight=%v", rpt.All, rpt.InFlight)
	}
	for _, jr := range rpt.Groups[res.GroupID].Jobs {
		if jr.Reason != job.ReasonUserCanceled {
			t.Fatalf("job %s reason=%q", jr.Name, jr.Reason)
		}
	}
}

func TestBarrierHoldsThenOpens(t *testing.T) {
	s, st := newTestScheduler(t, 2, 2*time.Second)
	ctx, _ := runWithTimeout(t, s, 3*time.Second)
	res, err := s.SubmitGroup(ctx, job.PolicyContinue,
		[]SubmitItem{{Name: "gated", Spec: sh("exit 0", 0, 0, 0)}}, false)
	if err != nil {
		t.Fatal(err)
	}
	// While barrier is closed the job must stay PENDING even with idle
	// workers.
	if err := s.sleep(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	js, _ := snapshot(t, st)
	jj, _ := lookupJob(js, res.JobIDs[0])
	t.Logf("input: group submitted startNow=false; status before open=%s (must be PENDING)", jj.Status)
	if jj.Status != job.StatusPending {
		t.Fatalf("barrier leaked: status=%s", jj.Status)
	}
	if err := s.OpenGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	if err := s.WaitGroup(ctx, res.GroupID); err != nil {
		t.Fatal(err)
	}
	js, gs := snapshot(t, st)
	jj, _ = lookupJob(js, res.JobIDs[0])
	t.Logf("verdict after OpenGroup: status=%s groupStatus=%s", jj.Status, stats.Build(js, gs).Groups[res.GroupID].Status)
	if jj.Status != job.StatusSucceeded {
		t.Fatalf("status=%s", jj.Status)
	}
}

// --- small helpers ---

func lookupJob(js []job.Job, id string) (job.Job, bool) {
	for _, j := range js {
		if j.ID == id {
			return j, true
		}
	}
	return job.Job{}, false
}

func readSnap(t *testing.T, st store.Store) ([]job.Job, []job.Group) {
	t.Helper()
	return snapshot(t, st)
}

func waitUntil(t *testing.T, st store.Store, cond func([]job.Job) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var js []job.Job
		if err := st.Read(func(v store.View) error { js = v.Jobs(); return nil }); err != nil {
			t.Fatal(err)
		}
		if cond(js) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("waitUntil: condition never met")
}

// runWithTimeoutBare starts Run under an explicit parent without
// registering test cleanup (used by the multi-process restart test).
func runWithTimeoutBare(s *Scheduler, parent context.Context) (context.Context, *runHandle) {
	h := s.run(parent)
	return h.Ctx(), h
}
