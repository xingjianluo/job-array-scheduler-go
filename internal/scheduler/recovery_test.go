package scheduler_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/scheduler"
	"github.com/xingjianluo/job-array-scheduler-go/internal/stats"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
	"github.com/xingjianluo/job-array-scheduler-go/internal/task"
)

// TestRestartExactlyOnce: the worker "process dies" after recording its side
// effect (crash task) without reporting. A brand new engine reopened from the
// same data directory reaps the expired lease and completes the job, while
// the task ledger proves the body produced its effect exactly once.
func TestRestartExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	cfg := domain.DefaultConfig()
	cfg.DataDir = dir
	cfg.NumWorkers = 2
	cfg.MaxParallel = 2
	cfg.HeartbeatInterval = 10 * time.Millisecond
	cfg.LeaseTimeout = 120 * time.Millisecond
	cfg.PollInterval = 2 * time.Millisecond

	st, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open1: %v", err)
	}
	reg := task.NewRegistry(dir)
	sched := scheduler.New(cfg, st, reg)
	sched.SetLogger(func(f string, a ...any) { t.Logf(f, a...) })
	sched.Start()

	spec := domain.GroupSpec{
		Name: "restart", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: nRetry(3, 0),
	}
	g, jobs, err := sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "critical", Task: domain.TaskSpec{Kind: "crash", Payload: "EFFECT-42"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT crash job %s: effect persisted, worker dies before completion; then full restart", jobs[0].ID)

	// The crash worker died immediately after leasing; its lease is still
	// valid. Shut the whole process down BEFORE the lease expires so that
	// requeue is performed entirely by the restarted engine from durable
	// state (snapshot + log), not by the old in-memory recovery loop.
	waitFor(jobs[0].ID, domain.StateRunning, &testEnv{t: t, cfg: cfg, st: st, sched: sched}, time.Second)
	time.Sleep(30 * time.Millisecond) // < LeaseTimeout: lease still held on disk
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	if err := sched.Shutdown(ctx1); err != nil {
		t.Fatalf("shutdown1: %v", err)
	}
	cancel1()
	if err := st.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	// ---- Simulated process restart: reopen all durable state. ----
	st2, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	reg2 := task.NewRegistry(dir)
	sched2 := scheduler.New(cfg, st2, reg2)
	sched2.SetLogger(func(f string, a ...any) { t.Logf("(restarted) "+f, a...) })
	sched2.Start()

	gf, err := sched2.WaitGroup(context.Background(), g.ID)
	if err != nil {
		t.Fatalf("wait after restart: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: after restart group=%s want SUCCEEDED", gf.State)
	}
	final, _ := sched2.GetJob(jobs[0].ID)
	if final.State != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: job=%s want SUCCEEDED", final.State)
	}
	if final.Output != "EFFECT-42" {
		t.Fatalf("JUDGMENT: output=%q want EFFECT-42", final.Output)
	}

	// BodyRuns counts only genuine body executions in THIS process: the
	// restarted attempt (key #2) must have been served entirely from the
	// ledger replay, i.e. zero body runs.
	if runs := reg2.BodyRuns(jobs[0].ID, 2); runs != 0 {
		t.Fatalf("JUDGMENT: restarted attempt executed its body %d times; want 0 (ledger replay only)", runs)
	}
	t.Logf("JUDGMENT: restarted attempt body-runs=0; the crash-replayed side effect was not re-executed")

	if len(final.Attempts) != 2 {
		t.Fatalf("JUDGMENT: attempts=%d want 2 (crashed + replay)", len(final.Attempts))
	}
	t.Logf("JUDGMENT: restart recovered job to SUCCEEDED; output=%q; 2 attempts recorded, second served from durable ledger",
		final.Output)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := sched2.Shutdown(ctx2); err != nil {
		t.Fatalf("shutdown2: %v", err)
	}
	_ = st2.Close()
}

// TestEventTraceAndStableStats: the append-only event log contains the full,
// ordered decision trail and the stats summary is stable across rebuilds.
func TestEventTraceAndStableStats(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "trace", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureFailFast, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "fast-bad", Task: domain.TaskSpec{Kind: "fail", Payload: "trace-boom"}},
		{Name: "long-peer", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT fail-fast group fast-fail+long-peer; will assert event trace + stats classification")
	_, _ = e.sched.WaitGroup(shutdownCtx(t), g.ID)

	events := e.st.Events()
	seq := int64(0)
	counts := map[string]int{}
	var claimTokens []int64
	for _, ev := range events {
		if ev.Seq <= seq {
			t.Fatalf("JUDGMENT: event seq not strictly increasing: %d after %d", ev.Seq, seq)
		}
		seq = ev.Seq
		counts[ev.Type]++
		if ev.Type == domain.EvJobClaimed {
			claimTokens = append(claimTokens, ev.Token)
		}
	}
	// Every important decision type is represented.
	required := []string{
		domain.EvGroupSubmitted, domain.EvJobSubmitted, domain.EvGroupAdmitted,
		domain.EvJobClaimed, domain.EvJobFailed, domain.EvJobCanceled,
		domain.EvGroupFailed,
	}
	for _, ty := range required {
		if counts[ty] == 0 {
			t.Fatalf("JUDGMENT: event %q missing from trace (counts=%v)", ty, counts)
		}
	}
	t.Logf("JUDGMENT: %d events, strictly ordered seq 1..%d; tokens issued: %v; all key event types present",
		len(events), seq, claimTokens)

	// Stats classification + stability: build twice, compare reports.
	s1 := stats.Build(e.st).String()
	s2 := stats.Build(e.st).String()
	if s1 != s2 {
		t.Fatalf("JUDGMENT: stats report not stable across rebuilds")
	}
	if !containsAll(s1, "failed=1", "canceled=1", "reason group_failfast") {
		t.Fatalf("JUDGMENT: stats missing expected classification:\n%s", s1)
	}
	t.Logf("JUDGMENT: stable stats report with failed/canceled/group_failfast classification:\n%s", s1)
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
