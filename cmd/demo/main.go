// Command demo runs a deterministic local end-to-end scenario and prints, for
// every scenario, the key inputs, the judgment basis (state + reason + token
// trace) and the final classified summary.
//
// Scenarios:
//  1. barrier batch ("all") with one flaky job that needs 3 attempts;
//  2. fail-fast group: one failing member cancels the peers;
//  3. a job that exceeds its timeout (terminal TIMED_OUT, no retry);
//  4. a long job canceled through the API (CANCELED + user_canceled);
//  5. process-restart recovery: a "crash" task whose worker dies after the
//     durable side effect; a fresh scheduler reopens the store, reaps the
//     lease and the job succeeds exactly once (no duplicate effect).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/scheduler"
	"github.com/xingjianluo/job-array-scheduler-go/internal/stats"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
	"github.com/xingjianluo/job-array-scheduler-go/internal/task"
)

func main() {
	const dir = ".scheduler-data"
	_ = os.RemoveAll(dir)

	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	logf := logger.Printf

	cfg := domain.DefaultConfig()
	cfg.DataDir = dir
	cfg.NumWorkers = 4
	cfg.MaxParallel = 4
	cfg.LeaseTimeout = 250 * time.Millisecond // short so the restart case is quick

	st, err := store.Open(dir, cfg)
	must(err)
	reg := task.NewRegistry(dir)
	sched := scheduler.New(cfg, st, reg)
	sched.SetLogger(logf)
	sched.Start()

	ctx := context.Background()

	// ---- Scenario 1: barrier + retry/backoff ----------------------------
	logf("SCENARIO 1: all-barrier batch, one flaky member (3 attempts, linear 20ms backoff)")
	retry := domain.RetryPolicy{MaxAttempts: 3, Backoff: domain.BackoffLinear, BaseDelay: 20 * time.Millisecond}
	g1Spec := domain.GroupSpec{Name: "batch-with-flaky", Admission: domain.AdmissionAll,
		FailurePolicy: domain.FailureContinue, ExpectedMembers: 3, Retry: retry}
	g1, _, err := sched.SubmitGroup(g1Spec, []domain.JobSpec{
		{Name: "a-ok1", Task: domain.TaskSpec{Kind: "ok", Payload: "5ms"}},
		{Name: "a-flaky3", Task: domain.TaskSpec{Kind: "flaky", Payload: "3"}},
		{Name: "a-ok2", Task: domain.TaskSpec{Kind: "ok", Payload: "5ms:done"}},
	})
	must(err)

	// ---- Scenario 2: fail-fast ------------------------------------------
	logf("SCENARIO 2: fail-fast group; failing member must cancel peers")
	g2Spec := domain.GroupSpec{Name: "failfast", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureFailFast, Retry: domain.RetryPolicy{MaxAttempts: 1}}
	g2, _, err := sched.SubmitGroup(g2Spec, []domain.JobSpec{
		{Name: "b-fast-fail", Task: domain.TaskSpec{Kind: "fail", Payload: "boom"}},
		{Name: "b-slow1", Task: domain.TaskSpec{Kind: "sleep", Payload: "600ms"}},
		{Name: "b-slow2", Task: domain.TaskSpec{Kind: "sleep", Payload: "600ms"}},
	})
	must(err)

	// ---- Scenario 3: timeout --------------------------------------------
	logf("SCENARIO 3: job timeout=80ms on a 500ms sleep -> TIMED_OUT (terminal)")
	g3Spec := domain.GroupSpec{Name: "timeout-group", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: domain.RetryPolicy{MaxAttempts: 1}}
	g3, _, err := sched.SubmitGroup(g3Spec, []domain.JobSpec{
		{Name: "c-runaway", Task: domain.TaskSpec{Kind: "sleep", Payload: "500ms"}, Timeout: 80 * time.Millisecond},
	})
	must(err)

	// ---- Scenario 4: API cancel -----------------------------------------
	logf("SCENARIO 4: long job canceled after 60ms -> CANCELED + user_canceled")
	g4Spec := domain.GroupSpec{Name: "cancel-group", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: domain.RetryPolicy{MaxAttempts: 1}}
	_, g4jobs, err := sched.SubmitGroup(g4Spec, []domain.JobSpec{
		{Name: "d-cancel-me", Task: domain.TaskSpec{Kind: "sleep", Payload: "10s"}},
	})
	must(err)
	time.AfterFunc(60*time.Millisecond, func() {
		logf("SCENARIO 4: issuing CancelJob(%s)", g4jobs[0].ID)
		if err := sched.CancelJob(g4jobs[0].ID); err != nil {
			logf("cancel error: %v", err)
		}
	})

	// Wait for 1..4.
	for _, id := range []string{g1.ID, g2.ID, g3.ID} {
		_, err := sched.WaitGroup(ctx, id)
		must(err)
	}
	// g4 is one job; wait on the group too.
	_, _ = sched.WaitGroup(ctx, g4Spec2ID(g4jobs))

	// ---- Scenario 6: in-group dependencies (DAG) ------------------------
	logf("SCENARIO 6a: dependency chain prep -> {work-a, work-b}; dependents run only after prep SUCCEEDED")
	g6Spec := domain.GroupSpec{Name: "dep-chain", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: domain.RetryPolicy{MaxAttempts: 1}}
	g6, _, err := sched.SubmitGroup(g6Spec, []domain.JobSpec{
		{Name: "prep", Task: domain.TaskSpec{Kind: "ok", Payload: "30ms:prepared"}},
		{Name: "work-a", Task: domain.TaskSpec{Kind: "ok", Payload: "10ms:A"}, DependsOn: []string{"prep"}},
		{Name: "work-b", Task: domain.TaskSpec{Kind: "ok", Payload: "10ms:B"}, DependsOn: []string{"prep"}},
	})
	must(err)

	logf("SCENARIO 6b: failing root -> dependents SKIPPED/dependency_failed (distinct from FAILED/TIMED_OUT/CANCELED)")
	g7Spec := domain.GroupSpec{Name: "dep-cascade", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: domain.RetryPolicy{MaxAttempts: 1}}
	g7, _, err := sched.SubmitGroup(g7Spec, []domain.JobSpec{
		{Name: "root", Task: domain.TaskSpec{Kind: "fail", Payload: "prep-broken"}},
		{Name: "mid", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"root"}},
		{Name: "leaf", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"mid"}},
	})
	must(err)

	// ---- Scenario 7: per-group concurrency cap --------------------------
	logf("SCENARIO 7: group MaxParallel=1 serializes its 2 members even with 4 workers")
	g8Spec := domain.GroupSpec{Name: "capped", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, MaxParallel: 1,
		Retry: domain.RetryPolicy{MaxAttempts: 1}}
	g8, _, err := sched.SubmitGroup(g8Spec, []domain.JobSpec{
		{Name: "seq-1", Task: domain.TaskSpec{Kind: "ok", Payload: "40ms:one"}},
		{Name: "seq-2", Task: domain.TaskSpec{Kind: "ok", Payload: "40ms:two"}},
	})
	must(err)

	for _, id := range []string{g6.ID, g7.ID, g8.ID} {
		_, err := sched.WaitGroup(ctx, id)
		must(err)
	}

	// ---- Scenario 5: simulated process restart --------------------------	logf("SCENARIO 5: crash-after-effect + process restart; effect executes exactly once")
	g5Spec := domain.GroupSpec{Name: "restart-recovery", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: domain.RetryPolicy{MaxAttempts: 2}}
	g5, g5jobs, err := sched.SubmitGroup(g5Spec, []domain.JobSpec{
		{Name: "e-crash-then-recover", Task: domain.TaskSpec{Kind: "crash", Payload: "critical-side-effect"}},
	})
	must(err)
	// The crash task persists its side effect, returns ErrSimulatedCrash and
	// the worker dies without reporting completion. Its lease then expires.
	reg.SetCrashHook(func(jobID string, attempt int) {
		logf("  !! side effect durably recorded for %s attempt %d; worker will die without reporting",
			jobID, attempt)
	})
	waitJobState(sched, g5jobs[0].ID, domain.StateRunning, time.Second)
	time.Sleep(cfg.LeaseTimeout + 150*time.Millisecond)
	logf("  shutting down engine (simulated process exit)...")
	shutCtx, cancelShut := context.WithTimeout(ctx, 2*time.Second)
	must(sched.Shutdown(shutCtx))
	cancelShut()
	must(st.Close())

	// Reopen everything: new process, same disk. Recovery must requeue the
	// dead lease and the task ledger must suppress a second body execution.
	st2, err := store.Open(dir, cfg)
	must(err)
	reg2 := task.NewRegistry(dir) // ledger reopened from disk
	sched2 := scheduler.New(cfg, st2, reg2)
	sched2.SetLogger(logf)
	sched2.Start()
	g5final, err := sched2.WaitGroup(ctx, g5.ID)
	must(err)
	logf("SCENARIO 5: after restart group=%s", g5final.State)

	sum := stats.Build(st2)
	fmt.Print(sum.String())

	shut2, cancel2 := context.WithTimeout(ctx, time.Second)
	defer cancel2()
	must(sched2.Shutdown(shut2))
	must(st2.Close())

	// Final stability assertion: the classification must be exactly what the
	// deterministic inputs dictate.
	t, s, f, to, c, k := sum.Totals()
	fmt.Printf("CHECK: total=%d succeeded=%d failed=%d timed_out=%d canceled=%d skipped=%d\n", t, s, f, to, c, k)
	want := [6]int{17, 9, 2, 1, 3, 2}
	got := [6]int{t, s, f, to, c, k}
	if got != want {
		log.Fatalf("NON-DETERMINISTIC RESULT: got %v want %v", got, want)
	}
	fmt.Println("CHECK: deterministic classification OK")
}

func g4Spec2ID(jobs []*domain.Job) string { return jobs[0].GroupID }

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func waitJobState(s *scheduler.Scheduler, jobID string, want domain.JobState, within time.Duration) {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		j, err := s.GetJob(jobID)
		if err == nil && j.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
