package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// TestLedgerIdempotentSuccess: a successful attempt replayed with the same
// (job, attempt) returns the cached output and does not enter the body again.
func TestLedgerIdempotentSuccess(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	ctx := context.Background()
	spec := domain.TaskSpec{Kind: "ok", Payload: "1ms:first"}

	res1, err := r.Run(ctx, "jobA", 1, spec)
	if err != nil || res1.Output != "first" {
		t.Fatalf("first run: res=%v err=%v", res1, err)
	}
	if runs := r.BodyRuns("jobA", 1); runs != 1 {
		t.Fatalf("body runs after first call=%d want 1", runs)
	}

	// Second call with same key, even with a DIFFERENT payload: the ledger
	// wins, proving the body was not entered.
	res2, err := r.Run(ctx, "jobA", 1, domain.TaskSpec{Kind: "fail", Payload: "should-not-run"})
	if err != nil || res2.Output != "first" {
		t.Fatalf("replay: res=%v err=%v want cached first", res2, err)
	}
	if runs := r.BodyRuns("jobA", 1); runs != 1 {
		t.Fatalf("JUDGMENT: body runs=%d after replay want still 1 (no double execution)", runs)
	}
	t.Logf("JUDGMENT: same-attempt replay returned cached output=%q, body runs=1", res2.Output)
}

// TestLedgerFailureReplay: a recorded failure replays as the same failure;
// a different attempt number executes the body normally again (retry).
func TestLedgerFailureReplay(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	ctx := context.Background()
	spec := domain.TaskSpec{Kind: "fail", Payload: "boom"}

	_, err := r.Run(ctx, "jobB", 1, spec)
	if err == nil {
		t.Fatal("first run should fail")
	}
	_, err = r.Run(ctx, "jobB", 1, spec)
	if err == nil || err.Error() != "replayed recorded failure: boom" {
		t.Fatalf("JUDGMENT: replay err=%v want replayed recorded failure: boom", err)
	}
	if runs := r.BodyRuns("jobB", 1); runs != 1 {
		t.Fatalf("JUDGMENT: failed body ran %d times want 1", runs)
	}

	// A fresh attempt number DOES run (this is a legitimate retry).
	_, err = r.Run(ctx, "jobB", 2, spec)
	if err == nil {
		t.Fatal("attempt 2 should execute and fail again")
	}
	if runs := r.BodyRuns("jobB", 2); runs != 1 {
		t.Fatalf("JUDGMENT: retry body runs=%d want 1", runs)
	}
	t.Logf("JUDGMENT: failure replay stable; new attempt executes body exactly once")
}

// TestLedgerSurvivesReopen: entries written by one Registry instance are
// observed by a new instance over the same directory (process-restart model).
func TestLedgerSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	r1 := NewRegistry(dir)
	res, err := r1.Run(context.Background(), "jobC", 1, domain.TaskSpec{Kind: "ok", Payload: "durable"})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	_ = res

	r2 := NewRegistry(dir)
	res2, err := r2.Run(context.Background(), "jobC", 1,
		domain.TaskSpec{Kind: "fail", Payload: "never"})
	if err != nil || res2.Output != "durable" {
		t.Fatalf("JUDGMENT: after reopen res=%v err=%v want cached durable", res2, err)
	}
	if runs := r2.BodyRuns("jobC", 1); runs != 0 {
		t.Fatalf("JUDGMENT: reopened process ran body %d times want 0", runs)
	}
	t.Logf("JUDGMENT: ledger survived reopen; replay output=%q with zero body runs in new process", res2.Output)
}

// TestCrashTaskAndReplay: the crash task commits the effect then returns
// ErrSimulatedCrash; the next attempt for the same job resolves from the
// ledger without executing the body, even though the attempt number differs.
func TestCrashTaskAndReplay(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry(dir)
	r.SetCrashHook(func(jobID string, attempt int) {
		t.Logf("hook: effect committed for %s attempt %d", jobID, attempt)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := r.Run(ctx, "jobD", 1, domain.TaskSpec{Kind: "crash", Payload: "EFFECT"})
	if !errors.Is(err, ErrSimulatedCrash) {
		t.Fatalf("JUDGMENT: crash run err=%v want ErrSimulatedCrash", err)
	}

	// Attempt 2 (after recovery): must return the committed effect, not crash.
	res, err := r.Run(ctx, "jobD", 2, domain.TaskSpec{Kind: "crash", Payload: "EFFECT"})
	if err != nil || res.Output != "EFFECT" {
		t.Fatalf("JUDGMENT: replay after crash res=%v err=%v want EFFECT/nil", res, err)
	}
	if runs := r.BodyRuns("jobD", 2); runs != 0 {
		t.Fatalf("JUDGMENT: replay attempt body runs=%d want 0", runs)
	}
	if runs := r.BodyRuns("jobD", 1); runs != 1 {
		t.Fatalf("JUDGMENT: original attempt body runs=%d want 1", runs)
	}
	t.Logf("JUDGMENT: crash effect committed once; replay attempt served from ledger (body runs=0)")
}

// TestRetryDelayPure checks the backoff arithmetic used by the retry policy.
func TestRetryDelayPure(t *testing.T) {
	r := domain.RetryPolicy{MaxAttempts: 4, Backoff: domain.BackoffExponential, BaseDelay: 10 * time.Millisecond, MaxDelay: 30 * time.Millisecond}
	cases := map[int]time.Duration{1: 10 * time.Millisecond, 2: 20 * time.Millisecond, 3: 30 * time.Millisecond, 4: 30 * time.Millisecond}
	for failed, want := range cases {
		if got := r.Delay(failed); got != want {
			t.Fatalf("Delay(%d)=%s want %s", failed, got, want)
		}
	}
	l := domain.RetryPolicy{Backoff: domain.BackoffLinear, BaseDelay: 10 * time.Millisecond}
	if got := l.Delay(3); got != 30*time.Millisecond {
		t.Fatalf("linear Delay(3)=%s want 30ms", got)
	}
	t.Logf("JUDGMENT: exponential capped at MaxDelay and linear delays match the policy table")
}
