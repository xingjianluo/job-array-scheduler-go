package scheduler_test

import (
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// TestRetryBackoffLinear: a flaky job needing 3 attempts succeeds, with the
// linear backoff waits respected and each failed attempt recorded distinctly.
func TestRetryBackoffLinear(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	policy := domain.RetryPolicy{
		MaxAttempts: 3, Backoff: domain.BackoffLinear, BaseDelay: 20 * time.Millisecond,
	}
	spec := domain.GroupSpec{
		Name: "retry", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: policy,
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "flaky", Task: domain.TaskSpec{Kind: "flaky", Payload: "3"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT flaky needs 3 executions; max_attempts=3 linear base=20ms (waits 20ms,40ms)")
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s want SUCCEEDED", gf.State)
	}
	j := e.st.ListJobs(g.ID)[0]
	if j.State != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: job=%s want SUCCEEDED", j.State)
	}
	if len(j.Attempts) != 3 {
		t.Fatalf("JUDGMENT: attempts=%d want 3", len(j.Attempts))
	}
	if j.Attempts[0].State != domain.StateFailed || j.Attempts[1].State != domain.StateFailed {
		t.Fatalf("JUDGMENT: first two attempts should be FAILED, got %s/%s",
			j.Attempts[0].State, j.Attempts[1].State)
	}
	// Verify linear backoff: start2 >= end1 + 20ms, start3 >= end2 + 40ms.
	w1 := j.Attempts[1].StartedAt.Sub(j.Attempts[0].EndedAt)
	w2 := j.Attempts[2].StartedAt.Sub(j.Attempts[1].EndedAt)
	if w1 < 20*time.Millisecond || w2 < 40*time.Millisecond {
		t.Fatalf("JUDGMENT: backoff waits %s/%s smaller than linear 20ms/40ms", w1, w2)
	}
	t.Logf("JUDGMENT: SUCCEEDED on attempt 3; waits measured %s and %s satisfy linear policy",
		w1.Round(time.Millisecond), w2.Round(time.Millisecond))
}

// TestRetryExhausted: a task that always fails ends FAILED+task_failed after
// exactly MaxAttempts attempts, with no further re-execution.
func TestRetryExhausted(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "exhaust", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue,
		Retry:         domain.RetryPolicy{MaxAttempts: 2, Backoff: domain.BackoffFixed, BaseDelay: 5 * time.Millisecond},
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "always-fail", Task: domain.TaskSpec{Kind: "fail", Payload: "nope"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT always-failing task max_attempts=2 fixed 5ms")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupFailed {
		t.Fatalf("JUDGMENT: group=%s want FAILED", gf.State)
	}
	j := e.st.ListJobs(g.ID)[0]
	if j.State != domain.StateFailed || j.Reason != domain.ReasonTaskFailed {
		t.Fatalf("JUDGMENT: state=%s reason=%s want FAILED/task_failed", j.State, j.Reason)
	}
	if len(j.Attempts) != 2 {
		t.Fatalf("JUDGMENT: attempts=%d want exactly 2 (no extra execution)", len(j.Attempts))
	}
	t.Logf("JUDGMENT: job FAILED reason=task_failed after exactly 2 attempts; group FAILED")
}

// TestGroupContinuePartialFailure: under "continue" a failing member does NOT
// cancel peers; the group has mixed results and still SUCCEEDS (>=1 success).
func TestGroupContinuePartialFailure(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "partial", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "ok1", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
		{Name: "bad", Task: domain.TaskSpec{Kind: "fail", Payload: "boom"}},
		{Name: "ok2", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT continue-policy group: 2 ok + 1 failing member")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s want SUCCEEDED (>=1 success under continue)", gf.State)
	}
	states := map[string]domain.JobState{}
	reasons := map[string]string{}
	for _, j := range e.st.ListJobs(g.ID) {
		states[j.Name] = j.State
		reasons[j.Name] = j.Reason
	}
	if states["ok1"] != domain.StateSucceeded || states["ok2"] != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: peers were affected: %+v", states)
	}
	if states["bad"] != domain.StateFailed || reasons["bad"] != domain.ReasonTaskFailed {
		t.Fatalf("JUDGMENT: bad member state=%s reason=%s want FAILED/task_failed", states["bad"], reasons["bad"])
	}
	t.Logf("JUDGMENT: peers SUCCEEDED untouched; bad member FAILED/task_failed; group SUCCEEDED")
}

// TestGroupFailFastPartialFailure: one failing member cancels the running
// peers (CANCELED+group_failfast, distinguishable from user cancel) and the
// group settles FAILED deterministically.
func TestGroupFailFastPartialFailure(t *testing.T) {
	e := newEnv(t, 6)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "ff", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureFailFast, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "fast-bad", Task: domain.TaskSpec{Kind: "fail", Payload: "boom"}},
		{Name: "slow1", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
		{Name: "slow2", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
		{Name: "slow3", Task: domain.TaskSpec{Kind: "sleep", Payload: "1s"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT fail-fast group: 1 immediate failure + 3 long-running peers")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupFailed {
		t.Fatalf("JUDGMENT: group=%s want FAILED", gf.State)
	}
	var failed, canceledFF int
	for _, j := range e.st.ListJobs(g.ID) {
		switch {
		case j.State == domain.StateFailed && j.Reason == domain.ReasonTaskFailed:
			failed++
		case j.State == domain.StateCanceled && j.Reason == domain.ReasonGroupFailFast:
			canceledFF++
		default:
			t.Fatalf("JUDGMENT: unexpected %s state=%s reason=%s", j.Name, j.State, j.Reason)
		}
	}
	if failed != 1 || canceledFF != 3 {
		t.Fatalf("JUDGMENT: want 1 task_failed + 3 group_failfast, got %d/%d", failed, canceledFF)
	}
	t.Logf("JUDGMENT: 1 FAILED/task_failed + 3 CANCELED/group_failfast; group FAILED (reasons distinct)")
}
