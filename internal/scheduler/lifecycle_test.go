package scheduler_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// TestJobTimeout: a sleep longer than its per-job timeout settles TIMED_OUT
// +timeout, does not consume retries (timeout is terminal) and releases the
// worker for other jobs.
func TestJobTimeout(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "timeout", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: nRetry(3, 0),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "runaway", Task: domain.TaskSpec{Kind: "sleep", Payload: "2s"}, Timeout: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT sleep 2s with timeout=50ms, max_attempts=3 (timeout must be terminal)")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupFailed {
		t.Fatalf("JUDGMENT: group=%s want FAILED", gf.State)
	}
	j := e.st.ListJobs(g.ID)[0]
	if j.State != domain.StateTimedOut {
		t.Fatalf("JUDGMENT: state=%s want TIMED_OUT", j.State)
	}
	if j.Reason != domain.ReasonTimeout {
		t.Fatalf("JUDGMENT: reason=%s want timeout", j.Reason)
	}
	if len(j.Attempts) != 1 {
		t.Fatalf("JUDGMENT: attempts=%d want 1 (timeout terminal, no retries)", len(j.Attempts))
	}
	t.Logf("JUDGMENT: job TIMED_OUT reason=timeout, exactly 1 attempt; worker resource released")
}

// TestCancelRunningJob: a running long job canceled via the API finishes
// CANCELED+user_canceled and the lease goroutines stop.
func TestCancelRunningJob(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "cancel1", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	_, jobs, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "long", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	waitFor(jobs[0].ID, domain.StateRunning, e, time.Second)
	t.Logf("INPUT cancel running job %s via API", jobs[0].ID)
	if err := e.sched.CancelJob(jobs[0].ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	waitFor(jobs[0].ID, domain.StateCanceled, e, time.Second)
	j, _ := e.sched.GetJob(jobs[0].ID)
	if j.Reason != domain.ReasonUserCanceled {
		t.Fatalf("JUDGMENT: reason=%s want user_canceled", j.Reason)
	}
	if j.State != domain.StateCanceled {
		t.Fatalf("JUDGMENT: state=%s want CANCELED", j.State)
	}
	t.Logf("JUDGMENT: job CANCELED reason=user_canceled cooperatively; resources released")
}

// TestCancelWholeGroupRunning: cancel a running group; running members get
// CANCELED+user_canceled and the group itself ends CANCELED.
func TestCancelWholeGroupRunning(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "cancelgrp", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, jobs, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "a", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
		{Name: "b", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
		{Name: "c", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	for _, jb := range jobs {
		waitFor(jb.ID, domain.StateRunning, e, time.Second)
	}
	t.Logf("INPUT cancel whole running group %s (3 running members)", g.ID)
	if err := e.sched.CancelGroup(g.ID); err != nil {
		t.Fatalf("cancel group: %v", err)
	}
	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupCanceled {
		t.Fatalf("JUDGMENT: group=%s want CANCELED", gf.State)
	}
	for _, jb := range jobs {
		j, _ := e.sched.GetJob(jb.ID)
		if j.State != domain.StateCanceled || j.Reason != domain.ReasonUserCanceled {
			t.Fatalf("JUDGMENT: %s state=%s reason=%s want CANCELED/user_canceled",
				jb.ID, j.State, j.Reason)
		}
	}
	// Canceling again must conflict rather than silently succeed.
	if err := e.sched.CancelGroup(g.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("JUDGMENT: double group cancel err=%v want ErrConflict", err)
	}
	if err := e.sched.CancelJob(jobs[0].ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("JUDGMENT: cancel of terminal job err=%v want ErrConflict", err)
	}
	t.Logf("JUDGMENT: all members CANCELED/user_canceled; group CANCELED; repeated cancels -> ErrConflict")
}

// TestFailFastTriggeredByTimeout: a timed-out member triggers fail-fast
// cancellation of peers, proving fail-fast reacts to TIMED_OUT too and the
// two reasons (timeout vs group_failfast) stay separable.
func TestFailFastTriggeredByTimeout(t *testing.T) {
	e := newEnv(t, 4)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "ffto", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureFailFast, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "runaway", Task: domain.TaskSpec{Kind: "sleep", Payload: "2s"}, Timeout: 50 * time.Millisecond},
		{Name: "peer", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT fail-fast group where the trigger is a 50ms timeout")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupFailed {
		t.Fatalf("JUDGMENT: group=%s want FAILED", gf.State)
	}
	byName := map[string]*domain.Job{}
	for _, j := range e.st.ListJobs(g.ID) {
		byName[j.Name] = j
	}
	if byName["runaway"].State != domain.StateTimedOut {
		t.Fatalf("JUDGMENT: trigger state=%s want TIMED_OUT", byName["runaway"].State)
	}
	if byName["peer"].State != domain.StateCanceled || byName["peer"].Reason != domain.ReasonGroupFailFast {
		t.Fatalf("JUDGMENT: peer state=%s reason=%s want CANCELED/group_failfast",
			byName["peer"].State, byName["peer"].Reason)
	}
	t.Logf("JUDGMENT: timeout member TIMED_OUT/timeout, peer CANCELED/group_failfast, group FAILED")
}

// TestPanicTaskClassified: a panicking task body does not kill the worker and
// is classified FAILED+task_panic, distinct from task_failed; the worker
// survives to run the next job.
func TestPanicTaskClassified(t *testing.T) {
	e := newEnv(t, 1)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "panicgrp", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "panicker", Task: domain.TaskSpec{Kind: "panic", Payload: "kaboom"}},
		{Name: "after", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT panicking task followed by a healthy job on a single worker")
	gf, _ := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s want SUCCEEDED (second job keeps worker alive)", gf.State)
	}
	byName := map[string]*domain.Job{}
	for _, j := range e.st.ListJobs(g.ID) {
		byName[j.Name] = j
	}
	if byName["panicker"].State != domain.StateFailed || byName["panicker"].Reason != domain.ReasonTaskPanic {
		t.Fatalf("JUDGMENT: panic job state=%s reason=%s want FAILED/task_panic",
			byName["panicker"].State, byName["panicker"].Reason)
	}
	if !strings.Contains(byName["panicker"].Message, "kaboom") {
		t.Fatalf("JUDGMENT: panic message lost: %q", byName["panicker"].Message)
	}
	if byName["after"].State != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: after job=%s want SUCCEEDED (worker survived panic)", byName["after"].State)
	}
	t.Logf("JUDGMENT: panic -> FAILED/task_panic (msg kept), worker survived and ran next job")
}

// waitFor polls until jobID reaches state or fails the test.
func waitFor(jobID string, want domain.JobState, e *testEnv, within time.Duration) {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		j, err := e.sched.GetJob(jobID)
		if err == nil && j.State == want {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	j, _ := e.sched.GetJob(jobID)
	e.t.Fatalf("waitFor: job %s never reached %s (last state=%s)", jobID, want, stateOf(j))
}

func stateOf(j *domain.Job) string {
	if j == nil {
		return "<nil>"
	}
	return string(j.State)
}
