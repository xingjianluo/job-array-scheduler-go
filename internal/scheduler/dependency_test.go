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

// jobsByName indexes a group's jobs by member name for readable assertions.
func jobsByName(t *testing.T, e *testEnv, groupID string) map[string]*domain.Job {
	t.Helper()
	out := map[string]*domain.Job{}
	for _, j := range e.st.ListJobs(groupID) {
		out[j.Name] = j
	}
	return out
}

// eventSeq returns the sequence number of the first event of typ for jobID,
// or -1 when absent.
func eventSeq(events []domain.Event, typ, jobID string) int64 {
	for _, ev := range events {
		if ev.Type == typ && ev.JobID == jobID {
			return ev.Seq
		}
	}
	return -1
}

// TestDependencyChainOrder: prep -> {work-a, work-b}. Dependents are born
// BLOCKED, are never claimed before prep SUCCEEDED, and run exactly once
// after it.
func TestDependencyChainOrder(t *testing.T) {
	e := newEnv(t, 3)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "dep-chain", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, jobs, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "prep", Task: domain.TaskSpec{Kind: "ok", Payload: "30ms:prepared"}},
		{Name: "work-a", Task: domain.TaskSpec{Kind: "ok", Payload: "5ms:A"}, DependsOn: []string{"prep"}},
		{Name: "work-b", Task: domain.TaskSpec{Kind: "ok", Payload: "5ms:B"}, DependsOn: []string{"prep"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT chain prep->work-a, prep->work-b (jobs %s,%s,%s); 3 workers, no retry",
		jobs[0].ID, jobs[1].ID, jobs[2].ID)

	// Right after submission the dependents must be BLOCKED, not READY.
	byName := jobsByName(t, e, g.ID)
	for _, n := range []string{"work-a", "work-b"} {
		if byName[n].State != domain.StateBlocked {
			t.Fatalf("JUDGMENT: %s state=%s right after submit, want BLOCKED", n, byName[n].State)
		}
	}
	t.Logf("JUDGMENT: work-a/work-b are BLOCKED while prep has not succeeded")

	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s want SUCCEEDED", gf.State)
	}
	byName = jobsByName(t, e, g.ID)
	for n, j := range byName {
		if j.State != domain.StateSucceeded {
			t.Fatalf("JUDGMENT: %s state=%s want SUCCEEDED", n, j.State)
		}
		if len(j.Attempts) != 1 {
			t.Fatalf("JUDGMENT: %s attempts=%d want exactly 1", n, len(j.Attempts))
		}
	}

	// Ordering proof from the durable event trace: every dependent's claim
	// is sequenced strictly after prep's success.
	events := e.st.Events()
	prepOK := eventSeq(events, domain.EvJobSucceeded, byName["prep"].ID)
	if prepOK < 0 {
		t.Fatalf("JUDGMENT: no job_succeeded event for prep")
	}
	for _, n := range []string{"work-a", "work-b"} {
		claim := eventSeq(events, domain.EvJobClaimed, byName[n].ID)
		blocked := eventSeq(events, domain.EvJobBlocked, byName[n].ID)
		if blocked < 0 {
			t.Fatalf("JUDGMENT: %s never emitted job_blocked", n)
		}
		if claim <= prepOK {
			t.Fatalf("JUDGMENT: %s claimed at seq=%d but prep succeeded at seq=%d", n, claim, prepOK)
		}
		t.Logf("JUDGMENT: %s blocked(seq=%d) -> claimed(seq=%d) only after prep succeeded(seq=%d)",
			n, blocked, claim, prepOK)
	}
}

// TestDependencyRejectedInvalid: unknown references, self-dependencies,
// duplicate names and cycles are all rejected up front, and a rejected batch
// leaves no group and no member behind.
func TestDependencyRejectedInvalid(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	cases := []struct {
		name string
		jobs []domain.JobSpec
		want string
	}{
		{"unknown-dep", []domain.JobSpec{
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"ghost"}},
		}, "unknown member"},
		{"self-dep", []domain.JobSpec{
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"a"}},
		}, "depends on itself"},
		{"dup-name", []domain.JobSpec{
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}},
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"a"}},
		}, "duplicate member name"},
		{"cycle-2", []domain.JobSpec{
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"b"}},
			{Name: "b", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"a"}},
		}, "cycle"},
		{"cycle-3", []domain.JobSpec{
			{Name: "a", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"c"}},
			{Name: "b", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"a"}},
			{Name: "c", Task: domain.TaskSpec{Kind: "ok"}, DependsOn: []string{"b"}},
		}, "cycle"},
	}
	spec := domain.GroupSpec{
		Name: "bad-deps", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	for _, tc := range cases {
		_, _, err := e.sched.SubmitGroup(spec, tc.jobs)
		if err == nil {
			t.Fatalf("JUDGMENT: case %q accepted, want rejection (%s)", tc.name, tc.want)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("JUDGMENT: case %q error=%q want substring %q", tc.name, err, tc.want)
		}
		t.Logf("JUDGMENT: case %-12s rejected: %v", tc.name, err)
	}

	// Nothing may be left behind: no groups, no jobs, no stuck members.
	if gs := e.st.ListGroups(); len(gs) != 0 {
		t.Fatalf("JUDGMENT: %d groups left after rejected submissions, want 0", len(gs))
	}
	t.Logf("JUDGMENT: rejected batches left no group and no member behind")
}

// TestDependencyCascadeSkip: a failing root SKIPs its whole downstream
// chain (mid, leaf) with reason dependency_failed — classified apart from
// FAILED / TIMED_OUT / CANCELED — and the group result is stable.
func TestDependencyCascadeSkip(t *testing.T) {
	e := newEnv(t, 3)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "dep-cascade", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "root", Task: domain.TaskSpec{Kind: "fail", Payload: "prep-broken"}},
		{Name: "mid", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"root"}},
		{Name: "leaf", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"mid"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT root(fail,no-retry) -> mid -> leaf; expect root FAILED, mid+leaf SKIPPED/dependency_failed")

	gf, err := e.sched.WaitGroup(shutdownCtx(t), g.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gf.State != domain.GroupFailed {
		t.Fatalf("JUDGMENT: group=%s want FAILED (root failed)", gf.State)
	}
	byName := jobsByName(t, e, g.ID)
	if byName["root"].State != domain.StateFailed || byName["root"].Reason != domain.ReasonTaskFailed {
		t.Fatalf("JUDGMENT: root=%s/%s want FAILED/task_failed", byName["root"].State, byName["root"].Reason)
	}
	for _, n := range []string{"mid", "leaf"} {
		j := byName[n]
		if j.State != domain.StateSkipped || j.Reason != domain.ReasonDependencyFailed {
			t.Fatalf("JUDGMENT: %s=%s/%s want SKIPPED/dependency_failed", n, j.State, j.Reason)
		}
		if len(j.Attempts) != 0 {
			t.Fatalf("JUDGMENT: %s ran %d attempts; a skipped job must never run", n, len(j.Attempts))
		}
		t.Logf("JUDGMENT: %s SKIPPED (%s), 0 attempts: %s", n, j.Reason, j.Message)
	}

	// The cascade is visible in the durable trace as job_skipped events.
	events := e.st.Events()
	if eventSeq(events, domain.EvJobSkipped, byName["mid"].ID) < 0 ||
		eventSeq(events, domain.EvJobSkipped, byName["leaf"].ID) < 0 {
		t.Fatalf("JUDGMENT: job_skipped events missing for mid/leaf")
	}

	// Classification: skipped is its own column, distinct from
	// failed/timed_out/canceled, and stable across rebuilds.
	s1 := stats.Build(e.st)
	s2 := stats.Build(e.st)
	if s1.String() != s2.String() {
		t.Fatalf("JUDGMENT: stats not stable across rebuilds")
	}
	gs := s1.Groups[0]
	if gs.Failed != 1 || gs.Skipped != 2 || gs.Canceled != 0 || gs.TimedOut != 0 {
		t.Fatalf("JUDGMENT: classification failed=%d skipped=%d canceled=%d timed_out=%d want 1/2/0/0",
			gs.Failed, gs.Skipped, gs.Canceled, gs.TimedOut)
	}
	if gs.ByReason[domain.ReasonDependencyFailed] != 2 {
		t.Fatalf("JUDGMENT: ByReason[dependency_failed]=%d want 2", gs.ByReason[domain.ReasonDependencyFailed])
	}
	t.Logf("JUDGMENT: classification failed=1 skipped=2 (dependency_failed), distinct from canceled/timed_out; stable across rebuilds")
}

// TestDependencySkipOnTimeoutAndCancel: a dependency that TIMES OUT or is
// CANCELED also converges its dependents to SKIPPED/dependency_failed —
// they never wait forever.
func TestDependencySkipOnTimeoutAndCancel(t *testing.T) {
	e := newEnv(t, 2)
	defer e.close()
	e.start()

	spec := domain.GroupSpec{
		Name: "dep-timeout", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g1, _, err := e.sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "slow", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}, Timeout: 60 * time.Millisecond},
		{Name: "after", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"slow"}},
	})
	if err != nil {
		t.Fatalf("submit g1: %v", err)
	}
	t.Logf("INPUT g1: slow(timeout=60ms, sleeps 5s) -> after; expect slow TIMED_OUT, after SKIPPED")

	g2spec := spec
	g2spec.Name = "dep-cancel"
	g2, g2jobs, err := e.sched.SubmitGroup(g2spec, []domain.JobSpec{
		{Name: "victim", Task: domain.TaskSpec{Kind: "sleep", Payload: "5s"}},
		{Name: "after", Task: domain.TaskSpec{Kind: "ok", Payload: "1ms"}, DependsOn: []string{"victim"}},
	})
	if err != nil {
		t.Fatalf("submit g2: %v", err)
	}
	t.Logf("INPUT g2: victim(sleep 5s, canceled via API) -> after; expect victim CANCELED, after SKIPPED")
	if err := e.sched.CancelJob(g2jobs[0].ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if _, err := e.sched.WaitGroup(shutdownCtx(t), g1.ID); err != nil {
		t.Fatalf("wait g1: %v", err)
	}
	if _, err := e.sched.WaitGroup(shutdownCtx(t), g2.ID); err != nil {
		t.Fatalf("wait g2: %v", err)
	}

	b1 := jobsByName(t, e, g1.ID)
	if b1["slow"].State != domain.StateTimedOut {
		t.Fatalf("JUDGMENT: g1 slow=%s want TIMED_OUT", b1["slow"].State)
	}
	if b1["after"].State != domain.StateSkipped || b1["after"].Reason != domain.ReasonDependencyFailed {
		t.Fatalf("JUDGMENT: g1 after=%s/%s want SKIPPED/dependency_failed", b1["after"].State, b1["after"].Reason)
	}
	t.Logf("JUDGMENT: timeout upstream -> dependent SKIPPED/dependency_failed (%s)", b1["after"].Message)

	b2 := jobsByName(t, e, g2.ID)
	if b2["victim"].State != domain.StateCanceled {
		t.Fatalf("JUDGMENT: g2 victim=%s want CANCELED", b2["victim"].State)
	}
	if b2["after"].State != domain.StateSkipped || b2["after"].Reason != domain.ReasonDependencyFailed {
		t.Fatalf("JUDGMENT: g2 after=%s/%s want SKIPPED/dependency_failed", b2["after"].State, b2["after"].Reason)
	}
	t.Logf("JUDGMENT: canceled upstream -> dependent SKIPPED/dependency_failed (%s)", b2["after"].Message)
}

// TestGroupMaxParallel: a group-level concurrency cap limits how many of its
// members run at once (stricter than the global cap), while another
// uncapped group still gets global slots — no starvation.
func TestGroupMaxParallel(t *testing.T) {
	e := newEnv(t, 4) // 4 workers, global MaxParallel=4
	defer e.close()
	e.start()

	capped := domain.GroupSpec{
		Name: "capped", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, MaxParallel: 2,
		Retry: noRetry(),
	}
	var cappedJobs []domain.JobSpec
	for i := 0; i < 5; i++ {
		cappedJobs = append(cappedJobs, domain.JobSpec{
			Name:    "capped-" + string(rune('a'+i)),
			Task:    domain.TaskSpec{Kind: "sleep", Payload: "120ms"},
			Timeout: time.Second,
		})
	}
	g1, _, err := e.sched.SubmitGroup(capped, cappedJobs)
	if err != nil {
		t.Fatalf("submit capped: %v", err)
	}
	other := domain.GroupSpec{
		Name: "other", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: noRetry(),
	}
	g2, _, err := e.sched.SubmitGroup(other, []domain.JobSpec{
		{Name: "free-1", Task: domain.TaskSpec{Kind: "ok", Payload: "10ms"}},
		{Name: "free-2", Task: domain.TaskSpec{Kind: "ok", Payload: "10ms"}},
	})
	if err != nil {
		t.Fatalf("submit other: %v", err)
	}
	t.Logf("INPUT capped group: 5x sleep 120ms, MaxParallel=2, global cap=4; other group: 2 quick jobs (starvation check)")

	// Sample the capped group's running count for the whole run.
	done := make(chan struct{})
	maxRunning := 0
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			n := 0
			for _, j := range e.st.ListJobs(g1.ID) {
				if j.State == domain.StateRunning {
					n++
				}
			}
			if n > maxRunning {
				maxRunning = n
			}
			time.Sleep(time.Millisecond)
		}
	}()

	if _, err := e.sched.WaitGroup(shutdownCtx(t), g1.ID); err != nil {
		t.Fatalf("wait capped: %v", err)
	}
	close(done)
	g2f, err := e.sched.WaitGroup(shutdownCtx(t), g2.ID)
	if err != nil {
		t.Fatalf("wait other: %v", err)
	}

	if maxRunning > 2 {
		t.Fatalf("JUDGMENT: capped group ran %d members concurrently, want <= 2", maxRunning)
	}
	if maxRunning == 0 {
		t.Fatalf("JUDGMENT: never observed the capped group running")
	}
	if g2f.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: uncapped group=%s want SUCCEEDED (must not starve)", g2f.State)
	}
	t.Logf("JUDGMENT: capped group peak concurrency=%d (cap=2, global=4); uncapped group finished %s — no starvation",
		maxRunning, g2f.State)
}

// TestDependencySurvivesRestart: a dependency chain whose upstream member
// crashes (worker dies after the durable side effect) is recovered by a
// restarted engine: the lease is reaped, the upstream job completes from the
// ledger, and only then is the dependent unblocked. Dependency state
// survives the restart intact.
func TestDependencySurvivesRestart(t *testing.T) {
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
		Name: "dep-restart", Admission: domain.AdmissionImmediate,
		FailurePolicy: domain.FailureContinue, Retry: nRetry(3, 0),
	}
	g, jobs, err := sched.SubmitGroup(spec, []domain.JobSpec{
		{Name: "critical", Task: domain.TaskSpec{Kind: "crash", Payload: "EFFECT-7"}},
		{Name: "downstream", Task: domain.TaskSpec{Kind: "ok", Payload: "5ms:after"}, DependsOn: []string{"critical"}},
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("INPUT critical(crash after effect, retry=3) -> downstream; restart whole engine while lease held")

	env := &testEnv{t: t, cfg: cfg, st: st, sched: sched}
	waitFor(jobs[0].ID, domain.StateRunning, env, time.Second)
	// The dependent must be BLOCKED and must stay so across the restart.
	if j, _ := sched.GetJob(jobs[1].ID); j.State != domain.StateBlocked {
		t.Fatalf("JUDGMENT: downstream=%s before restart, want BLOCKED", j.State)
	}
	time.Sleep(30 * time.Millisecond) // lease still held on disk
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	if err := sched.Shutdown(ctx1); err != nil {
		t.Fatalf("shutdown1: %v", err)
	}
	cancel1()
	_ = st.Close()

	// ---- Simulated process restart over the same durable state. ----
	st2, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatalf("open2: %v", err)
	}
	reg2 := task.NewRegistry(dir)
	sched2 := scheduler.New(cfg, st2, reg2)
	sched2.SetLogger(func(f string, a ...any) { t.Logf("(restarted) "+f, a...) })

	// Dependency state survived the restart: downstream is still BLOCKED.
	if j, _ := st2.GetJob(jobs[1].ID); j.State != domain.StateBlocked {
		t.Fatalf("JUDGMENT: downstream=%s after reopen, want BLOCKED (dep state lost)", j.State)
	}
	if j, _ := st2.GetJob(jobs[1].ID); len(j.DependsOn) != 1 || j.DependsOn[0] != jobs[0].ID {
		t.Fatalf("JUDGMENT: downstream DependsOn=%v after reopen, want [%s]", j.DependsOn, jobs[0].ID)
	}
	t.Logf("JUDGMENT: after reopen downstream still BLOCKED with durable DependsOn=%v", jobs[0].ID)

	sched2.Start()
	gf, err := sched2.WaitGroup(context.Background(), g.ID)
	if err != nil {
		t.Fatalf("wait after restart: %v", err)
	}
	if gf.State != domain.GroupSucceeded {
		t.Fatalf("JUDGMENT: group=%s after restart, want SUCCEEDED", gf.State)
	}
	up, _ := sched2.GetJob(jobs[0].ID)
	down, _ := sched2.GetJob(jobs[1].ID)
	if up.State != domain.StateSucceeded || down.State != domain.StateSucceeded {
		t.Fatalf("JUDGMENT: upstream=%s downstream=%s, want both SUCCEEDED", up.State, down.State)
	}
	if runs := reg2.BodyRuns(jobs[0].ID, 2); runs != 0 {
		t.Fatalf("JUDGMENT: restarted upstream attempt body-runs=%d, want 0 (ledger replay)", runs)
	}

	// The dependent was claimed only after the upstream success, even across
	// the restart.
	events := st2.Events()
	upOK := eventSeq(events, domain.EvJobSucceeded, jobs[0].ID)
	downClaim := eventSeq(events, domain.EvJobClaimed, jobs[1].ID)
	if upOK < 0 || downClaim <= upOK {
		t.Fatalf("JUDGMENT: downstream claimed seq=%d, upstream succeeded seq=%d; order violated", downClaim, upOK)
	}
	t.Logf("JUDGMENT: restart recovered upstream (attempt served from ledger), downstream unblocked and claimed(seq=%d) after upstream success(seq=%d)",
		downClaim, upOK)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	if err := sched2.Shutdown(ctx2); err != nil {
		t.Fatalf("shutdown2: %v", err)
	}
	_ = st2.Close()
}
