package scheduler_test

import (
	"testing"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/scheduler"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
	"github.com/xingjianluo/job-array-scheduler-go/internal/task"
)

// testEnv bundles an engine built on a fresh temp directory. Every test logs
// the essential inputs (policies, counts, timeouts) and the judgment basis
// (final states, reasons, event sequence) so failures are explainable.
type testEnv struct {
	t       *testing.T
	dir     string
	cfg     domain.Config
	st      *store.FileStore
	reg     *task.Registry
	sched   *scheduler.Scheduler
	started bool
}

func newEnv(t *testing.T, workers int) *testEnv {
	t.Helper()
	dir := t.TempDir()
	cfg := domain.DefaultConfig()
	cfg.DataDir = dir
	if workers > 0 {
		cfg.NumWorkers = workers
		cfg.MaxParallel = workers
	}
	// Fast, stable local timing.
	cfg.HeartbeatInterval = 10 * 1e6 // 10ms
	cfg.LeaseTimeout = 150 * 1e6     // 150ms
	cfg.PollInterval = 2 * 1e6       // 2ms
	cfg.DefaultJobTimeout = 2 * 1e9

	st, err := store.Open(dir, cfg)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	reg := task.NewRegistry(dir)
	sched := scheduler.New(cfg, st, reg)
	logger := func(format string, args ...any) {
		t.Logf(format, args...)
	}
	sched.SetLogger(logger)
	e := &testEnv{t: t, dir: dir, cfg: cfg, st: st, reg: reg, sched: sched}
	return e
}

func (e *testEnv) start() {
	e.t.Helper()
	e.sched.Start()
	e.started = true
}

func (e *testEnv) close() {
	e.t.Helper()
	if e.started {
		_ = e.sched.Shutdown(shutdownCtx(e.t))
	}
	_ = e.st.Close()
}

// noRetry / nRetry are small retry-policy builders for readable test inputs.
func noRetry() domain.RetryPolicy {
	return domain.RetryPolicy{MaxAttempts: 1}
}

func nRetry(n int, delayNs int64) domain.RetryPolicy {
	return domain.RetryPolicy{
		MaxAttempts: n,
		Backoff:     domain.BackoffFixed,
		BaseDelay:   nsDur(delayNs),
	}
}
