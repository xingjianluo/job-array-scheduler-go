// Package scheduler ties persistence, resources and execution together:
// group-gated submission, lease-based claiming with a stale-lease
// recoverer, retry/backoff, timeout and cancel, and deterministic
// terminal statistics.
package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/executor"
	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// Default tuning values used when Config leaves a field zero.
const (
	defaultSlots           = 4
	defaultLeaseTTL        = 10 * time.Second
	defaultLeaseRenew      = 3 * time.Second
	defaultPollInterval    = 50 * time.Millisecond
	defaultRecoverInterval = 1 * time.Second
)

// Config holds scheduler tuning knobs.
type Config struct {
	// Slots bounds the number of concurrently running jobs.
	Slots int
	// LeaseTTL is how long a claim stays valid without renewal.
	LeaseTTL time.Duration
	// LeaseRenew is the heartbeat interval (must be < LeaseTTL).
	LeaseRenew time.Duration
	// PollInterval is the worker idle sleep when nothing is claimable.
	PollInterval time.Duration
	// RecoverInterval is how often stale leases are reclaimed.
	RecoverInterval time.Duration
	// WorkDir is the working directory for child processes ("" = inherit).
	WorkDir string
	// OwnerPrefix names this process in lease ownership tokens.
	OwnerPrefix string
}

func (c *Config) withDefaults() {
	if c.Slots <= 0 {
		c.Slots = defaultSlots
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = defaultLeaseTTL
	}
	if c.LeaseRenew <= 0 || c.LeaseRenew >= c.LeaseTTL {
		c.LeaseRenew = c.LeaseTTL / 3
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.RecoverInterval <= 0 {
		c.RecoverInterval = defaultRecoverInterval
	}
	if c.OwnerPrefix == "" {
		c.OwnerPrefix = "proc"
	}
}

// SubmitItem is one element of a batch submission.
type SubmitItem struct {
	Name string
	Spec job.Spec
}

// SubmitResult is the identity outcome of a submission.
type SubmitResult struct {
	GroupID string
	JobIDs  []string
}

// Scheduler is the single-process coordinator + worker pool. Multiple
// Scheduler instances may run against the same on-disk store from
// different processes; the store lock plus lease protocol keep claims
// mutually exclusive.
type Scheduler struct {
	st     store.Store
	runner executor.Runner
	cfg    Config
	now    func() time.Time

	pool *slots

	ownerRoot string
	ownerSeq  uint64
	ownerMu   sync.Mutex

	// cancelReqs lets CancelJob/CancelGroup interrupt running attempts.
	reqMu       sync.Mutex
	cancelFuncs map[string]context.CancelFunc // jobID -> attempt cancel

	// runWg tracks in-flight worker goroutines for Close.
	runWg sync.WaitGroup
	// startedCh is closed by Run once all goroutines are registered, so
	// callers can safely wait for shutdown without racing runWg.Add.
	startedCh chan struct{}

	closed   bool
	closedMu sync.Mutex
}

// New builds a Scheduler over the given store and runner.
func New(st store.Store, runner executor.Runner, cfg Config) *Scheduler {
	cfg.withDefaults()
	s := &Scheduler{
		st:          st,
		runner:      runner,
		cfg:         cfg,
		now:         time.Now,
		pool:        newSlots(cfg.Slots),
		ownerRoot:   cfg.OwnerPrefix + "-" + job.NewID("own")[4:],
		cancelFuncs: map[string]context.CancelFunc{},
		startedCh:   make(chan struct{}),
	}
	return s
}

// newOwner mints a unique lease owner token per claim (process-unique
// prefix + monotonic counter).
func (s *Scheduler) newOwner() string {
	s.ownerMu.Lock()
	s.ownerSeq++
	n := s.ownerSeq
	s.ownerMu.Unlock()
	return s.ownerRoot + "#" + itoa(n)
}

// Slots reports the configured concurrency (for logs/tests).
func (s *Scheduler) Slots() int { return s.cfg.Slots }

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// Close stops accepting new work. It does not kill running jobs; call
// Shutdown (or cancel the context passed to Run) for that. It waits for
// worker goroutines to settle and clears registered cancel funcs.
func (s *Scheduler) Close() error {
	s.closedMu.Lock()
	if s.closed {
		s.closedMu.Unlock()
		return nil
	}
	s.closed = true
	s.closedMu.Unlock()
	s.reqMu.Lock()
	for _, cf := range s.cancelFuncs {
		cf()
	}
	s.cancelFuncs = map[string]context.CancelFunc{}
	s.reqMu.Unlock()
	s.runWg.Wait()
	return nil
}

// runHandle groups the run context and its cancel func with a started
// channel for an orderly test/demo shutdown.
type runHandle struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// RunHandle is the exported handle returned by Start.
type RunHandle = runHandle

// Ctx returns the run context.
func (h *runHandle) Ctx() context.Context { return h.ctx }

// Start runs Run under parent in a goroutine and returns a handle whose
// Shutdown cancels the run and waits for all workers to exit.
func (s *Scheduler) Start(parent context.Context) *RunHandle { return s.run(parent) }

// run starts Run in a goroutine and returns a handle whose Shutdown
// cancels the run context and waits for every worker to exit.
func (s *Scheduler) run(parent context.Context) *runHandle {
	ctx, cancel := context.WithCancel(parent)
	go func() { _ = s.Run(ctx) }()
	<-s.startedCh
	return &runHandle{ctx: ctx, cancel: cancel}
}

// Shutdown cancels the run and waits for worker goroutines to stop. It
// does not force terminal states for in-flight jobs: their leases expire
// and are requeued by the next live process.
func (h *runHandle) Shutdown(s *Scheduler) {
	h.cancel()
	s.runWg.Wait()
}
