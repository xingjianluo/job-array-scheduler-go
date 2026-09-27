// Package executor runs claimed jobs. One Executor simulates one cluster
// worker: it polls the store for READY jobs, leases them with a fencing
// token, keeps the lease alive with heartbeats, runs the task under a
// timeout- and cancel-aware context, and reports exactly one fenced outcome.
//
// At-most-once execution is enforced at two layers:
//
//  1. the store hands out one monotonic token per lease and rejects every
//     heartbeat/completion that does not match the current lease (a zombie
//     worker after lease expiry is fenced off);
//  2. the task registry returns the durably recorded outcome when the same
//     (job, attempt) is replayed after a crash, so the task body itself is
//     never executed twice.
package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
	"github.com/xingjianluo/job-array-scheduler-go/internal/task"
)

// Executor is a single worker loop.
type Executor struct {
	cfg      domain.Config
	st       store.Store
	reg      *task.Registry
	workerID string

	wg      sync.WaitGroup
	stopCh  chan struct{}
	stopOne sync.Once
	dead    atomic.Bool
	logger  func(format string, args ...any)
}

// New builds a worker with the given workerID.
func New(cfg domain.Config, st store.Store, reg *task.Registry, workerID string) *Executor {
	return &Executor{
		cfg:      cfg,
		st:       st,
		reg:      reg,
		workerID: workerID,
		stopCh:   make(chan struct{}),
		logger:   func(string, ...any) {},
	}
}

// SetLogger installs a printf-style trace logger (tests/demo use it to print
// the decisive inputs and judgment basis).
func (e *Executor) SetLogger(f func(format string, args ...any)) { e.logger = f }

// Start launches the worker loop.
func (e *Executor) Start() {
	e.wg.Add(1)
	go e.loop()
}

// Stop signals the loop to exit and waits for it. An in-flight task is not
// interrupted: it finishes (honoring its own timeout/cancel) and its result
// is committed before the worker exits — stop means "claim nothing new".
func (e *Executor) Stop() {
	e.stopOne.Do(func() { close(e.stopCh) })
	e.wg.Wait()
}

func (e *Executor) stopRequested() bool {
	select {
	case <-e.stopCh:
		return true
	default:
		return false
	}
}

func (e *Executor) loop() {
	defer e.wg.Done()
	for {
		if e.stopRequested() || e.dead.Load() {
			return
		}
		job, attempt, err := e.st.ClaimNext(e.workerID, time.Now())
		if err != nil {
			// Claim errors are not expected in local operation; back off and
			// retry rather than killing the worker.
			e.logger("[%s] claim error: %v", e.workerID, err)
			if !e.sleep(e.cfg.PollInterval) {
				return
			}
			continue
		}
		if job == nil {
			if !e.sleep(e.cfg.PollInterval) {
				return
			}
			continue
		}
		e.runOne(job, attempt)
		if e.dead.Load() {
			return
		}
	}
}

// sleep waits for d or until stop; returns false when the worker must exit.
func (e *Executor) sleep(d time.Duration) bool {
	if d <= 0 {
		return !e.stopRequested()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-e.stopCh:
		return false
	}
}

// runOne executes one leased attempt end to end. Any exit path produces
// exactly one FinishAttempt call carrying the lease token; the store decides
// whether it is still valid.
func (e *Executor) runOne(job *domain.Job, attempt *domain.Attempt) {
	token := attempt.Token
	timeout := job.Timeout
	if timeout <= 0 {
		timeout = e.cfg.DefaultJobTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Cooperative cancellation: poll the durable cancel flag.
	cancelPollerDone := make(chan struct{})
	go func() {
		t := time.NewTicker(e.cfg.PollInterval * 2)
		defer t.Stop()
		for {
			select {
			case <-cancelPollerDone:
				return
			case <-t.C:
				if e.st.CancelRequested(job.ID, e.workerID, token) {
					cancel()
					return
				}
			}
		}
	}()

	// Lease renewal. The heartbeat loop exits when the task finishes or when
	// the store rejects the lease (it expired and was re-leased): in the
	// latter case the context is canceled so a fenced-away zombie cannot keep
	// doing unbounded work, and its eventual FinishAttempt will be rejected.
	hbDone := make(chan struct{})
	go func() {
		t := time.NewTicker(e.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-hbDone:
				return
			case <-t.C:
				if err := e.st.Heartbeat(job.ID, e.workerID, token); err != nil {
					e.logger("[%s] lease fenced for %s token=%d: %v — aborting attempt",
						e.workerID, job.ID, token, err)
					cancel()
					return
				}
			}
		}
	}()

	e.logger("[%s] claimed %s (name=%q kind=%s attempt=%d/%d token=%d timeout=%s)",
		e.workerID, job.ID, job.Name, job.Task.Kind, attempt.Attempt,
		job.Retry.MaxAttempts, token, timeout)

	res, runErr := e.runGuarded(ctx, job, attempt)
	close(hbDone)
	close(cancelPollerDone)

	// Simulated worker-process death: the side effect is durable but no
	// completion is EVER reported and heartbeats have stopped. The lease will
	// expire and recovery will requeue the job; the task ledger guarantees
	// the body is not executed a second time. This worker is dead for good.
	if errors.Is(runErr, task.ErrSimulatedCrash) {
		e.logger("[%s] SIMULATED CRASH after effect for %s token=%d; worker exits without reporting",
			e.workerID, job.ID, token)
		e.dead.Store(true)
		// Return without FinishAttempt; the loop's deferred wg.Done runs when
		// it observes the dead flag, and Shutdown's Wait then completes.
		return
	}

	out := e.classify(job, token, res, runErr)
	e.logger("[%s] finish  %s token=%d -> %s reason=%s msg=%q",
		e.workerID, job.ID, token, out.State, out.Reason, out.Message)

	if err := e.st.FinishAttempt(job.ID, e.workerID, token, out); err != nil {
		// The most common path: our lease expired and the job was re-leased.
		// Discard our outcome; the newer attempt owns the job.
		if errors.Is(err, domain.ErrConflict) {
			e.logger("[%s] result for %s rejected (stale token=%d): %v",
				e.workerID, job.ID, token, err)
			return
		}
		e.logger("[%s] finish error for %s: %v", e.workerID, job.ID, err)
	}
}

// runGuarded executes the task body and converts a panicking task into a
// classified failure instead of killing the worker goroutine.
func (e *Executor) runGuarded(ctx context.Context, job *domain.Job, attempt *domain.Attempt) (res *task.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{v: r}
		}
	}()
	return e.reg.Run(ctx, job.ID, attempt.Attempt, job.Task)
}

type panicError struct{ v any }

func (e *panicError) Error() string { return "task panicked: " + fmt.Sprint(e.v) }

// classify maps the task result and context state to the four distinguishable
// outcomes. The durable CancelRequested flag is authoritative for telling
// cancellation apart from a timeout when the two race:
//
//	cancel flag set          -> CANCELED (user_canceled / group_failfast
//	                            is recorded on the job by the store path;
//	                            the attempt's own reason here is user_canceled
//	                            and the store keeps the requested reason)
//	deadline exceeded, no cancel flag        -> TIMED_OUT
//	task returned its own error              -> FAILED
//	nil error                                -> SUCCEEDED
func (e *Executor) classify(job *domain.Job, token int64, res *task.Result, runErr error) store.Outcome {
	out := store.Outcome{}
	switch {
	case runErr == nil:
		out.State = domain.StateSucceeded
		if res != nil {
			out.Output = res.Output
		}
		return out

	case errors.Is(runErr, context.Canceled):
		if reason, ok := e.st.CancelReason(job.ID, e.workerID, token); ok {
			out.State = domain.StateCanceled
			out.Reason = reason
			out.Message = "task stopped after cancel request"
			return out
		}
		// Context was canceled by the fencing heartbeat after lease loss.
		// Report as a normal failure; the store will reject the stale token
		// anyway (and if it does not, the retry path requeues safely).
		out.State = domain.StateFailed
		out.Reason = "lease_fenced"
		out.Message = "lease lost while task was running"
		return out

	case errors.Is(runErr, context.DeadlineExceeded):
		if reason, ok := e.st.CancelReason(job.ID, e.workerID, token); ok {
			out.State = domain.StateCanceled
			out.Reason = reason
			out.Message = "canceled while near the timeout deadline"
			return out
		}
		out.State = domain.StateTimedOut
		out.Reason = domain.ReasonTimeout
		out.Message = "task exceeded job timeout"
		return out

	default:
		var pe *panicError
		if errors.As(runErr, &pe) {
			out.State = domain.StateFailed
			out.Reason = domain.ReasonTaskPanic
			out.Message = pe.Error()
			return out
		}
		out.State = domain.StateFailed
		out.Reason = domain.ReasonTaskFailed
		out.Message = runErr.Error()
		return out
	}
}
