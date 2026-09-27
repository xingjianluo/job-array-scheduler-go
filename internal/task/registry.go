// Package task implements the local, process-based simulated workloads and
// the "side-effect ledger" that makes at-most-once execution verifiable.
//
// Simulated task kinds (domain.TaskSpec):
//
//	kind "ok"    payload: ["10ms"][":output text"]  succeed after a delay
//	kind "fail"  payload: ["10ms|"]error message   fail with that message
//	kind "sleep" payload: "100ms"                  sleep then succeed
//	kind "flaky" payload: "N"                      fail attempts 1..N-1,
//	                                               succeed on attempt N
//	kind "crash" payload: "ok" (as "ok")           record the side effect
//	                                               durably then invoke the
//	                                               crash hook instead of
//	                                               reporting back
//
// Every FINISHED attempt is recorded in a JSON ledger keyed by "job#attempt".
// If the same attempt is run again (the typical case after a worker crash:
// the lease expired and the store requeued the job, while the task body had
// already produced its effect), the recorded outcome is returned without
// executing the body again. The flaky counter is derived from ledger entries
// too, so a crash-replayed failed attempt does not consume an extra failure.
package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// Outcome classification stored in the ledger.
type outcomeKind string

const (
	outcomeSuccess outcomeKind = "SUCCESS"
	outcomeFailure outcomeKind = "FAILURE"
)

// ledgerEntry is the durable record of one finished attempt.
type ledgerEntry struct {
	Kind   outcomeKind `json:"kind"`
	Output string      `json:"output,omitempty"`
	Error  string      `json:"error,omitempty"`
	At     time.Time   `json:"at"`
}

// Result is a successful task output.
type Result struct {
	Output string
}

// ErrSimulatedCrash is returned by a "crash" task AFTER its side effect has
// been durably recorded. The executor treats it as "the worker process died
// right here": no completion is reported, heartbeats stop, and the lease
// expires. When the job is later requeued and retried, Run finds the ledger
// entry and returns the success without re-executing the body.
var ErrSimulatedCrash = errors.New("simulated worker crash after durable side effect")

// CrashHook, if set, is invoked AFTER the "crash" task's side effect has been
// durably recorded but BEFORE ErrSimulatedCrash is returned. Tests use it to
// observe the crash point; production code leaves it nil.
type CrashHook func(jobID string, attempt int)

// Registry resolves task kinds and executes them with idempotent de-dup.
type Registry struct {
	mu     sync.Mutex
	path   string
	ledger map[string]ledgerEntry
	hook   CrashHook

	// bodyRuns counts calls that actually entered executeBody per
	// (job, attempt). Replays from the ledger do NOT increment it, so a test
	// can assert that a retried-after-crash attempt executed its body once.
	bodyRuns map[string]int
}

// NewRegistry creates (or reopens) the registry whose ledger lives at
// <dataDir>/task-ledger.json. Existing entries survive process restart, which
// is what proves at-most-once execution across restarts.
func NewRegistry(dataDir string) *Registry {
	r := &Registry{
		path:     filepath.Join(dataDir, "task-ledger.json"),
		ledger:   map[string]ledgerEntry{},
		bodyRuns: map[string]int{},
	}
	if data, err := os.ReadFile(r.path); err == nil {
		_ = json.Unmarshal(data, &r.ledger)
	}
	return r
}

// SetCrashHook installs the simulated-crash hook (see CrashHook).
func (r *Registry) SetCrashHook(h CrashHook) { r.hook = h }

func ledgerKey(jobID string, attempt int) string {
	return fmt.Sprintf("%s#%d", jobID, attempt)
}

// latestForJobLocked returns the newest ledger entry for a job, scanning keys
// "jobID#n" by attempt number. Caller holds r.mu.
func (r *Registry) latestForJobLocked(jobID string) (ledgerEntry, bool) {
	prefix := jobID + "#"
	bestN := -1
	var best ledgerEntry
	for k, e := range r.ledger {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(k, prefix))
		if err != nil {
			continue
		}
		if n > bestN {
			bestN, best = n, e
		}
	}
	return best, bestN >= 0
}

// persistLocked writes the ledger atomically (tmp file + rename), so a crash
// at any point leaves either the old or the new complete file.
func (r *Registry) persistLocked() error {
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r.ledger, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

// Run executes spec for attempt key (jobID, attempt).
//
// Returning a cached entry when the key already exists is the whole point of
// the ledger: the caller (executor) does not know whether the previous
// executor died before or after the task's side effect; re-running the body
// would double-execute work, pretending it never ran would lose work.
// BodyRuns returns how often executeBody actually ran for a (job, attempt)
// key in this process. A value >1 would mean the same attempt double-executed
// its side effects.
func (r *Registry) BodyRuns(jobID string, attempt int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bodyRuns[ledgerKey(jobID, attempt)]
}

func (r *Registry) Run(ctx context.Context, jobID string, attempt int, spec domain.TaskSpec) (*Result, error) {
	key := ledgerKey(jobID, attempt)

	r.mu.Lock()
	if e, ok := r.ledger[key]; ok {
		// Exact (job, attempt) replay: body never runs again. This also
		// covers a crash task whose worker died after recording the effect —
		// the replay returns the recorded success normally, which is the
		// observable proof of at-most-once execution.
		if e.Kind == outcomeSuccess {
			r.mu.Unlock()
			return &Result{Output: e.Output}, nil
		}
		r.mu.Unlock()
		return nil, fmt.Errorf("replayed recorded failure: %s", e.Error)
	}
	// Crash tasks are re-executed under a NEW attempt number after lease
	// recovery, so the exact key misses; replay against the most recently
	// recorded effect of the same job instead.
	if spec.Kind == "crash" {
		if e, ok := r.latestForJobLocked(jobID); ok && e.Kind == outcomeSuccess {
			// Record the replay under the new key, persist, and return the
			// original result WITHOUT executing the body again.
			re := ledgerEntry{Kind: outcomeSuccess, Output: e.Output, At: time.Now()}
			r.ledger[key] = re
			_ = r.persistLocked()
			out := e.Output
			r.mu.Unlock()
			return &Result{Output: out}, nil
		}
	}
	r.mu.Unlock()

	r.mu.Lock()
	r.bodyRuns[key]++
	r.mu.Unlock()
	res, err := r.executeBody(ctx, jobID, attempt, spec)
	// A simulated crash already persisted its SUCCESS entry deliberately and
	// must not be overwritten by the generic error recording below.
	if errors.Is(err, ErrSimulatedCrash) {
		return res, err
	}

	// Only terminal outcomes are recorded. Timeout/cancel are decided by the
	// executor (the context ends); here the body either returns success/failure
	// or a context error, which is not recorded so a timeout retry can run.
	if err == nil || !isContextErr(err) {
		e := ledgerEntry{At: time.Now()}
		if err == nil {
			e.Kind, e.Output = outcomeSuccess, res.Output
		} else {
			e.Kind, e.Error = outcomeFailure, err.Error()
		}
		r.mu.Lock()
		// Concurrent replays cannot happen for the same key (one lease per
		// job), but guard against a racing close/reopen anyway.
		if _, exists := r.ledger[key]; !exists {
			r.ledger[key] = e
			_ = r.persistLocked()
		}
		r.mu.Unlock()
	}
	return res, err
}

func isContextErr(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}

// executeBody runs the simulated task semantics without ledger concerns.
func (r *Registry) executeBody(ctx context.Context, jobID string, attempt int, spec domain.TaskSpec) (*Result, error) {
	switch spec.Kind {
	case "ok", "crash":
		delay, text := parseDelayText(spec.Payload, ":")
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
		out := text
		if out == "" {
			out = spec.Payload
		}
		// The "crash" task simulates the worker dying AFTER the effect is
		// durable but BEFORE completion is reported.
		// The "crash" task ALWAYS persists its effect first (the hook is only
		// an observer), then dies via ErrSimulatedCrash without reporting
		// completion. The durable write is the exact point being simulated:
		// "side effect committed; acknowledgment lost".
		if spec.Kind == "crash" {
			r.mu.Lock()
			if _, exists := r.ledger[ledgerKey(jobID, attempt)]; !exists {
				r.ledger[ledgerKey(jobID, attempt)] = ledgerEntry{
					Kind: outcomeSuccess, Output: out, At: time.Now(),
				}
				_ = r.persistLocked()
			}
			r.mu.Unlock()
			if r.hook != nil {
				r.hook(jobID, attempt)
			}
			return nil, ErrSimulatedCrash
		}
		return &Result{Output: out}, nil

	case "fail":
		delay, msg := parseDelayText(spec.Payload, "|")
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
		if strings.TrimSpace(msg) == "" {
			msg = "task failed"
		}
		return nil, fmt.Errorf("%s", msg)

	case "sleep":
		d, err := time.ParseDuration(strings.TrimSpace(spec.Payload))
		if err != nil {
			return nil, fmt.Errorf("sleep: bad duration %q: %w", spec.Payload, err)
		}
		if err := sleepCtx(ctx, d); err != nil {
			return nil, err
		}
		return &Result{Output: "slept " + d.String()}, nil

	case "flaky":
		n, err := strconv.Atoi(strings.TrimSpace(spec.Payload))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("flaky: payload must be N>=1, got %q", spec.Payload)
		}
		// Count prior EXECUTED attempts of this job from the ledger. Because a
		// crash replay returns the cached entry without re-executing, this
		// number equals the number of attempts that genuinely ran.
		r.mu.Lock()
		prior := 0
		for k := range r.ledger {
			if strings.HasPrefix(k, jobID+"#") {
				prior++
			}
		}
		r.mu.Unlock()
		// "prior" counts genuine earlier executions; a crash replay did not
		// run the body again and therefore does not consume a failure.
		genuineRun := prior + 1
		if genuineRun < n {
			return nil, fmt.Errorf("flaky failure on execution %d of %d (attempt %d)", genuineRun, n, attempt)
		}
		return &Result{Output: fmt.Sprintf("flaky success on execution %d (attempt %d)", genuineRun, attempt)}, nil

	case "panic":
		// Simulates a task that crashes its own execution. The executor's
		// recover() converts this into FAILED+task_panic; the worker loop
		// itself survives. Payload is the panic message.
		msg := strings.TrimSpace(spec.Payload)
		if msg == "" {
			msg = "boom: task body panicked"
		}
		panic(msg)

	default:
		return nil, fmt.Errorf("unknown task kind %q", spec.Kind)
	}
}

// parseDelayText accepts "10ms<sep>text" or "text" or "10ms" and splits the
// leading optional Go duration from the remainder.
func parseDelayText(payload, sep string) (time.Duration, string) {
	if i := strings.Index(payload, sep); i >= 0 {
		head := strings.TrimSpace(payload[:i])
		if d, err := time.ParseDuration(head); err == nil {
			return d, strings.TrimSpace(payload[i+len(sep):])
		}
	}
	if d, err := time.ParseDuration(strings.TrimSpace(payload)); err == nil {
		return d, ""
	}
	return 0, strings.TrimSpace(payload)
}

// sleepCtx sleeps for d but returns early when ctx is canceled/times out.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
