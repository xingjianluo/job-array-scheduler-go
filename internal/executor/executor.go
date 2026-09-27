// Package executor runs a job's local process for one attempt and reports
// a structured outcome (success / non-zero exit / spawn error / timeout /
// canceled), so the scheduler can map outcomes onto the job state machine
// without ambiguity.
package executor

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"syscall"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// OutcomeKind enumerates the mutually exclusive results of one attempt.
type OutcomeKind string

const (
	OutcomeSucceeded OutcomeKind = "SUCCEEDED"
	OutcomeFailed    OutcomeKind = "FAILED"   // process ran, exit code != 0
	OutcomeSpawn     OutcomeKind = "SPAWN"    // process could not start
	OutcomeTimeout   OutcomeKind = "TIMEOUT"  // attempt deadline elapsed
	OutcomeCanceled  OutcomeKind = "CANCELED" // ctx canceled before/during run
)

// maxCapture bounds captured stdout/stderr so a chatty child cannot grow
// memory without limit.
const maxCapture = 64 * 1024

// Outcome is the structured result of a single attempt.
type Outcome struct {
	Kind     OutcomeKind
	ExitCode int
	Reason   job.FailureReason
	Stdout   string
	Stderr   string
	Duration time.Duration
}

// Runner executes one job attempt as a local OS process.
type Runner interface {
	Run(ctx context.Context, spec job.Spec) Outcome
}

// Executor is the default Runner. Every child is started in its own
// process group (Setpgid); on timeout/cancel the whole group is SIGKILLed
// and waited for, which guarantees the occupied process resources are
// always reaped.
type Executor struct {
	workDir string
}

// New creates an Executor. workDir is the working directory of children
// (empty means inherit the parent's cwd).
func New(workDir string) *Executor {
	return &Executor{workDir: workDir}
}

type cappedBuffer struct {
	buf bytes.Buffer
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	remaining := maxCapture - c.buf.Len()
	if remaining <= 0 {
		return len(p), nil // pretend consumed; data is dropped
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return c.buf.Write(p)
}

// Run implements Runner.
func (e *Executor) Run(ctx context.Context, spec job.Spec) Outcome {
	start := time.Now()
	defer func() {}()

	// A pre-canceled context means the attempt was aborted before start.
	if err := ctx.Err(); err != nil {
		return classifyContext(err, time.Since(start))
	}

	cmd := exec.CommandContext(ctx, spec.Command, spec.Args...)
	cmd.Env = append(cmd.Environ(), spec.Env...)
	if e.workDir != "" {
		cmd.Dir = e.workDir
	}
	// New process group: the negative pid is used to kill children too.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var stdout, stderr cappedBuffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// cmd.Cancel kills the entire group instead of just the parent shell.
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return osErrProcessKilled
	}
	cmd.WaitDelay = 5 * time.Second

	if err := cmd.Start(); err != nil {
		return Outcome{
			Kind:     OutcomeSpawn,
			ExitCode: -1,
			Reason:   job.ReasonSpawnFailure,
			Stderr:   trimCapture(stderr.buf.String()),
			Duration: time.Since(start),
		}
	}

	waitErr := cmd.Wait()
	dur := time.Since(start)

	out := Outcome{
		ExitCode: exitCodeOf(cmd),
		Stdout:   trimCapture(stdout.buf.String()),
		Stderr:   trimCapture(stderr.buf.String()),
		Duration: dur,
	}
	switch {
	case waitErr == nil:
		out.Kind = OutcomeSucceeded
		out.ExitCode = 0
		return out
	case ctx.Err() == context.DeadlineExceeded:
		out.Kind = OutcomeTimeout
		out.ExitCode = -1
		out.Reason = job.ReasonTimeout
		return out
	case ctx.Err() == context.Canceled:
		out.Kind = OutcomeCanceled
		out.ExitCode = -1
		out.Reason = job.ReasonUserCanceled
		return out
	default:
		// Process genuinely ran and ended non-zero (or was signaled
		// without our context being the cause).
		out.Kind = OutcomeFailed
		out.Reason = job.ReasonExitNonZero
		return out
	}
}

func classifyContext(err error, dur time.Duration) Outcome {
	o := Outcome{ExitCode: -1, Duration: dur}
	if errors.Is(err, context.DeadlineExceeded) {
		o.Kind = OutcomeTimeout
		o.Reason = job.ReasonTimeout
	} else {
		o.Kind = OutcomeCanceled
		o.Reason = job.ReasonUserCanceled
	}
	return o
}

func exitCodeOf(cmd *exec.Cmd) int {
	if cmd.ProcessState == nil {
		return -1
	}
	return cmd.ProcessState.ExitCode()
}

func trimCapture(s string) string {
	if len(s) > maxCapture {
		return s[:maxCapture]
	}
	return s
}

// osErrProcessKilled is reported by cmd.Cancel; it is never surfaced
// directly (Wait maps it onto our context-based classification).
var osErrProcessKilled = errors.New("process group killed")
