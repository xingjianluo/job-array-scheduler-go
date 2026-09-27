// Package job defines the core domain model of the job array scheduler:
// jobs, job groups, terminal failure reasons and the auditable event log.
package job

import "time"

// Status is the state of a single job in its lifecycle.
type Status string

const (
	// StatusPending means the job is persisted but its group has not
	// opened the barrier yet, so it is not eligible for claiming.
	StatusPending Status = "PENDING"
	// StatusReady means the group barrier is open and the job may be
	// claimed by a worker.
	StatusReady Status = "READY"
	// StatusRunning means a worker holds an active lease on the job.
	StatusRunning Status = "RUNNING"
	// StatusSucceeded is the terminal success state.
	StatusSucceeded Status = "SUCCEEDED"
	// StatusFailed is the terminal failure state (retries exhausted or
	// non-retryable error).
	StatusFailed Status = "FAILED"
	// StatusTimedOut is the terminal state reached when the job's own
	// deadline expires.
	StatusTimedOut Status = "TIMED_OUT"
	// StatusCanceled is the terminal state caused by a user/group cancel.
	StatusCanceled Status = "CANCELED"
)

// Terminal reports whether the status never changes again.
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusTimedOut, StatusCanceled:
		return true
	}
	return false
}

// FailureReason is the mutually exclusive, auditable reason a job ended
// without SUCCEEDED. It stays empty for successful jobs.
type FailureReason string

const (
	// ReasonNone marks a successful (or not-yet-finished) job.
	ReasonNone FailureReason = ""
	// ReasonExitNonZero: the task process exited with a non-zero code.
	ReasonExitNonZero FailureReason = "EXIT_NON_ZERO"
	// ReasonSpawnFailure: the task process could not be started.
	ReasonSpawnFailure FailureReason = "SPAWN_FAILURE"
	// ReasonTimeout: the per-job timeout elapsed.
	ReasonTimeout FailureReason = "TIMEOUT"
	// ReasonUserCanceled: the job/group was explicitly canceled.
	ReasonUserCanceled FailureReason = "USER_CANCELED"
	// ReasonGroupAborted: another group member failed and the group uses
	// FAIL_FAST, so this member was aborted while not yet finished.
	ReasonGroupAborted FailureReason = "GROUP_ABORTED"
	// ReasonRetriesExhausted: all retry attempts ended in failure.
	ReasonRetriesExhausted FailureReason = "RETRIES_EXHAUSTED"
)

// GroupPolicy decides when members of a group become runnable and what
// happens when one member fails.
type GroupPolicy string

const (
	// PolicyFailFast: all members become ready together; the first member
	// failure aborts every other unfinished member.
	PolicyFailFast GroupPolicy = "FAIL_FAST"
	// PolicyContinue: all members become ready together; member failures
	// do not affect the other members.
	PolicyContinue GroupPolicy = "CONTINUE"
)

// GroupStatus is the aggregate lifecycle state of a job group.
type GroupStatus string

const (
	GroupPending   GroupStatus = "PENDING"   // barrier closed
	GroupRunning   GroupStatus = "RUNNING"   // barrier open, members in flight
	GroupSucceeded GroupStatus = "SUCCEEDED" // all members succeeded
	GroupFailed    GroupStatus = "FAILED"    // at least one member failed/timed out
	GroupCanceled  GroupStatus = "CANCELED"  // the group was canceled
)

// Spec describes the command a job executes locally.
type Spec struct {
	// Command is the executable (looked up in PATH when not absolute).
	Command string
	Args    []string
	// Env is extra environment variables ("K=V") for the process.
	Env []string
	// Timeout, when > 0, bounds the whole execution of one attempt.
	Timeout time.Duration
	// MaxRetries is the number of retries after the first attempt
	// (0 means run exactly once).
	MaxRetries int
	// RetryBackoff is the fixed delay before each retry attempt.
	RetryBackoff time.Duration
}

// Job is the persisted state of one array element.
type Job struct {
	ID       string
	GroupID  string
	Name     string
	Spec     Spec
	Status   Status
	Reason   FailureReason
	Attempts int
	// ExitCode holds the last attempt's process exit code (set even when
	// the result is a non-zero exit), -1 when the process did not exit
	// normally or never ran.
	ExitCode  int
	CreatedAt time.Time
	UpdatedAt time.Time
	// StartedAt / FinishedAt bracket the first start / terminal transition.
	StartedAt  time.Time
	FinishedAt time.Time
	// Lease fields, meaningful only while RUNNING.
	LeaseOwner string
	LeaseUntil time.Time
}

// Group is the persisted state of a job group (array).
type Group struct {
	ID        string
	Policy    GroupPolicy
	Status    GroupStatus
	Size      int
	Open      bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Event is one immutable state-transition record.
type Event struct {
	Seq     int64
	Time    time.Time
	GroupID string
	JobID   string
	Kind    string // e.g. SUBMITTED, READY, CLAIMED, SUCCEEDED, FAILED ...
	From    Status
	To      Status
	Reason  FailureReason
	Attempt int
	Owner   string
	Detail  string
}

// IsTerminalFailure reports whether the reason denotes a definitive
// non-canceled failure (used by group policies and statistics).
func (r FailureReason) IsTerminalFailure() bool {
	switch r {
	case ReasonExitNonZero, ReasonSpawnFailure, ReasonRetriesExhausted:
		return true
	}
	return false
}
