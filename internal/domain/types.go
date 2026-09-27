// Package domain defines the core state-machine types for the local
// job-array scheduler: jobs, groups, attempts, events and configuration.
package domain

import "time"

// ---------------------------------------------------------------------------
// Job state machine
// ---------------------------------------------------------------------------

// JobState is the lifecycle state of a single job.
//
//	PENDING -> READY -> RUNNING -> SUCCEEDED
//	                  |          -> FAILED (retryable failure goes back to READY)
//	                  |          -> TIMED_OUT
//	                  |          -> CANCELED
//	PENDING / READY / RUNNING -> CANCELED
//
// FAILED, TIMED_OUT and CANCELED are all non-retryable terminal states;
// the distinction between them is the final statistical classification.
type JobState string

const (
	// StatePending: job accepted but its group has not been admitted yet.
	StatePending JobState = "PENDING"
	// StateReady: group admitted; the job is eligible to be claimed.
	// A job that is waiting for a retry backoff also stays in READY and
	// becomes claimable once NextAttemptAt has passed.
	StateReady JobState = "READY"
	// StateRunning: a worker holds the lease and an attempt is executing.
	StateRunning JobState = "RUNNING"
	// StateSucceeded: one attempt completed successfully. Terminal.
	StateSucceeded JobState = "SUCCEEDED"
	// StateFailed: all attempts were exhausted with task errors. Terminal.
	StateFailed JobState = "FAILED"
	// StateTimedOut: an attempt exceeded the job timeout and retries are
	// exhausted (or retry is disabled). Terminal.
	StateTimedOut JobState = "TIMED_OUT"
	// StateCanceled: the job was canceled by the user or by its group's
	// fail-fast policy. Terminal.
	StateCanceled JobState = "CANCELED"
)

// Terminal reports whether the state is a final state.
func (s JobState) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateTimedOut, StateCanceled:
		return true
	default:
		return false
	}
}

// Reasons explain WHY a job reached a non-success terminal state. The state
// itself gives the coarse classification (failure / timeout / cancel) and the
// reason gives the distinguishable sub-cause required by statistics and logs.
const (
	// ReasonTaskFailed: the task returned an ordinary error.
	ReasonTaskFailed = "task_failed"
	// ReasonTaskPanic: the task panicked; recovered by the executor.
	ReasonTaskPanic = "task_panic"
	// ReasonTimeout: an attempt did not finish within the job timeout.
	ReasonTimeout = "timeout"
	// ReasonUserCanceled: the job was canceled through the cancel API.
	ReasonUserCanceled = "user_canceled"
	// ReasonGroupFailFast: canceled because another member of the group
	// failed and the group uses the fail-fast failure policy.
	ReasonGroupFailFast = "group_failfast"
	// ReasonGroupRejected: the group submission itself was rejected and
	// its members never became runnable.
	ReasonGroupRejected = "group_rejected"
)

// ---------------------------------------------------------------------------
// Group policies and state
// ---------------------------------------------------------------------------

// AdmissionPolicy decides when a group as a whole is allowed to start.
type AdmissionPolicy string

const (
	// AdmissionImmediate: every member becomes runnable as soon as it is
	// submitted, without waiting for the other members.
	AdmissionImmediate AdmissionPolicy = "immediate"
	// AdmissionAll: the group starts only once all ExpectedMembers have
	// been submitted. No member runs before the barrier is released.
	AdmissionAll AdmissionPolicy = "all"
	// AdmissionQuorum: the group starts once a quorum
	// (ExpectedMembers/2+1) has been submitted. Late members become
	// runnable immediately on submission because the barrier is already open.
	AdmissionQuorum AdmissionPolicy = "quorum"
)

// FailurePolicy decides how a group reacts to a terminal failure of one member.
type FailurePolicy string

const (
	// FailureContinue: keep running the remaining members; the group
	// succeeds if at least one member succeeds.
	FailureContinue FailurePolicy = "continue"
	// FailureFailFast: the first non-success terminal member cancels all
	// other not-yet-terminal members of the group.
	FailureFailFast FailurePolicy = "failfast"
)

// GroupState is the lifecycle state of a job group.
type GroupState string

const (
	GroupPending   GroupState = "PENDING"   // barrier not released yet
	GroupRunning   GroupState = "RUNNING"   // admitted, at least one member active
	GroupSucceeded GroupState = "SUCCEEDED" // all members terminal, at least one success
	GroupFailed    GroupState = "FAILED"    // all members terminal, none succeeded
	GroupCanceled  GroupState = "CANCELED"  // group canceled before admission or wholesale
)

// ---------------------------------------------------------------------------
// Retry / backoff
// ---------------------------------------------------------------------------

// Backoff is the waiting strategy between attempts.
type Backoff string

const (
	BackoffFixed       Backoff = "fixed"       // always BaseDelay
	BackoffLinear      Backoff = "linear"      // BaseDelay * failedAttempts
	BackoffExponential Backoff = "exponential" // BaseDelay * 2^(failedAttempts-1), capped at MaxDelay
)

// RetryPolicy controls how many times a task attempt is retried and how long
// the scheduler waits between attempts.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	// Values <= 1 mean the job runs exactly once.
	MaxAttempts int `json:"max_attempts"`
	// Backoff selects the delay strategy.
	Backoff Backoff `json:"backoff"`
	// BaseDelay is the first / fixed delay unit.
	BaseDelay time.Duration `json:"base_delay"`
	// MaxDelay caps a single computed delay (0 = no cap).
	MaxDelay time.Duration `json:"max_delay"`
}

// Delay returns the wait before attempt number "nextAttempt" (1-based), given
// that "failedAttempts" attempts have already failed.
func (p RetryPolicy) Delay(failedAttempts int) time.Duration {
	if failedAttempts <= 0 || p.BaseDelay <= 0 {
		return 0
	}
	var d time.Duration
	switch p.Backoff {
	case BackoffLinear:
		d = p.BaseDelay * time.Duration(failedAttempts)
	case BackoffExponential:
		d = p.BaseDelay << (failedAttempts - 1)
	default: // BackoffFixed or unspecified
		d = p.BaseDelay
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// ---------------------------------------------------------------------------
// Submission specs
// ---------------------------------------------------------------------------

// TaskSpec describes which local simulated task to run and its input.
//
// Supported kinds:
//   - "ok":    succeed after Payload-parsed duration ("10ms"), echo Payload.
//   - "fail":  fail with message Payload, after an optional delay encoded as
//     "delay|message".
//   - "flaky": fail the first FailUntil successful runs ... see task registry;
//     persistently counts successful completions so a re-executed
//     attempt never changes history. Payload: "N" (succeed on Nth run).
//   - "sleep": sleep Payload duration then succeed (used for timeout tests).
type TaskSpec struct {
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

// JobSpec is one member of a submission batch.
type JobSpec struct {
	Name    string        `json:"name"`
	Task    TaskSpec      `json:"task"`
	Timeout time.Duration `json:"timeout"` // 0 => Config.DefaultJobTimeout
}

// GroupSpec describes a batch. A group decides as a whole when its members
// start and how member failures affect the rest of the batch.
type GroupSpec struct {
	Name            string          `json:"name"`
	Admission       AdmissionPolicy `json:"admission"`
	FailurePolicy   FailurePolicy   `json:"failure_policy"`
	ExpectedMembers int             `json:"expected_members"` // used by all/quorum
	// Retry is applied to every member of the batch.
	Retry RetryPolicy `json:"retry"`
}

// ---------------------------------------------------------------------------
// Persistent entities
// ---------------------------------------------------------------------------

// Attempt records one execution of a job by one worker. Attempts are
// append-only: every lease produces exactly one attempt record, which is what
// makes repeated claims, retries and process restarts auditable.
type Attempt struct {
	Attempt   int       `json:"attempt"`
	WorkerID  string    `json:"worker_id"`
	Token     int64     `json:"token"` // fencing token, unique per lease
	StartedAt time.Time `json:"started_at"`
	// LastHeartbeat is renewed by the worker while the attempt is alive.
	// Recovery considers an attempt dead when now-LastHeartbeat > lease TTL.
	LastHeartbeat time.Time `json:"last_heartbeat,omitempty"`
	EndedAt       time.Time `json:"ended_at,omitempty"`
	// Outcome of this attempt: SUCCEEDED / FAILED / TIMED_OUT / CANCELED.
	State   JobState `json:"state,omitempty"`
	Reason  string   `json:"reason,omitempty"`
	Message string   `json:"message,omitempty"`
	Output  string   `json:"output,omitempty"`
}

// Job is the persistent state of one unit of work.
type Job struct {
	ID      string `json:"id"`
	GroupID string `json:"group_id"`
	Name    string `json:"name"`

	Task    TaskSpec      `json:"task"`
	Retry   RetryPolicy   `json:"retry"`
	Timeout time.Duration `json:"timeout"`

	State   JobState `json:"state"`
	Reason  string   `json:"reason,omitempty"`
	Message string   `json:"message,omitempty"`

	// Attempts holds finished and in-flight attempts. The in-flight attempt
	// (if any) is the last element with zero EndedAt.
	Attempts []*Attempt `json:"attempts"`
	// NextAttemptAt: while READY, the job is not claimable before this time
	// (retry backoff). Zero means claimable immediately.
	NextAttemptAt time.Time `json:"next_attempt_at,omitempty"`

	// CancelRequested is set when a running job must stop cooperatively
	// (user cancel or group fail-fast). Workers observe it via the store.
	CancelRequested bool `json:"cancel_requested,omitempty"`
	// CancelReason explains a pending cancel: user_canceled / group_failfast.
	CancelReason string `json:"cancel_reason,omitempty"`
	// Output holds the terminal output (copied from the winning attempt).
	Output string `json:"output,omitempty"`

	EnqueueAt  time.Time `json:"enqueue_at"`
	ReadyAt    time.Time `json:"ready_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// CurrentAttempt returns the attempt number that should run next (1-based).
func (j *Job) CurrentAttempt() int {
	return len(j.Attempts) + 1
}

// InFlightAttempt returns the currently executing attempt, or nil.
func (j *Job) InFlightAttempt() *Attempt {
	if len(j.Attempts) == 0 {
		return nil
	}
	a := j.Attempts[len(j.Attempts)-1]
	if a.EndedAt.IsZero() {
		return a
	}
	return nil
}

// Group is the persistent state of a batch.
type Group struct {
	ID     string     `json:"id"`
	Spec   GroupSpec  `json:"spec"`
	State  GroupState `json:"state"`
	Reason string     `json:"reason,omitempty"`

	JobIDs []string `json:"job_ids"`

	AdmittedAt time.Time `json:"admitted_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// Event is one append-only trace record. Every state transition emits an
// event, providing a recoverable, explainable history.
type Event struct {
	Seq      int64     `json:"seq"`
	Time     time.Time `json:"time"`
	Type     string    `json:"type"`
	GroupID  string    `json:"group_id,omitempty"`
	JobID    string    `json:"job_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Attempt  int       `json:"attempt,omitempty"`
	Token    int64     `json:"token,omitempty"`
	Detail   string    `json:"detail,omitempty"`
}

// Event type constants.
const (
	EvGroupSubmitted  = "group_submitted"
	EvJobSubmitted    = "job_submitted"
	EvGroupAdmitted   = "group_admitted"
	EvGroupRejected   = "group_rejected"
	EvJobReady        = "job_ready"
	EvJobClaimed      = "job_claimed"
	EvJobHeartbeat    = "job_heartbeat"
	EvJobLeaseExpired = "job_lease_expired"
	EvJobSucceeded    = "job_succeeded"
	EvJobFailed       = "job_failed"
	EvJobRetryWait    = "job_retry_wait"
	EvJobTimedOut     = "job_timed_out"
	EvJobCanceled     = "job_canceled"
	EvJobCancelReq    = "job_cancel_requested"
	EvGroupSucceeded  = "group_succeeded"
	EvGroupFailed     = "group_failed"
	EvGroupCanceled   = "group_canceled"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// Config holds scheduler-wide configuration and tunables.
type Config struct {
	// DataDir is where the snapshot, event log and task-side ledgers live.
	DataDir string
	// NumWorkers is the number of concurrent executor goroutines.
	NumWorkers int
	// MaxParallel globally caps how many jobs may run at once (0 = NumWorkers).
	MaxParallel int
	// HeartbeatInterval: workers renew leases at this cadence.
	HeartbeatInterval time.Duration
	// LeaseTimeout: a lease whose latest heartbeat is older than this is
	// considered dead and its job becomes reclaimable (simulating crash).
	LeaseTimeout time.Duration
	// DefaultJobTimeout applies to jobs without an explicit timeout.
	DefaultJobTimeout time.Duration
	// DefaultRetry applies to jobs/groups without an explicit policy.
	DefaultRetry RetryPolicy
	// PollInterval is the scheduler/executor polling cadence.
	PollInterval time.Duration
}

// DefaultConfig returns a configuration suitable for local demos and tests.
func DefaultConfig() Config {
	return Config{
		DataDir:           ".scheduler-data",
		NumWorkers:        4,
		MaxParallel:       4,
		HeartbeatInterval: 20 * time.Millisecond,
		LeaseTimeout:      200 * time.Millisecond,
		DefaultJobTimeout: 5 * time.Second,
		DefaultRetry: RetryPolicy{
			MaxAttempts: 1,
			Backoff:     BackoffFixed,
			BaseDelay:   20 * time.Millisecond,
		},
		PollInterval: 5 * time.Millisecond,
	}
}
