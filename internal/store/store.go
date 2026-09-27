// Package store provides the durable, process-recoverable state for jobs
// and groups. The reference implementation FileStore uses an exclusive
// OS file lock plus atomic snapshot writes and an append-only event log,
// which makes state changes recoverable after process restarts.
package store

import (
	"context"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// ErrNotFound is returned when a requested group/job does not exist.
var ErrNotFound = storeErr("not found")

// ErrConflict is returned when an optimistic transition does not apply
// (e.g. claiming a job that another worker already owns).
var ErrConflict = storeErr("conflict")

type storeErr string

func (e storeErr) Error() string { return string(e) }

// View is a consistent read snapshot taken while holding the store lock.
// Slices returned by a View must not be retained or mutated after Release.
type View interface {
	Groups() []job.Group
	Jobs() []job.Job
	Group(id string) (job.Group, bool)
	Job(id string) (job.Job, bool)
	JobsOfGroup(groupID string) []job.Job
	Release()
}

// Mutate applies fn to a locked, mutable copy of the state. If fn returns
// an error nothing is persisted and that error is returned. Otherwise the
// resulting state is atomically snapshotted and every returned event is
// appended to the durable event log.
type MutateFunc func(tx Tx) error

// Tx is the locked mutation handle passed to Mutate.
type Tx interface {
	View
	// InsertGroup/InsertJob add new records.
	InsertGroup(g job.Group)
	InsertJob(j job.Job)
	// UpdateGroup/UpdateJob replace records matched by ID.
	UpdateGroup(g job.Group)
	UpdateJob(j job.Job)
	// Emit records an auditable state-transition event.
	Emit(e job.Event)
}

// Store is the persistence boundary of the scheduler.
type Store interface {
	// Read runs fn against a locked snapshot without persisting anything.
	Read(fn func(v View) error) error
	// Mutate runs fn as an atomic, durable read-modify-write transaction.
	Mutate(ctx context.Context, fn MutateFunc) error
	// Events returns the event log in append order, optionally filtered
	// to a single job (empty jobID means all events).
	Events(jobID string) ([]job.Event, error)
	// Close releases all held resources (file locks, handles).
	Close() error
}
