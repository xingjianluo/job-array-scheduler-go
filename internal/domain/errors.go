package domain

import "errors"

// Sentinel errors shared across packages.
var (
	// ErrNotFound: no such job or group.
	ErrNotFound = errors.New("not found")
	// ErrConflict: optimistic concurrency / state-machine conflict, e.g.
	// claiming a job that is not claimable, or double completion.
	ErrConflict = errors.New("conflict")
	// ErrGroupClosed: the group is already admitted/terminal and cannot
	// accept new members.
	ErrGroupClosed = errors.New("group closed")
)
