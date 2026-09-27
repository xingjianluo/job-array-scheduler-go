package store

import "time"

// Config configures a FileStore.
type Config struct {
	// Dir is the directory holding LOCK and journal.log (created if absent).
	Dir string
	// LeaseTTL is advisory: the scheduler decides when a lease is stale.
	// Kept here so a reopened store can share one configuration source.
	LeaseTTL time.Duration
	// Now injects the clock (tests pin it; nil means time.Now).
	Now func() time.Time
}
