package scheduler

import (
	"context"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// RecoverStaleLeases finds RUNNING jobs whose leases have expired (dead
// owner process, or a restart that interrupted an attempt before its
// terminal commit) and atomically returns them to READY. An auditable
// REQUEUED event keeps the history explainable, and Attempts is preserved
// so the retry budget is not reset by a crash.
func (s *Scheduler) RecoverStaleLeases(ctx context.Context) (int, error) {
	now := s.now()
	n := 0
	err := s.st.Mutate(ctx, func(tx store.Tx) error {
		for _, j := range tx.Jobs() {
			if j.Status != job.StatusRunning {
				continue
			}
			if now.Before(j.LeaseUntil) {
				continue // healthy lease
			}
			from := j.Status
			oldOwner := j.LeaseOwner
			j.Status = job.StatusReady
			j.LeaseOwner = ""
			j.LeaseUntil = time.Time{}
			j.UpdatedAt = now
			tx.UpdateJob(j)
			tx.Emit(job.Event{
				Time: now, GroupID: j.GroupID, JobID: j.ID, Kind: "REQUEUED",
				From: from, To: job.StatusReady, Owner: oldOwner,
				Attempt: j.Attempts, Detail: "stale lease recovered",
			})
			n++
		}
		return nil
	})
	return n, err
}
