package stats

import (
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

func TestBuildBucketsAreExclusiveAndStable(t *testing.T) {
	now := time.Unix(1700000000, 0)
	mk := func(id string, st job.Status, reason job.FailureReason) job.Job {
		return job.Job{ID: id, GroupID: "g1", Status: st, Reason: reason, ExitCode: -1, Attempts: 1, CreatedAt: now, UpdatedAt: now}
	}
	jobs := []job.Job{
		mk("j1", job.StatusSucceeded, job.ReasonNone),
		mk("j2", job.StatusFailed, job.ReasonRetriesExhausted),
		mk("j3", job.StatusTimedOut, job.ReasonTimeout),
		mk("j4", job.StatusCanceled, job.ReasonUserCanceled),
		mk("j5", job.StatusCanceled, job.ReasonGroupAborted),
		{ID: "j6", GroupID: "g1", Status: job.StatusRunning, CreatedAt: now},
	}
	groups := []job.Group{{ID: "g1", Policy: job.PolicyFailFast, Status: job.GroupFailed}}

	r1 := Build(jobs, groups)
	r2 := Build(jobs, groups)
	t.Logf("input buckets: 1 success,1 fail,1 timeout,2 canceled,1 in-flight")
	t.Logf("verdict: S=%d F=%d T=%d C=%d total=%d inflight=%d",
		r1.All.Succeeded, r1.All.Failed, r1.All.TimedOut, r1.All.Canceled, r1.All.Total(), len(r1.InFlight))
	if r1.All != (Counters{Succeeded: 1, Failed: 1, TimedOut: 1, Canceled: 2}) {
		t.Fatalf("buckets wrong: %+v", r1.All)
	}
	if r1.All.Total() != 5 || len(r1.InFlight) != 1 {
		t.Fatalf("exhaustiveness broken total=%d inflight=%d", r1.All.Total(), len(r1.InFlight))
	}
	// Determinism: same input -> identical output across runs.
	if r1.All != r2.All || r1.Groups["g1"].Counters != r2.Groups["g1"].Counters {
		t.Fatalf("Build is not stable across calls: %+v vs %+v", r1, r2)
	}
}
