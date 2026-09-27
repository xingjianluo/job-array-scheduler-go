// Package stats produces deterministic terminal counters for groups and
// for the whole scheduler: success/failure/timeout/canceled are mutually
// exclusive buckets and every terminal job falls into exactly one.
package stats

import (
	"sort"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// Counters is the exhaustive, mutually exclusive terminal breakdown.
type Counters struct {
	Succeeded int
	Failed    int
	TimedOut  int
	Canceled  int
}

// Total returns the number of accounted terminal jobs.
func (c Counters) Total() int {
	return c.Succeeded + c.Failed + c.TimedOut + c.Canceled
}

// Add places one terminal status into exactly one bucket. Non-terminal
// statuses are ignored.
func (c *Counters) Add(st job.Status) {
	switch st {
	case job.StatusSucceeded:
		c.Succeeded++
	case job.StatusFailed:
		c.Failed++
	case job.StatusTimedOut:
		c.TimedOut++
	case job.StatusCanceled:
		c.Canceled++
	}
}

// JobResult is the explainable per-job final line.
type JobResult struct {
	Name     string
	Status   job.Status
	Reason   job.FailureReason
	Attempts int
	ExitCode int
}

// GroupReport adds per-group policy and job-level detail.
type GroupReport struct {
	Policy job.GroupPolicy
	Status job.GroupStatus
	Counters
	Jobs []JobResult
}

// Report is a full statistics snapshot for one run.
type Report struct {
	GroupIDs []string
	Groups   map[string]GroupReport
	All      Counters
	// InFlight lists non-terminal job IDs (empty once the run is idle).
	InFlight []string
}

// Build computes a Report from a store snapshot. The bucket placement is
// a pure function of terminal (status, reason), so the same final state
// always yields the same counters, run after run.
func Build(jobs []job.Job, groups []job.Group) Report {
	r := Report{Groups: map[string]GroupReport{}}
	index := map[string]int{}
	for gi, g := range groups {
		index[g.ID] = gi
		r.Groups[g.ID] = GroupReport{Policy: g.Policy, Status: g.Status}
		r.GroupIDs = append(r.GroupIDs, g.ID)
	}
	sort.Strings(r.GroupIDs)
	// Stable per-job order: submission (created) then ID.
	sortedJobs := append([]job.Job(nil), jobs...)
	sort.SliceStable(sortedJobs, func(a, b int) bool {
		if !sortedJobs[a].CreatedAt.Equal(sortedJobs[b].CreatedAt) {
			return sortedJobs[a].CreatedAt.Before(sortedJobs[b].CreatedAt)
		}
		return sortedJobs[a].ID < sortedJobs[b].ID
	})
	for _, j := range sortedJobs {
		if !j.Status.Terminal() {
			r.InFlight = append(r.InFlight, j.ID)
			continue
		}
		r.All.Add(j.Status)
		gr, ok := r.Groups[j.GroupID]
		if !ok {
			gr = GroupReport{}
		}
		gr.Add(j.Status)
		gr.Jobs = append(gr.Jobs, JobResult{
			Name: j.Name, Status: j.Status, Reason: j.Reason,
			Attempts: j.Attempts, ExitCode: j.ExitCode,
		})
		r.Groups[j.GroupID] = gr
	}
	return r
}

// InFlight returns the IDs of jobs that have not reached a terminal state.
func InFlight(jobs []job.Job) []string {
	var out []string
	for _, j := range jobs {
		if !j.Status.Terminal() {
			out = append(out, j.ID)
		}
	}
	sort.Strings(out)
	return out
}
