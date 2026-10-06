// Package stats produces stable, classified final summaries over the store.
//
// Classification is two-level on purpose:
//
//	coarse (one of SUCCEEDED / FAILED / TIMED_OUT / CANCELED / SKIPPED) —
//	the final statistical columns required for repeatable end-to-end
//	conclusions;
//	fine (job.Reason: task_failed / task_panic / timeout / user_canceled /
//	group_failfast / lease_expired / dependency_failed ...) — the
//	distinguishable cause.
//
// SKIPPED is its own coarse column: a member that never ran because a
// dependency did not succeed is never folded into failed/timed_out/canceled.
//
// Every string is rendered from sorted inputs (group/job IDs are generated
// deterministically), so the same scenario always produces byte-identical
// reports across repeated runs.
package stats

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// GroupSummary classifies one group's members.
type GroupSummary struct {
	GroupID   string
	Name      string
	State     domain.GroupState
	Total     int
	Succeeded int
	Failed    int
	TimedOut  int
	Canceled  int
	Skipped   int
	ByReason  map[string]int
	Jobs      []*domain.Job
}

// Duration returns wall time from group admission (or enqueue) to finish.
func (g *GroupSummary) Duration() time.Duration {
	var min, max time.Time
	for _, j := range g.Jobs {
		if min.IsZero() || j.EnqueueAt.Before(min) {
			min = j.EnqueueAt
		}
		if j.FinishedAt.After(max) {
			max = j.FinishedAt
		}
	}
	if max.IsZero() {
		return 0
	}
	return max.Sub(min)
}

// Summary is the overall result classification.
type Summary struct {
	Groups []*GroupSummary
}

// Build computes a snapshot summary from the store. Groups and jobs are
// emitted in ID order, which makes the report deterministic.
func Build(st store.Store) *Summary {
	groups := st.ListGroups()
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })

	sum := &Summary{Groups: make([]*GroupSummary, 0, len(groups))}
	for _, g := range groups {
		jobs := st.ListJobs(g.ID)
		sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
		gs := &GroupSummary{
			GroupID:  g.ID,
			Name:     g.Spec.Name,
			State:    g.State,
			Total:    len(jobs),
			ByReason: map[string]int{},
			Jobs:     jobs,
		}
		for _, j := range jobs {
			switch j.State {
			case domain.StateSucceeded:
				gs.Succeeded++
			case domain.StateFailed:
				gs.Failed++
			case domain.StateTimedOut:
				gs.TimedOut++
			case domain.StateCanceled:
				gs.Canceled++
			case domain.StateSkipped:
				gs.Skipped++
			}
			if j.State != domain.StateSucceeded {
				r := j.Reason
				if r == "" {
					r = "unknown"
				}
				gs.ByReason[r]++
			}
		}
		sum.Groups = append(sum.Groups, gs)
	}
	return sum
}

// Totals aggregates all groups.
func (s *Summary) Totals() (total, ok, failed, timeout, canceled, skipped int) {
	for _, g := range s.Groups {
		total += g.Total
		ok += g.Succeeded
		failed += g.Failed
		timeout += g.TimedOut
		canceled += g.Canceled
		skipped += g.Skipped
	}
	return
}

// String renders a stable, human-readable report.
func (s *Summary) String() string {
	var b strings.Builder
	b.WriteString("==================== JOB-ARRAY SCHEDULER SUMMARY ====================\n")
	var T, S, F, TO, C, K int
	for _, g := range s.Groups {
		T += g.Total
		S += g.Succeeded
		F += g.Failed
		TO += g.TimedOut
		C += g.Canceled
		K += g.Skipped
		fmt.Fprintf(&b, "group %s %q state=%s duration=%s\n", g.GroupID, g.Name, g.State, g.Duration().Round(time.Millisecond))
		fmt.Fprintf(&b, "  jobs total=%d  succeeded=%d  failed=%d  timed_out=%d  canceled=%d  skipped=%d\n",
			g.Total, g.Succeeded, g.Failed, g.TimedOut, g.Canceled, g.Skipped)
		reasons := make([]string, 0, len(g.ByReason))
		for r := range g.ByReason {
			reasons = append(reasons, r)
		}
		sort.Strings(reasons)
		for _, r := range reasons {
			fmt.Fprintf(&b, "    reason %-18s %d\n", r, g.ByReason[r])
		}
		for _, j := range g.Jobs {
			line := fmt.Sprintf("    %s %-9q state=%-9s", j.ID, j.Name, j.State)
			if j.State != domain.StateSucceeded {
				line += fmt.Sprintf(" reason=%s", j.Reason)
			}
			if n := len(j.Attempts); n > 0 {
				line += fmt.Sprintf(" attempts=%d", n)
			}
			if j.Output != "" {
				line += fmt.Sprintf(" output=%q", j.Output)
			}
			b.WriteString(line + "\n")
		}
	}
	fmt.Fprintf(&b, "TOTAL: jobs=%d succeeded=%d failed=%d timed_out=%d canceled=%d skipped=%d\n", T, S, F, TO, C, K)
	b.WriteString("=====================================================================\n")
	return b.String()
}
