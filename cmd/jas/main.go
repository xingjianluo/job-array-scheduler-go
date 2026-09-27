// Command jas is a self-contained demo of the job array scheduler. It
// submits two groups (one CONTINUE, one FAIL_FAST) whose members cover
// success, retry-then-failure, timeout and cancellation, runs a worker
// pool in this process, waits for quiescence, and prints the per-group
// and global terminal statistics plus a short audit trail.
//
// State lives under --state-dir, so stopping and re-running the command
// demonstrates crash recovery: terminal jobs are never re-executed.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/executor"
	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
	"github.com/xingjianluo/job-array-scheduler-go/internal/scheduler"
	"github.com/xingjianluo/job-array-scheduler-go/internal/stats"
	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

func main() {
	stateDir := flag.String("state-dir", "./.jas-state", "directory for LOCK and journal.log")
	slots := flag.Int("slots", 4, "concurrent execution slots (simulated cluster capacity)")
	demo := flag.Bool("demo", true, "run the built-in mixed workload demo")
	reportOnly := flag.Bool("report-only", false, "do not submit; just print the recovered final report")
	flag.Parse()

	st, err := store.OpenFileStore(store.Config{Dir: *stateDir, LeaseTTL: 10 * time.Second})
	must(err)
	defer st.Close()

	sched := scheduler.New(st, executor.New(""), scheduler.Config{
		Slots: *slots, OwnerPrefix: "jas",
		LeaseTTL: 10 * time.Second, LeaseRenew: 3 * time.Second,
		PollInterval: 20 * time.Millisecond, RecoverInterval: time.Second,
	})
	defer sched.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	h := sched.Start(ctx)
	defer h.Shutdown(sched)

	if *reportOnly {
		if err := printReport(st); err != nil {
			fmt.Fprintln(os.Stderr, "report error:", err)
			os.Exit(1)
		}
		return
	}
	if *demo {
		if err := runDemo(ctx, sched, st); err != nil {
			fmt.Fprintln(os.Stderr, "demo error:", err)
			os.Exit(1)
		}
	}
}

func sh(script string, timeout time.Duration, retries int, backoff time.Duration) job.Spec {
	return job.Spec{Command: "sh", Args: []string{"-c", script},
		Timeout: timeout, MaxRetries: retries, RetryBackoff: backoff}
}

func runDemo(ctx context.Context, sched *scheduler.Scheduler, st store.Store) error {
	fmt.Println("== submitting group A (CONTINUE): success + retry-fail + timeout ==")
	gA, err := sched.SubmitGroup(ctx, job.PolicyContinue, []scheduler.SubmitItem{
		{Name: "a-ok", Spec: sh("echo '  a-ok running'; exit 0", 0, 0, 0)},
		{Name: "a-bad", Spec: sh("echo '  a-bad attempt'; exit 2", 0, 1, 100*time.Millisecond)},
		{Name: "a-slow", Spec: sh("echo '  a-slow running'; sleep 5", 400*time.Millisecond, 0, 0)},
		{Name: "a-ok2", Spec: sh("echo '  a-ok2 running'; exit 0", 0, 0, 0)},
	}, true)
	if err != nil {
		return err
	}
	fmt.Printf("group A=%s\n", gA.GroupID)

	fmt.Println("== submitting group B (FAIL_FAST): 1 failure aborts siblings ==")
	gB, err := sched.SubmitGroup(ctx, job.PolicyFailFast, []scheduler.SubmitItem{
		{Name: "b-ok", Spec: sh("echo '  b-ok running'; exit 0", 0, 0, 0)},
		{Name: "b-bad", Spec: sh("echo '  b-bad failing'; exit 1", 0, 0, 0)},
		{Name: "b-slow1", Spec: sh("echo '  b-slow1 running'; sleep 3", 0, 0, 0)},
		{Name: "b-slow2", Spec: sh("echo '  b-slow2 running'; sleep 3", 0, 0, 0)},
	}, true)
	if err != nil {
		return err
	}
	fmt.Printf("group B=%s\n", gB.GroupID)

	// Demonstrate explicit cancel: cancel a member of group A right away
	// (a-ok2 is fast; instead cancel by ID after a tiny delay when it is
	// still pending/running is racy — cancel group B's behavior is shown
	// via fail-fast, so here we cancel a freshly gated held job instead).
	fmt.Println("== submitting held group C then canceling it before open ==")
	gC, err := sched.SubmitGroup(ctx, job.PolicyContinue, []scheduler.SubmitItem{
		{Name: "c-never-runs", Spec: sh("exit 0", 0, 0, 0)},
	}, false)
	if err != nil {
		return err
	}
	if err := sched.CancelGroup(ctx, gC.GroupID); err != nil {
		return err
	}
	fmt.Printf("group C=%s canceled while barrier closed (member stays CANCELED)\n", gC.GroupID)

	fmt.Println("== waiting for groups A and B to finish ==")
	if err := sched.WaitGroup(ctx, gA.GroupID); err != nil {
		return err
	}
	if err := sched.WaitGroup(ctx, gB.GroupID); err != nil {
		return err
	}
	if err := sched.WaitGroup(ctx, gC.GroupID); err != nil {
		return err
	}

	if err := printReport(st); err != nil {
		return err
	}
	fmt.Println()
	fmt.Println("Re-run with --report-only against the same --state-dir: every job")
	fmt.Println("is already terminal, so nothing is executed again (crash-safe).")
	return nil
}

// printReport reads the store and prints the deterministic per-group and
// global statistics plus a compact audit trail.
func printReport(st store.Store) error {
	var jobs []job.Job
	var groups []job.Group
	if err := st.Read(func(v store.View) error {
		jobs = v.Jobs()
		groups = v.Groups()
		return nil
	}); err != nil {
		return err
	}
	rpt := stats.Build(jobs, groups)

	fmt.Println()
	fmt.Println("== final report (deterministic buckets) ==")
	for _, gid := range rpt.GroupIDs {
		g := rpt.Groups[gid]
		fmt.Printf("group %s policy=%s status=%s  ok=%d failed=%d timeout=%d canceled=%d\n",
			gid, g.Policy, g.Status, g.Succeeded, g.Failed, g.TimedOut, g.Canceled)
		for _, jr := range g.Jobs {
			reason := jr.Reason
			if reason == job.ReasonNone {
				reason = "-"
			}
			fmt.Printf("    %-12s %-10s reason=%-18s attempts=%d exit=%d\n",
				jr.Name, jr.Status, reason, jr.Attempts, jr.ExitCode)
		}
	}
	fmt.Printf("TOTAL: succeeded=%d failed=%d timed_out=%d canceled=%d (total terminal=%d, in-flight=%d)\n",
		rpt.All.Succeeded, rpt.All.Failed, rpt.All.TimedOut, rpt.All.Canceled,
		rpt.All.Total(), len(rpt.InFlight))

	fmt.Println()
	fmt.Println("== audit trail (key events per job) ==")
	for _, j := range jobs {
		evs, err := st.Events(j.ID)
		if err != nil {
			return err
		}
		line := ""
		for _, e := range evs {
			if e.Kind == "LEASE_RENEWED" || e.Kind == "ATTEMPT_START" {
				continue
			}
			line += e.Kind + " "
		}
		fmt.Printf("    %-12s %s\n", j.Name, line)
	}
	return nil
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
