package executor

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// markerSleep compiles a tiny uniquely named program that sleeps forever,
// so a leftover-process check with `pgrep -x <name>` cannot match the
// checker command line itself.
func markerSleep(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	dst := filepath.Join(dir, name)
	out, err := exec.Command("go", "build", "-o", dst, "marker.go").CombinedOutput()
	if err != nil {
		t.Fatalf("build marker: %v\n%s", err, out)
	}
	return dst
}

func shell(script string) job.Spec {
	return job.Spec{Command: "sh", Args: []string{"-c", script}}
}

func TestRunSuccess(t *testing.T) {
	e := New("")
	o := e.Run(context.Background(), shell("echo hello; exit 0"))
	t.Logf("input=echo hello kind=%s exit=%d reason=%q stdout=%q", o.Kind, o.ExitCode, o.Reason, o.Stdout)
	if o.Kind != OutcomeSucceeded || o.ExitCode != 0 || o.Reason != job.ReasonNone {
		t.Fatalf("unexpected outcome: %+v", o)
	}
}

func TestRunNonZeroExit(t *testing.T) {
	e := New("")
	o := e.Run(context.Background(), shell("echo boom >&2; exit 7"))
	t.Logf("input=exit7 kind=%s exit=%d reason=%q stderr=%q", o.Kind, o.ExitCode, o.Reason, o.Stderr)
	if o.Kind != OutcomeFailed || o.ExitCode != 7 || o.Reason != job.ReasonExitNonZero {
		t.Fatalf("unexpected outcome: %+v", o)
	}
}

func TestRunSpawnFailure(t *testing.T) {
	e := New("")
	o := e.Run(context.Background(), job.Spec{Command: "/no/such/binary-xyz"})
	t.Logf("input=missing-binary kind=%s reason=%q", o.Kind, o.Reason)
	if o.Kind != OutcomeSpawn || o.Reason != job.ReasonSpawnFailure {
		t.Fatalf("unexpected outcome: %+v", o)
	}
}

func TestRunTimeoutKillsProcessGroup(t *testing.T) {
	sleepBin := markerSleep(t, "slptest1")
	e := New("")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	// Child spawns a grandchild; both must die with the process group.
	start := time.Now()
	o := e.Run(ctx, shell("\""+sleepBin+"\" 30 & \""+sleepBin+"\" 30"))
	elapsed := time.Since(start)
	t.Logf("input=2x sleep30 kind=%s reason=%q elapsed=%v", o.Kind, o.Reason, elapsed)
	if o.Kind != OutcomeTimeout || o.Reason != job.ReasonTimeout {
		t.Fatalf("unexpected outcome: %+v", o)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("timeout was not enforced, elapsed=%v", elapsed)
	}
	out := runShell(t, "pgrep", "-x", "slptest1")
	t.Logf("leftover marker-sleep processes after kill: %q", out)
	if out != "" {
		t.Fatalf("process group leaked, pgrep output=%q", out)
	}
}

func TestRunCanceled(t *testing.T) {
	e := New("")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	o := e.Run(ctx, shell("sleep 2"))
	t.Logf("input=sleep2+cancel kind=%s reason=%q", o.Kind, o.Reason)
	if o.Kind != OutcomeCanceled || o.Reason != job.ReasonUserCanceled {
		t.Fatalf("unexpected outcome: %+v", o)
	}
}

func TestPreCanceledContext(t *testing.T) {
	e := New("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	o := e.Run(ctx, shell("sleep 1"))
	t.Logf("input=pre-canceled kind=%s reason=%q", o.Kind, o.Reason)
	if o.Kind != OutcomeCanceled {
		t.Fatalf("unexpected outcome: %+v", o)
	}
}
