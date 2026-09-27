package executor

import (
	"os/exec"
	"testing"
)

func runShell(t *testing.T, name string, args ...string) string {
	t.Helper()
	b, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		// pgrep exits 1 when nothing matched, which is the success case.
		t.Logf("check command %s %s -> err=%v out=%q", name, args, err, string(b))
	}
	return string(b)
}
