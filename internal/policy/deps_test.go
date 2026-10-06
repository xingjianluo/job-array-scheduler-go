package policy_test

import (
	"strings"
	"testing"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
	"github.com/xingjianluo/job-array-scheduler-go/internal/policy"
)

func spec(name string, deps ...string) domain.JobSpec {
	return domain.JobSpec{Name: name, Task: domain.TaskSpec{Kind: "ok"}, DependsOn: deps}
}

// TestValidateDeps pins the pure admission rules for dependency
// declarations: the same input always yields the same verdict.
func TestValidateDeps(t *testing.T) {
	cases := []struct {
		name     string
		existing []string
		specs    []domain.JobSpec
		wantErr  string // "" means accepted
	}{
		{"no-deps-anything-goes", nil,
			[]domain.JobSpec{spec("a"), spec("a"), spec("")}, ""},
		{"linear-chain", nil,
			[]domain.JobSpec{spec("a"), spec("b", "a"), spec("c", "b")}, ""},
		{"diamond", nil,
			[]domain.JobSpec{spec("a"), spec("b", "a"), spec("c", "a"), spec("d", "b", "c")}, ""},
		{"dep-on-existing-member", []string{"old"},
			[]domain.JobSpec{spec("new", "old")}, ""},
		{"unknown-dep", nil,
			[]domain.JobSpec{spec("a", "ghost")}, "unknown member"},
		{"self-dep", nil,
			[]domain.JobSpec{spec("a", "a")}, "itself"},
		{"dup-name-in-batch", nil,
			[]domain.JobSpec{spec("a"), spec("a", "a")}, "duplicate"},
		{"dup-with-existing", []string{"a"},
			[]domain.JobSpec{spec("a", "a")}, "duplicate"},
		{"empty-name-with-deps", nil,
			[]domain.JobSpec{spec(""), spec("b", "a")}, "non-empty"},
		{"cycle-2", nil,
			[]domain.JobSpec{spec("a", "b"), spec("b", "a")}, "cycle"},
		{"cycle-3", nil,
			[]domain.JobSpec{spec("a", "c"), spec("b", "a"), spec("c", "b")}, "cycle"},
		{"cycle-only-in-new-batch", []string{"root"},
			[]domain.JobSpec{spec("x", "root", "y"), spec("y", "x")}, "cycle"},
	}
	for _, tc := range cases {
		err := policy.ValidateDeps(tc.existing, tc.specs)
		if tc.wantErr == "" {
			if err != nil {
				t.Fatalf("JUDGMENT: case %q rejected: %v, want accept", tc.name, err)
			}
			t.Logf("JUDGMENT: case %-22q accepted (input existing=%v specs=%d)", tc.name, tc.existing, len(tc.specs))
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("JUDGMENT: case %q err=%v, want substring %q", tc.name, err, tc.wantErr)
		}
		t.Logf("JUDGMENT: case %-22q rejected (%v)", tc.name, err)
	}
}
