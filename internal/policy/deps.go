package policy

import (
	"fmt"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// ValidateDeps checks the dependency declarations of a batch of job specs
// against the names already present in the group (existing may be empty for
// a one-shot SubmitGroup). It is a pure function: the same declarations
// always produce the same verdict, which is what makes an invalid batch
// rejectable up front with nothing left behind.
//
// Rules (evaluated only when at least one spec declares dependencies):
//
//   - every member name in the combined universe (existing + batch) must be
//     unique and non-empty, so a dependency reference is unambiguous;
//   - every DependsOn entry must name a member of that universe;
//   - no self-dependency;
//   - the dependency graph must be acyclic. Edges only ever point from a
//     new spec into the (already acyclic) existing graph or within the new
//     batch, so a cycle — if any — is entirely inside the new batch and a
//     DFS over the batch finds it.
func ValidateDeps(existing []string, specs []domain.JobSpec) error {
	hasDeps := false
	for _, s := range specs {
		if len(s.DependsOn) > 0 {
			hasDeps = true
			break
		}
	}
	if !hasDeps {
		return nil
	}

	universe := map[string]bool{}
	for _, n := range existing {
		universe[n] = true
	}
	for _, s := range specs {
		if s.Name == "" {
			return fmt.Errorf("%w: dependency declarations require non-empty member names", domain.ErrConflict)
		}
		if universe[s.Name] {
			return fmt.Errorf("%w: duplicate member name %q with dependencies in play", domain.ErrConflict, s.Name)
		}
		universe[s.Name] = true
	}
	for _, n := range existing {
		if n == "" {
			return fmt.Errorf("%w: dependency declarations require non-empty member names", domain.ErrConflict)
		}
	}

	edges := map[string][]string{}
	for _, s := range specs {
		for _, dep := range s.DependsOn {
			if dep == s.Name {
				return fmt.Errorf("%w: member %q depends on itself", domain.ErrConflict, s.Name)
			}
			if !universe[dep] {
				return fmt.Errorf("%w: member %q depends on unknown member %q", domain.ErrConflict, s.Name, dep)
			}
			edges[s.Name] = append(edges[s.Name], dep)
		}
	}

	// Cycle detection (DFS with white/gray/black coloring) over the new
	// batch; edges into existing members terminate the traversal because
	// the pre-existing graph is acyclic by induction.
	const (
		white = 0 // unvisited
		gray  = 1 // on the current path
		black = 2 // fully explored
	)
	color := map[string]int{}
	var visit func(n string) error
	visit = func(n string) error {
		color[n] = gray
		for _, dep := range edges[n] {
			switch color[dep] {
			case gray:
				return fmt.Errorf("%w: dependency cycle involving %q and %q", domain.ErrConflict, n, dep)
			case white:
				if err := visit(dep); err != nil {
					return err
				}
			}
		}
		color[n] = black
		return nil
	}
	for _, s := range specs {
		if color[s.Name] == white {
			if err := visit(s.Name); err != nil {
				return err
			}
		}
	}
	return nil
}
