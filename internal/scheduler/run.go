package scheduler

import (
	"context"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/store"
)

// Run starts worker loops and the stale-lease recoverer and blocks until
// ctx is canceled. In-flight jobs whose context is interrupted by a
// shutdown are NOT forced terminal: their lease expires and the next
// live process requeues them, so a restart never loses or double-counts
// a job.
func (s *Scheduler) Run(ctx context.Context) error {
	// One recoverer goroutine.
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		s.recoverLoop(ctx)
	}()

	// Workers = Slots: each worker claims one job and holds one slot for
	// its whole duration; the slot pool still enforces the bound even if
	// more workers were added later.
	for i := 0; i < s.cfg.Slots; i++ {
		s.runWg.Add(1)
		go func() {
			defer s.runWg.Done()
			s.workerLoop(ctx)
		}()
	}
	// All goroutines are registered before anyone is allowed to Wait on
	// runWg, which avoids an Add/Wait race on a fast-finishing run.
	close(s.startedCh)
	<-ctx.Done()
	return ctx.Err()
}

// started signals once the worker/recoverer goroutines of Run are all
// registered with runWg.
func (s *Scheduler) started() <-chan struct{} { return s.startedCh }

func (s *Scheduler) workerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Acquire capacity before claiming, so a claimed job always has a
		// guaranteed slot (runOne releases it).
		slot, err := s.pool.Acquire(ctx)
		if err != nil {
			return
		}
		cr, err := s.claimNext(ctx)
		if err != nil {
			slot.Release()
			if ctx.Err() != nil {
				return
			}
			// Store hiccup: brief backoff then retry.
			if err := s.sleep(ctx, s.cfg.PollInterval); err != nil {
				return
			}
			continue
		}
		if !cr.ok {
			slot.Release()
			if err := s.sleep(ctx, s.cfg.PollInterval); err != nil {
				return
			}
			continue
		}
		// Ownership of slot passes to runOne.
		s.runOneWithSlot(ctx, cr, slot)
	}
}

// runOneWithSlot executes a claimed job, guaranteeing the slot release is
// paired with the claim (runOne also releases defensively).
func (s *Scheduler) runOneWithSlot(ctx context.Context, cr claimResult, slot *slot) {
	defer slot.Release()
	s.runOne(ctx, cr.job, cr.owner)
}

func (s *Scheduler) recoverLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.RecoverInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = s.RecoverStaleLeases(ctx)
		}
	}
}

// WaitGroup polls until every member of the group is terminal or ctx
// ends.
func (s *Scheduler) WaitGroup(ctx context.Context, groupID string) error {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		done, err := s.groupDone(groupID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

// WaitIdle polls until all known jobs are terminal.
func (s *Scheduler) WaitIdle(ctx context.Context) error {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		allDone := true
		err := s.st.Read(func(v store.View) error {
			for _, j := range v.Jobs() {
				if !j.Status.Terminal() {
					allDone = false
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if allDone {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (s *Scheduler) groupDone(groupID string) (bool, error) {
	done := false
	err := s.st.Read(func(v store.View) error {
		g, ok := v.Group(groupID)
		if !ok {
			return store.ErrNotFound
		}
		if isGroupTerminal(g.Status) {
			done = true
			return nil
		}
		for _, j := range v.JobsOfGroup(groupID) {
			if !j.Status.Terminal() {
				return nil
			}
		}
		done = true
		return nil
	})
	return done, err
}
