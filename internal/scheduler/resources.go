package scheduler

import "context"

// slots models the cluster capacity as a fixed pool of execution tokens:
// Acquire blocks until a token is free or ctx expires, and every claimed
// token is returned exactly once by Release. Running jobs inside
// `defer slot.Release()` is what guarantees timeout/cancel paths give
// their capacity back.
type slots struct {
	tokens chan struct{}
	size   int
}

func newSlots(n int) *slots {
	if n <= 0 {
		n = 1
	}
	p := &slots{tokens: make(chan struct{}, n), size: n}
	for i := 0; i < n; i++ {
		p.tokens <- struct{}{}
	}
	return p
}

// slot is one acquired capacity token.
type slot struct {
	p *slots
	// released makes Release idempotent so a normal finish and a cancel
	// path can both call it without returning a token twice.
	released bool
}

// Acquire blocks for capacity. The returned error is ctx.Err() only.
func (p *slots) Acquire(ctx context.Context) (*slot, error) {
	select {
	case <-p.tokens:
		return &slot{p: p}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TryAcquire takes a token immediately or reports none available.
func (p *slots) TryAcquire() (*slot, bool) {
	select {
	case <-p.tokens:
		return &slot{p: p}, true
	default:
		return nil, false
	}
}

func (s *slot) Release() {
	if s == nil || s.released {
		return
	}
	s.released = true
	s.p.tokens <- struct{}{}
}

// free reports how many tokens are currently available (for logs/tests).
func (p *slots) free() int { return len(p.tokens) }
