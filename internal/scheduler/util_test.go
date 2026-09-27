package scheduler_test

import (
	"context"
	"testing"
	"time"
)

func shutdownCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func nsDur(ns int64) time.Duration { return time.Duration(ns) }
