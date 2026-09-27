package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

func testStore(t *testing.T) (*FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := OpenFileStore(Config{Dir: dir, Now: time.Now})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return st, dir
}

func TestMutateCommitAndRollback(t *testing.T) {
	st, _ := testStore(t)
	defer st.Close()
	ctx := context.Background()

	now := time.Unix(1700000000, 0)
	err := st.Mutate(ctx, func(tx Tx) error {
		tx.InsertGroup(job.Group{ID: "grp-1", Policy: job.PolicyFailFast, Status: job.GroupPending, CreatedAt: now, UpdatedAt: now})
		tx.InsertJob(job.Job{ID: "job-1", GroupID: "grp-1", Status: job.StatusPending, ExitCode: -1, CreatedAt: now, UpdatedAt: now})
		tx.Emit(job.Event{Time: now, GroupID: "grp-1", JobID: "job-1", Kind: "SUBMITTED", To: job.StatusPending})
		return nil
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Rollback must not change committed state.
	wantErr := errors.New("boom")
	if err := st.Mutate(ctx, func(tx Tx) error {
		j, _ := tx.Job("job-1")
		j.Status = job.StatusFailed
		tx.UpdateJob(j)
		return wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("want %v got %v", wantErr, err)
	}

	if err := st.Read(func(v View) error {
		j, ok := v.Job("job-1")
		if !ok || j.Status != job.StatusPending {
			t.Fatalf("rollback leaked, status=%v ok=%v", j.Status, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	evs, err := st.Events("")
	if err != nil || len(evs) != 1 || evs[0].Seq != 1 || evs[0].Kind != "SUBMITTED" {
		t.Fatalf("events = %+v, err=%v", evs, err)
	}
}

func TestDurabilityAfterReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenFileStore(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		id := "job-" + string(rune('a'+i))
		if err := st.Mutate(ctx, func(tx Tx) error {
			tx.InsertJob(job.Job{ID: id, Status: job.StatusReady, ExitCode: -1})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenFileStore(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if err := st2.Read(func(v View) error {
		if jobs := v.Jobs(); len(jobs) != 5 {
			t.Fatalf("recovered jobs=%d want 5", len(jobs))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTornJournalRepaired(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenFileStore(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Mutate(context.Background(), func(tx Tx) error {
		tx.InsertJob(job.Job{ID: "job-ok", Status: job.StatusReady, ExitCode: -1})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Append garbage as a crashed partial record.
	f, err := os.OpenFile(filepath.Join(dir, journalName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("deadbeef{\"torn\"\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := OpenFileStore(Config{Dir: dir})
	if err != nil {
		t.Fatalf("reopen after torn write: %v", err)
	}
	defer st2.Close()
	if err := st2.Read(func(v View) error {
		if _, ok := v.Job("job-ok"); !ok {
			t.Fatal("committed job lost after torn-tail recovery")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A new commit after repair must succeed.
	if err := st2.Mutate(context.Background(), func(tx Tx) error {
		tx.InsertJob(job.Job{ID: "job-after", Status: job.StatusReady, ExitCode: -1})
		return nil
	}); err != nil {
		t.Fatalf("commit after repair: %v", err)
	}
}

func TestMutateSerializesConcurrentWriters(t *testing.T) {
	st, _ := testStore(t)
	defer st.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				id := job.NewID("j")
				if err := st.Mutate(context.Background(), func(tx Tx) error {
					tx.InsertJob(job.Job{ID: id, Status: job.StatusReady, ExitCode: -1})
					return nil
				}); err != nil {
					t.Errorf("mutate: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if err := st.Read(func(v View) error {
		if n := len(v.Jobs()); n != 160 {
			t.Fatalf("jobs=%d want 160 (no lost update)", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
