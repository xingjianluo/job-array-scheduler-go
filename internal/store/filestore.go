package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// compactionRecords bounds journal growth: once more than this many
// records exist, the journal is atomically replaced by one checkpoint
// record containing the full state.
const compactionRecords = 64

// FileStore is a file-backed Store:
//
//   - LOCK is held with flock(LOCK_EX) for the store's lifetime, so two
//     processes can never mutate the same directory concurrently (a
//     second opener blocks and then takes over on restart/failover).
//   - journal.log is write-ahead: every successful Mutate is one fsynced,
//     checksummed record containing the full post-commit state plus the
//     emitted events. Recovery replays the last good record and truncates
//     any torn tail, so state changes are both recoverable and auditable.
type FileStore struct {
	cfg      Config
	now      func() time.Time
	mu       sync.RWMutex
	dir      string
	lockFile *os.File
	jf       *os.File
	st       *state
	records  int // good records since last compaction
	closed   bool
}

// OpenFileStore opens (creating when absent) the store under cfg.Dir and
// acquires its process-exclusive lock. A torn journal tail left by a
// crashed process is repaired on open.
func OpenFileStore(cfg Config) (*FileStore, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("store: empty Dir")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	fs := &FileStore{cfg: cfg, now: cfg.Now, dir: cfg.Dir}

	lf, err := os.OpenFile(filepath.Join(cfg.Dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// Blocking exclusive lock: a restarted process waits for the old one
	// to release (or the OS releasing the lock when it died).
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		_ = lf.Close()
		return nil, fmt.Errorf("store: flock: %w", err)
	}
	fs.lockFile = lf

	if err := fs.openAndRecoverJournal(); err != nil {
		_ = fs.Close()
		return nil, err
	}
	return fs, nil
}

func (s *FileStore) journalPath() string { return filepath.Join(s.dir, journalName) }

func (s *FileStore) openAndRecoverJournal() error {
	f, err := os.OpenFile(s.journalPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	recs, validEnd, err := scanJournal(f)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("store: scan journal: %w", err)
	}
	// Repair a torn tail (crash mid-write).
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	if fi.Size() > validEnd {
		if err := f.Truncate(validEnd); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
	}
	if len(recs) > 0 {
		last := recs[len(recs)-1]
		s.st = last.State
		if s.st.Groups == nil {
			s.st.Groups = map[string]job.Group{}
		}
		if s.st.Jobs == nil {
			s.st.Jobs = map[string]job.Job{}
		}
	} else {
		s.st = newState()
	}
	s.records = len(recs)
	s.jf = f
	return nil
}

// Read implements Store. fn runs while holding the read lock; it must not
// retain values past its return.
func (s *FileStore) Read(fn func(v View) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return fmt.Errorf("store: closed")
	}
	return fn(viewImpl{st: s.st})
}

// Mutate implements Store as an atomic read-modify-write: fn operates on
// a private clone, so an error leaves the committed state untouched. On
// success the record is journaled (fsync) before the in-memory swap.
func (s *FileStore) Mutate(ctx context.Context, fn MutateFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("store: closed")
	}
	working := s.st.clone()
	tx := &txImpl{viewImpl: viewImpl{st: working}}
	if err := fn(tx); err != nil {
		return err // rollback: working clone is discarded
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	working.Seq++
	for i := range tx.pending {
		tx.pending[i].Seq = int64(len(working.Events)) + 1
		working.Events = append(working.Events, tx.pending[i])
	}

	rec := journalRecord{State: working, Events: tx.pending}
	if err := appendRecord(s.jf, rec); err != nil {
		return fmt.Errorf("store: journal append: %w", err)
	}
	s.st = working
	s.records++
	if s.records > compactionRecords {
		if err := rewriteJournal(s.dir, journalRecord{State: s.st}); err == nil {
			_ = s.jf.Close()
			f, err := os.OpenFile(s.journalPath(), os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return fmt.Errorf("store: reopen journal after compaction: %w", err)
			}
			s.jf = f
			s.records = 1
		}
		// Compaction failure is not fatal: the journal is still valid.
	}
	return nil
}

// Events implements Store.
func (s *FileStore) Events(jobID string) ([]job.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]job.Event, 0)
	for _, e := range s.st.Events {
		if jobID == "" || e.JobID == jobID {
			out = append(out, e)
		}
	}
	return out, nil
}

// Close releases the journal and the cross-process lock.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if s.jf != nil {
		if err := s.jf.Close(); err != nil {
			firstErr = err
		}
	}
	if s.lockFile != nil {
		_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
		if err := s.lockFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
