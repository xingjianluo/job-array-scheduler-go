// Package store provides durable, concurrency-safe storage for the
// scheduler's state machine.
//
// Durability model (single machine, no external services):
//
//   - state.json: canonical state (groups, jobs, ID counters, event
//     sequence). It is rewritten atomically (write tmp + fsync + rename)
//     after every committed state transition, so a crash at any instant
//     leaves a complete, consistent snapshot;
//   - events.log: append-only JSON-lines audit trail. It is fsynced on
//     every append and is the recoverable, explainable history of every
//     decision ("who claimed what with which token", "why was this job
//     canceled", "why did the group become FAILED").
//
// Both files are written while holding the single store mutex, so the
// snapshot and log always describe the same committed prefix. On reopening,
// state is taken from the snapshot and any event lines beyond the snapshot's
// sequence are replayed on top.
package store

import (
	"context"
	"sync"
	"time"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// Outcome is the result reported by a worker when an attempt finishes.
type Outcome struct {
	State    domain.JobState // SUCCEEDED / FAILED / TIMED_OUT / CANCELED
	Reason   string
	Message  string
	Output   string
	WorkerID string // filled from the validated lease on commit
}

// Store is the persistence/coordination boundary between the scheduler and
// workers. All methods are safe for concurrent use.
type Store interface {
	Close() error

	// Submission
	CreateGroup(spec domain.GroupSpec) (*domain.Group, error)
	AddJobs(groupID string, specs []domain.JobSpec) ([]*domain.Job, error)
	GetGroup(id string) (*domain.Group, error)
	ListGroups() []*domain.Group

	// Admission / rejection (driven by the scheduler loop).
	TryAdmit() (*domain.Group, bool, error)
	RejectGroup(groupID, reason, message string) error

	// Worker claim / heartbeat / completion with fencing tokens.
	ClaimNext(workerID string, now time.Time) (*domain.Job, *domain.Attempt, error)
	Heartbeat(jobID, workerID string, token int64) error
	FinishAttempt(jobID, workerID string, token int64, out Outcome) error
	CancelRequested(jobID, workerID string, token int64) bool
	// CancelReason returns (reason, true) when the leased job carries a
	// pending cancel request, so the worker reports the distinguishable cause
	// (user_canceled vs group_failfast).
	CancelReason(jobID, workerID string, token int64) (string, bool)

	// Crash recovery: requeue attempts whose lease has expired.
	RecoverExpiredLeases(now time.Time) ([]string, error)

	// Cancellation.
	CancelJob(jobID, reason string) error
	CancelGroup(groupID, reason string) error

	// Reads / audit.
	GetJob(id string) (*domain.Job, error)
	ListJobs(groupID string) []*domain.Job
	Events() []domain.Event
	Watch(ctx context.Context) <-chan domain.Event
}

// snapshot is the on-disk canonical state.
type snapshot struct {
	NextEventSeq int64                    `json:"next_event_seq"`
	NextToken    int64                    `json:"next_token"`
	NextGroupNum int                      `json:"next_group_num"`
	NextJobNum   int                      `json:"next_job_num"`
	Groups       map[string]*domain.Group `json:"groups"`
	Jobs         map[string]*domain.Job   `json:"jobs"`
	JobOrder     []string                 `json:"job_order"` // FIFO claim order
}

// FileStore is the JSON-snapshot + event-log implementation.
type FileStore struct {
	mu       sync.Mutex
	dir      string
	cfg      domain.Config
	snap     snapshot
	logFile  string
	watchers map[chan domain.Event]struct{}
	closed   bool
}

// Open (re)opens a store rooted at dir, recovering any durable state.
func Open(dir string, cfg domain.Config) (*FileStore, error) {
	s := &FileStore{
		dir: dir,
		cfg: cfg,
		snap: snapshot{
			NextEventSeq: 1,
			NextToken:    1,
			Groups:       map[string]*domain.Group{},
			Jobs:         map[string]*domain.Job{},
		},
		logFile:  dir + "/events.log",
		watchers: map[chan domain.Event]struct{}{},
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func defaultRetryOf(cfg domain.Config) domain.RetryPolicy {
	r := cfg.DefaultRetry
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = 1
	}
	return r
}

func (s *FileStore) defaultRetryLocked() domain.RetryPolicy {
	return defaultRetryOf(s.cfg)
}

func (s *FileStore) defaultJobTimeoutLocked() time.Duration {
	if s.cfg.DefaultJobTimeout > 0 {
		return s.cfg.DefaultJobTimeout
	}
	return 5 * time.Second
}

// Close releases resources (the store is otherwise fully synchronous).
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for ch := range s.watchers {
		close(ch)
	}
	s.watchers = nil
	return nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

func (s *FileStore) GetGroup(id string) (*domain.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.snap.Groups[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cloneGroup(g), nil
}

func (s *FileStore) GetJob(id string) (*domain.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.snap.Jobs[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cloneJob(j), nil
}

func (s *FileStore) ListGroups() []*domain.Group {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*domain.Group, 0, len(s.snap.Groups))
	for _, g := range s.snap.Groups {
		out = append(out, cloneGroup(g))
	}
	return out
}

func (s *FileStore) ListJobs(groupID string) []*domain.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.snap.Groups[groupID]
	if g == nil {
		return nil
	}
	out := make([]*domain.Job, 0, len(g.JobIDs))
	for _, id := range g.JobIDs {
		if j := s.snap.Jobs[id]; j != nil {
			out = append(out, cloneJob(j))
		}
	}
	return out
}

func (s *FileStore) Events() []domain.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return readEventsLocked(s.dir)
}

// Watch returns a buffered channel that receives every future event until
// the context ends or the store closes.
func (s *FileStore) Watch(ctx context.Context) <-chan domain.Event {
	ch := make(chan domain.Event, 256)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(ch)
		return ch
	}
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		if _, ok := s.watchers[ch]; ok {
			delete(s.watchers, ch)
			close(ch)
		}
		s.mu.Unlock()
	}()
	return ch
}
