package store

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/xingjianluo/job-array-scheduler-go/internal/domain"
)

// load reads state.json (atomic snapshot) and then replays any event-log
// lines with sequence numbers beyond the snapshot, recovering after a crash
// that happened between the log append and the next snapshot write.
func (s *FileStore) load() error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("store: mkdir data dir: %w", err)
	}

	if data, err := os.ReadFile(filepath.Join(s.dir, "state.json")); err == nil {
		if err := json.Unmarshal(data, &s.snap); err != nil {
			return fmt.Errorf("store: corrupt state.json: %w", err)
		}
		if s.snap.Groups == nil {
			s.snap.Groups = map[string]*domain.Group{}
		}
		if s.snap.Jobs == nil {
			s.snap.Jobs = map[string]*domain.Job{}
		}
		if s.snap.NextEventSeq == 0 {
			s.snap.NextEventSeq = 1
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	// Replay log tail beyond the snapshot.
	if f, err := os.Open(s.logFile); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		for sc.Scan() {
			line := sc.Bytes()
			if len(line) == 0 {
				continue
			}
			var ev domain.Event
			if err := json.Unmarshal(line, &ev); err != nil {
				// A torn append at the tail: stop replay, keep the prefix.
				break
			}
			if ev.Seq >= s.snap.NextEventSeq {
				s.applyEvent(ev)
			}
		}
		_ = f.Close()
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// persistLocked atomically rewrites state.json. Caller MUST hold s.mu.
// Order per committed transition: append+fsync event log first, then
// rewrite+fsync snapshot, so the snapshot never lags behind a durable event
// by more than one transition (and replay closes even that gap).
func (s *FileStore) persistLocked() error {
	data, err := json.MarshalIndent(s.snap, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, "state.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := fsyncPath(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return fsyncPath(s.dir)
}

// emitLocked assigns a sequence number, appends the event to the durable log
// (fsync), applies it to in-memory state, fans it out to watchers and
// rewrites the snapshot. Caller MUST hold s.mu.
func (s *FileStore) emitLocked(ev domain.Event) {
	ev.Seq = s.snap.NextEventSeq
	s.snap.NextEventSeq++
	if ev.Time.IsZero() {
		ev.Time = nowTime()
	}

	if err := appendEvent(s.logFile, ev); err != nil {
		// With local disk a write failure means durability is lost; surface
		// it loudly instead of silently drifting.
		panic(fmt.Sprintf("store: cannot append event log: %v", err))
	}
	s.applyEvent(ev)

	for ch := range s.watchers {
		select {
		case ch <- ev:
		default:
			// Slow watcher must not block the state machine; drop instead.
		}
	}
	if err := s.persistLocked(); err != nil {
		panic(fmt.Sprintf("store: cannot persist snapshot: %v", err))
	}
}

// emitNoSnapLocked appends a high-volume event (heartbeats) without rewriting
// the snapshot each time. Heartbeats live only in the audit log; the snapshot
// still carries the latest heartbeat from the next state transition or the
// store also persists heartbeats in state via periodic flush (see Flush).
func (s *FileStore) emitNoSnapLocked(ev domain.Event) {
	ev.Seq = s.snap.NextEventSeq
	s.snap.NextEventSeq++
	if ev.Time.IsZero() {
		ev.Time = nowTime()
	}
	_ = appendEvent(s.logFile, ev)
	for ch := range s.watchers {
		select {
		case ch <- ev:
		default:
		}
	}
}

// Flush rewrites the snapshot (used after heartbeat-only batches).
func (s *FileStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persistLocked()
}

func appendEvent(path string, ev domain.Event) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	data, err := json.Marshal(ev)
	if err != nil {
		_ = f.Close()
		return err
	}
	data = append(data, '\n')
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func fsyncPath(p string) error {
	f, err := os.Open(p)
	if err != nil {
		return nil // directory open may fail on some platforms; rename still safe
	}
	err = f.Sync()
	_ = f.Close()
	return err
}

// readEventsLocked returns all events recorded in the log. Caller holds mu
// only to serialize with emit; reading is from disk so test assertions see
// exactly what is durable.
func readEventsLocked(dir string) []domain.Event {
	f, err := os.Open(filepath.Join(dir, "events.log"))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []domain.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 8<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var ev domain.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			break
		}
		out = append(out, ev)
	}
	return out
}

// clone helpers return deep copies so callers can never mutate durable state
// without going through a Store method (the fencing/state-machine guarantee).

func cloneGroup(g *domain.Group) *domain.Group {
	if g == nil {
		return nil
	}
	c := *g
	c.JobIDs = append([]string(nil), g.JobIDs...)
	return &c
}

func cloneJob(j *domain.Job) *domain.Job {
	if j == nil {
		return nil
	}
	c := *j
	if len(j.Attempts) > 0 {
		c.Attempts = make([]*domain.Attempt, len(j.Attempts))
		for i, a := range j.Attempts {
			ac := *a
			c.Attempts[i] = &ac
		}
	}
	return &c
}
