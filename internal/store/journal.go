package store

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/xingjianluo/job-array-scheduler-go/internal/job"
)

// Journal on-disk format: one record per line,
//
//	<8 hex crc32c-lowercase><json-payload>\n
//
// The checksum makes torn writes after a crash detectable. On open the
// file is scanned forward; at the first corrupt/short line the file is
// truncated there, so records are never read half-written.
const (
	journalName = "journal.log"
	hexLen      = 8
)

// journalRecord is the payload that is checksummed. State carries the
// full post-commit snapshot; events are the transitions the commit made.
type journalRecord struct {
	State  *state      `json:"state"`
	Events []job.Event `json:"events"`
}

func encodeRecord(rec journalRecord) ([]byte, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	sum := crc32.ChecksumIEEE(payload)
	line := make([]byte, 0, hexLen+len(payload)+1)
	line = append(line, fmt.Sprintf("%08x", sum)...)
	line = append(line, payload...)
	line = append(line, '\n')
	return line, nil
}

// scanJournal reads every good record from r. It returns the records in
// order and validEnd (number of bytes up to and including the last good
// line). A trailing partial/corrupt line is ignored by the caller which
// truncates at validEnd.
func scanJournal(r io.Reader) (records []journalRecord, validEnd int64, err error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var offset int64
	for {
		raw, readErr := br.ReadBytes('\n')
		if len(raw) > 0 {
			if verifyRecord(raw) {
				var rec journalRecord
				if jerr := json.Unmarshal(raw[hexLen:len(raw)-1], &rec); jerr == nil && rec.State != nil {
					records = append(records, rec)
					offset += int64(len(raw))
					validEnd = offset
				} else {
					// Corrupt payload: stop here.
					return records, validEnd, nil
				}
			} else {
				// Checksum/structure mismatch: stop here.
				return records, validEnd, nil
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return records, validEnd, nil
			}
			return records, validEnd, readErr
		}
	}
}

func verifyRecord(line []byte) bool {
	if len(line) < hexLen+2 || line[len(line)-1] != '\n' {
		return false
	}
	hexPart := line[:hexLen]
	payload := line[hexLen : len(line)-1]
	if _, err := hex.DecodeString(string(hexPart)); err != nil {
		return false
	}
	var want uint32
	for _, c := range hexPart {
		var v byte
		switch {
		case c >= '0' && c <= '9':
			v = c - '0'
		case c >= 'a' && c <= 'f':
			v = c - 'a' + 10
		default:
			return false
		}
		want = want<<4 | uint32(v)
	}
	return crc32.ChecksumIEEE(payload) == want
}

// appendRecord appends one encoded record and forces it to stable
// storage before returning.
func appendRecord(f *os.File, rec journalRecord) error {
	line, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	if _, err := f.Write(line); err != nil {
		return err
	}
	return f.Sync()
}

// rewriteJournal atomically replaces the journal with a single
// checkpoint record (compaction).
func rewriteJournal(dir string, rec journalRecord) error {
	tmp, err := os.CreateTemp(dir, ".journal-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmp != nil {
			tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	bw := bufio.NewWriter(tmp)
	line, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	if _, err := bw.Write(line); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	tmp = nil
	if err := os.Rename(tmpName, dir+"/"+journalName); err != nil {
		return err
	}
	// fsync the directory so the rename itself is durable.
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// decodeLastState is a small helper used by recovery tooling/tests.
func decodeLastState(r io.Reader) (*state, []job.Event, error) {
	recs, _, err := scanJournal(r)
	if err != nil {
		return nil, nil, err
	}
	if len(recs) == 0 {
		return nil, nil, nil
	}
	last := recs[len(recs)-1]
	return last.State, last.Events, nil
}
