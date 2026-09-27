package job

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"
)

// NewID returns a sortable, collision-resistant identifier with the given
// prefix (e.g. "grp", "job"). The timestamp prefix makes submission order
// visible when IDs are listed lexicographically.
func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// rand.Read failing is not recoverable in any useful way; fall
		// back to the current time rather than returning an error through
		// every call site.
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 16) + "-" + hex.EncodeToString(b[:])
}
