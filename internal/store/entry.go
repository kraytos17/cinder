package store

import (
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
)

// Entry is a cache value with LWW version metadata, an optional expiry,
// and an optional tombstone. VersionAndTTL packs the version and flags
// into the identity used by digests and IBLT sketches.
type Entry struct {
	Value          []byte
	Version        uint64
	WriterNodeHash uint64
	// Expiry is the TTL deadline; zero means none. One field replaces
	// the old HasTTL+ExpiresAt pair, which could desync (an instant
	// without its flag never expired).
	Expiry    expiry.Expiry
	Tombstone bool
}

// VersionAndTTL packs the entry identity: bit 0 indicates expiry, bit 1
// indicates tombstone, and bits 2-63 contain the version.
func (e Entry) VersionAndTTL() uint64 {
	v := e.Version << 2
	if !e.Expiry.IsNone() {
		v |= 1
	}
	if e.Tombstone {
		v |= 2
	}
	return v
}

// Expired reports whether e is past its deadline at now. Entries without
// an expiry never expire.
func (e Entry) Expired(now time.Time) bool {
	return e.Expiry.Expired(now)
}

// Clone deep-copies the value bytes.
func (e Entry) Clone() Entry {
	out := e
	if e.Value != nil {
		out.Value = append([]byte(nil), e.Value...)
	}
	return out
}
