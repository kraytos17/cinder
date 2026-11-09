package store

import (
	"time"
)

// Entry is a versioned cache record. Mirrors VersionedEntry in
// include/cinder/common/types.hpp, with the bit-packing unpacked into
// explicit fields. VersionAndTTL computes the packed identity
// (version<<2 | has_ttl | tombstone<<1) the C++ side stores, so digests
// and IBLT sketches stay compatible.
type Entry struct {
	Value          []byte
	Version        uint64
	WriterNodeHash uint64
	HasTTL         bool
	ExpiresAt      time.Time
	Tombstone      bool
}

// VersionAndTTL packs (version, flags) exactly like the C++
// version_and_ttl field: bit 0 = has_ttl, bit 1 = tombstone,
// bits 2-63 = version.
func (e Entry) VersionAndTTL() uint64 {
	v := e.Version << 2
	if e.HasTTL {
		v |= 1
	}
	if e.Tombstone {
		v |= 2
	}
	return v
}

// Expired reports whether e is past its deadline at now. Entries without
// a TTL never expire.
func (e Entry) Expired(now time.Time) bool {
	return e.HasTTL && !e.ExpiresAt.After(now)
}

// Clone deep-copies the value bytes.
func (e Entry) Clone() Entry {
	out := e
	if e.Value != nil {
		out.Value = append([]byte(nil), e.Value...)
	}
	return out
}
