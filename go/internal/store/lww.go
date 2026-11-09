package store

// LWW ordering shared by every convergence path (store apply, quorum
// reads, anti-entropy, hints, read repair). Mirrors compareLww in
// include/cinder/common/types.hpp: version, then writer hash, then a
// contradictory tombstone bit resolving tombstone-wins so replicas
// converge instead of disagreeing by arrival order.
type LWWOrder int

const (
	LWWLess LWWOrder = iota
	LWWEqual
	LWWGreater
)

// Stamp is the (version, writer, tombstone) identity of an entry.
type Stamp struct {
	Version   uint64
	Writer    uint64
	Tombstone bool
}

func stampOf(e Entry) Stamp {
	return Stamp{Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone}
}

// CompareLWW orders two stamps: version, then writer, then tombstone-wins.
func CompareLWW(a, b Stamp) LWWOrder {
	if a.Version != b.Version {
		if a.Version < b.Version {
			return LWWLess
		}
		return LWWGreater
	}
	if a.Writer != b.Writer {
		if a.Writer < b.Writer {
			return LWWLess
		}
		return LWWGreater
	}
	if a.Tombstone != b.Tombstone {
		if a.Tombstone {
			return LWWGreater
		}
		return LWWLess
	}
	return LWWEqual
}

// LWWWinsOver reports whether incoming strictly beats resident (store
// apply, read reduction, need/fetch derivation).
func LWWWinsOver(incoming, resident Stamp) bool {
	return CompareLWW(incoming, resident) == LWWGreater
}

// LWWCovers reports greater-or-equal (idempotent refresh decisions: an
// identical stamp refreshes TTL without changing the outcome).
func LWWCovers(incoming, resident Stamp) bool {
	return CompareLWW(incoming, resident) != LWWLess
}
