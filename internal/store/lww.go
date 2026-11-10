package store

// LWWOrder orders two stamps: less, equal, or greater. It is shared by
// every convergence path (store apply, quorum reads, anti-entropy,
// hints, read repair), mirroring compareLww in
// include/cinder/common/types.hpp: version, then writer hash, then a
// contradictory tombstone bit resolving tombstone-wins so replicas
// converge instead of disagreeing by arrival order.
type LWWOrder int

const (
	// LWWLess orders the incoming stamp strictly before the resident.
	LWWLess LWWOrder = iota
	// LWWEqual orders identical stamps (idempotent refresh, no outcome change).
	LWWEqual
	// LWWGreater orders the incoming stamp strictly after the resident.
	LWWGreater
)

// String renders the order for logs and test failures.
func (o LWWOrder) String() string {
	switch o {
	case LWWLess:
		return "less"
	case LWWEqual:
		return "equal"
	default:
		return "greater"
	}
}

// Stamp is the (version, writer, tombstone) identity of an entry.
type Stamp struct {
	Version   uint64
	Writer    uint64
	Tombstone bool
}

// Stamp returns the LWW identity of the entry: the single canonical
// conversion from stored entry to comparison currency, so callers never
// hand-map Version/WriterNodeHash/Tombstone (a transposition would
// silently corrupt ordering, and every field is uint64/bool).
func (e Entry) Stamp() Stamp {
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
