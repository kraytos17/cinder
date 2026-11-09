// Package counter implements state-based CRDT counters (Go port of
// cinder/store/crdt_counter.hpp): GCounter and PNCounter with merge-based
// convergence. All types are pure in-memory math (no I/O, no clocks, no
// locks); storage, replication, and anti-entropy apply merge() explicitly
// so concurrent updates survive regardless of delivery order — LWW would
// drop concurrent increments.
//
// Actor identity is the caller's responsibility: pass a stable per-node ID.
// A restart that mints a fresh identity orphans its old components, growing
// state without reusing it.
package counter

import (
	"errors"
	"maps"
	"math"
	"slices"
)

// ErrInvalidActor rejects empty actor ids: identity must be attributable.
var ErrInvalidActor = errors.New("counter: empty actor")

// ErrOverflow reports a component increment that would wrap uint64. The
// store maps it to.ErrCapacityExceeded, mirroring C++.
var ErrOverflow = errors.New("counter: component overflow")

// GCounter is a grow-only counter: one monotonic component per actor, value
// is the saturating sum, merge takes the pointwise maximum (commutative,
// associative, idempotent).
type GCounter struct {
	components map[string]uint64
}

// Increment adds delta to actor's component. Empty actor is rejected,
// delta 0 is a no-op (returns nil without touching the map, so a bare read
// never allocates), and a would-be overflow fails without mutating anything.
func (g *GCounter) Increment(actor string, delta uint64) error {
	if actor == "" {
		return ErrInvalidActor
	}
	if delta == 0 {
		return nil
	}
	cur := g.components[actor]
	if delta > math.MaxUint64-cur {
		return ErrOverflow
	}
	if g.components == nil {
		g.components = make(map[string]uint64)
	}
	g.components[actor] = cur + delta
	return nil
}

// Merge takes the pointwise maximum: every component keeps the highest
// value seen from either side.
func (g *GCounter) Merge(other GCounter) {
	if len(other.components) == 0 {
		return
	}
	if g.components == nil {
		g.components = make(map[string]uint64, len(other.components))
	}
	for actor, count := range other.components {
		if count > g.components[actor] {
			g.components[actor] = count
		}
	}
}

// Value returns the sum over actors, saturating at math.MaxUint64.
func (g GCounter) Value() uint64 {
	var total uint64
	for _, count := range g.components {
		if count > math.MaxUint64-total {
			return math.MaxUint64
		}
		total += count
	}
	return total
}

// Component returns actor's count, 0 when the actor never incremented.
func (g GCounter) Component(actor string) uint64 {
	return g.components[actor]
}

// Actors returns the sorted actor list (wire order is deterministic).
func (g GCounter) Actors() []string {
	out := make([]string, 0, len(g.components))
	for actor := range g.components {
		out = append(out, actor)
	}
	slices.Sort(out)
	return out
}

// Equal reports identical component maps.
func (g GCounter) Equal(other GCounter) bool {
	if len(g.components) != len(other.components) {
		return false
	}
	for actor, count := range g.components {
		if other.components[actor] != count {
			return false
		}
	}
	return true
}

// Clone deep-copies the component map. Struct copies share maps, so
// every store insert/read path clones (mirroring C++ std::map copy
// semantics); without this, Merge would mutate the resident before the
// capacity check and growth would always compute as zero.
func (g GCounter) Clone() GCounter {
	if len(g.components) == 0 {
		return GCounter{}
	}
	out := make(map[string]uint64, len(g.components))
	maps.Copy(out, g.components)
	return GCounter{components: out}
}

// Empty reports no components at all.
func (g GCounter) Empty() bool {
	return len(g.components) == 0
}

// PNCounter is an increment/decrement counter built from two GCounters:
// increments accumulate in P, decrements in N, value is sum(P) - sum(N).
// The internal state only grows even though the exposed value can go down,
// which is what lets concurrent increments and decrements both survive a
// merge.
type PNCounter struct {
	Pos GCounter
	Neg GCounter
}

// Increment adds delta on the P side (see GCounter.Increment for the
// empty-actor, zero-delta, and overflow rules).
func (p *PNCounter) Increment(actor string, delta uint64) error {
	return p.Pos.Increment(actor, delta)
}

// Decrement adds delta on the N side.
func (p *PNCounter) Decrement(actor string, delta uint64) error {
	return p.Neg.Increment(actor, delta)
}

// Merge merges both sides independently.
func (p *PNCounter) Merge(other PNCounter) {
	p.Pos.Merge(other.Pos)
	p.Neg.Merge(other.Neg)
}

// Value returns sum(P) - sum(N), clamped to the int64 range.
func (p PNCounter) Value() int64 {
	pos, neg := p.Pos.Value(), p.Neg.Value()
	switch {
	case pos >= neg:
		if d := pos - neg; d > uint64(math.MaxInt64) {
			return math.MaxInt64
		}
		return int64(pos - neg)
	default:
		if d := neg - pos; d > uint64(math.MaxInt64) {
			return math.MinInt64
		}
		return -int64(neg - pos)
	}
}

// Equal reports identical P and N sides.
func (p PNCounter) Equal(other PNCounter) bool {
	return p.Pos.Equal(other.Pos) && p.Neg.Equal(other.Neg)
}

// Clone deep-copies both sides (see GCounter.Clone).
func (p PNCounter) Clone() PNCounter {
	return PNCounter{Pos: p.Pos.Clone(), Neg: p.Neg.Clone()}
}

// Empty reports no components on either side.
func (p PNCounter) Empty() bool {
	return p.Pos.Empty() && p.Neg.Empty()
}
