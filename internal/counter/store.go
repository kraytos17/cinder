// Sharded CRDT counter store: key -> PNCounter with merge-based writes.
//
// This store is intentionally separate from the LWW cache. LWW would drop
// concurrent increments; here every write path merges: local adjusts merge
// into the resident counter, and replicated state merges component-wise, so
// concurrent updates on any replica survive regardless of arrival order or
// duplication.
//
// Deliberate non-goals, mirroring C++ CrdtCounterStore:
//   - No TTL/expiry: counters are state, not cache entries; expiry would
//     silently discard increments.
//   - No eviction: over-budget writes fail with ErrCapacityExceeded
//     instead of evicting a neighbor. Lowering capacity only rejects
//     future growth; existing state is never touched.
//   - No tombstones: Remove erases locally, and a merge from a peer that
//     still holds the key resurrects it. Remove is an operator-local
//     escape hatch, not a replicated delete.
//   - Counter state is not in the LWW WAL/snapshot or LWW digest;
//     replication and repair merge these states explicitly.
package counter

import (
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrCapacityExceeded rejects over-budget growth, mirroring C++
// Errc::CapacityExceeded on the counter paths.
var ErrCapacityExceeded = errors.New("counter: exceeds capacity")

const (
	maxShards     = 16
	minShardBytes = 65536
)

func shardCountFor(capacityBytes uint64) int {
	n := max(int(capacityBytes/minShardBytes), 1)
	if n > maxShards {
		n = maxShards
	}
	return n
}

func shardFor(key string, n int) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(n))
}

// addSat is a saturating add for capacity accounting: actor-controlled
// lengths must not wrap the budget check.
func addSat(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

// CounterBytes is the billable size: key plus every actor id plus its two
// component slots. Deterministic and cheap; recomputed on merge rather
// than cached per write.
func CounterBytes(key string, p PNCounter) uint64 {
	total := uint64(len(key))
	for _, side := range []GCounter{p.Pos, p.Neg} {
		for _, actor := range side.Actors() {
			total = addSat(total, uint64(len(actor)))
			total = addSat(total, 2*8)
		}
	}
	return total
}

type shard struct {
	mu      sync.RWMutex
	index   map[string]PNCounter
	current uint64
}

// Store is a sharded counter map. Keys stripe over shards (one per
// 64 KiB, clamped to [1,16]); reads copy out.
type Store struct {
	capacity atomic.Uint64
	shards   []shard
}

// NewStore returns a store with the given byte budget.
func NewStore(capacityBytes uint64) *Store {
	n := shardCountFor(capacityBytes)
	s := &Store{shards: make([]shard, n)}
	for i := range s.shards {
		s.shards[i].index = make(map[string]PNCounter)
	}
	s.capacity.Store(capacityBytes)
	return s
}

// CapacityBytes returns the current budget.
func (s *Store) CapacityBytes() uint64 { return s.capacity.Load() }

// SetCapacity changes the budget. Never evicts: shards already over the
// new slice only reject growth until removals bring them back under.
func (s *Store) SetCapacity(n uint64) { s.capacity.Store(n) }

func (s *Store) shardCapacity() uint64 {
	return s.capacity.Load() / uint64(len(s.shards))
}

// adjust is the shared increment/decrement body; positive selects P.
func (s *Store) adjust(key, actor string, delta uint64, positive bool) error {
	if delta == 0 {
		return nil
	}
	if actor == "" {
		return ErrInvalidActor
	}
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if it, ok := sh.index[key]; ok {
		// P and N are independent: adding the first component for an
		// actor on one side does not mean it exists on the other.
		// Charge each newly-created side separately.
		var side GCounter
		if positive {
			side = it.Pos
		} else {
			side = it.Neg
		}
		var growth uint64
		if _, exists := side.components[actor]; !exists {
			growth = uint64(len(actor)) + 2*8
		}
		cap := s.shardCapacity()
		if growth > 0 && (growth > cap || sh.current > cap-growth) {
			return ErrCapacityExceeded
		}
		if positive {
			if err := it.Pos.Increment(actor, delta); err != nil {
				if err == ErrOverflow {
					return ErrCapacityExceeded
				}
				return err
			}
		} else {
			if err := it.Neg.Increment(actor, delta); err != nil {
				if err == ErrOverflow {
					return ErrCapacityExceeded
				}
				return err
			}
		}
		sh.current += growth
		sh.index[key] = it
		return nil
	}
	var fresh PNCounter
	var err error
	if positive {
		err = fresh.Increment(actor, delta)
	} else {
		err = fresh.Decrement(actor, delta)
	}
	if err != nil {
		if err == ErrOverflow {
			return ErrCapacityExceeded
		}
		return err
	}
	need := CounterBytes(key, fresh)
	cap := s.shardCapacity()
	if need > cap || sh.current > cap-need {
		return ErrCapacityExceeded
	}
	sh.current += need
	sh.index[key] = fresh
	return nil
}

// Increment adds delta to key's counter, attributed to actor.
func (s *Store) Increment(key, actor string, delta uint64) error {
	return s.adjust(key, actor, delta, true)
}

// Decrement adds delta on the N side.
func (s *Store) Decrement(key, actor string, delta uint64) error {
	return s.adjust(key, actor, delta, false)
}

// Merge folds replicated state into the resident counter (idempotent).
// Over-budget merges fail without touching the resident, so a retried
// merge converges instead of partially applying.
func (s *Store) Merge(key string, remote PNCounter) error {
	if remote.Empty() {
		return nil
	}
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if it, ok := sh.index[key]; ok {
		merged := it.Clone()
		merged.Merge(remote)
		oldSize, newSize := CounterBytes(key, it), CounterBytes(key, merged)
		// Merge only adds components, so the size never shrinks.
		growth := newSize - oldSize
		cap := s.shardCapacity()
		if growth > 0 && (growth > cap || sh.current > cap-growth) {
			return ErrCapacityExceeded
		}
		sh.current += growth
		sh.index[key] = merged
		return nil
	}
	need := CounterBytes(key, remote)
	cap := s.shardCapacity()
	if need > cap || sh.current > cap-need {
		return ErrCapacityExceeded
	}
	sh.current += need
	sh.index[key] = remote.Clone()
	return nil
}

// Value returns the current value, or false when absent. Counters have
// neither tombstones nor expiry.
func (s *Store) Value(key string) (int64, bool) {
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	it, ok := sh.index[key]
	if !ok {
		return 0, false
	}
	return it.Value(), true
}

// Get returns a copy of the full state for replication sends.
func (s *Store) Get(key string) (PNCounter, bool) {
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	it, ok := sh.index[key]
	if !ok {
		return PNCounter{}, false
	}
	return it.Clone(), true
}

// Remove erases locally, returning whether anything was removed. A merge
// from a peer that still holds the key resurrects it (no tombstones).
func (s *Store) Remove(key string) bool {
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it, ok := sh.index[key]
	if !ok {
		return false
	}
	sh.current -= CounterBytes(key, it)
	delete(sh.index, key)
	return true
}

// Size returns the key count.
func (s *Store) Size() int {
	total := 0
	for i := range s.shards {
		s.shards[i].mu.RLock()
		total += len(s.shards[i].index)
		s.shards[i].mu.RUnlock()
	}
	return total
}

// SizeBytes returns the billable bytes.
func (s *Store) SizeBytes() uint64 {
	var total uint64
	for i := range s.shards {
		s.shards[i].mu.RLock()
		total += s.shards[i].current
		s.shards[i].mu.RUnlock()
	}
	return total
}

// LiveCounters is a point-in-time copy of every counter.
func (s *Store) LiveCounters() []BundleEntry {
	var out []BundleEntry
	for i := range s.shards {
		s.shards[i].mu.RLock()
		for k, v := range s.shards[i].index {
			out = append(out, BundleEntry{Key: k, Counter: v.Clone()})
		}
		s.shards[i].mu.RUnlock()
	}
	return out
}

// SortedLiveCounters returns live counters sorted by key (AE batches).
func (s *Store) SortedLiveCounters() []BundleEntry {
	out := s.LiveCounters()
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
