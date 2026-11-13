// Sharded CRDT counter store: key -> PNCounter with merge-based writes.
//
// This store is intentionally separate from the LWW cache. LWW would drop
// concurrent increments; here every write path merges: local adjusts merge
// into the resident counter, and replicated state merges component-wise, so
// concurrent updates on any replica survive regardless of arrival order or
// duplication.
//
// Counter-store constraints:
//   - No TTL/expiry: counters are state, not cache entries; expiry would
//     silently discard increments.
//   - No eviction: over-budget writes fail with ErrCapacityExceeded
//     instead of evicting a neighbor. Lowering capacity only rejects
//     future growth; existing state is never touched.
//   - No tombstones: Remove erases locally, and a merge from a peer that
//     still holds the key resurrects it. Remove is an operator-local
//     escape hatch, not a replicated delete.
//   - Counter state is persisted independently from the LWW snapshot and
//     participates in explicit replication and repair.
package counter

import (
	"cmp"
	"errors"
	"hash/maphash"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"unique"

	"github.com/kraytos17/cinder/internal/sharding"
)

// ErrCapacityExceeded rejects counter updates whose additional state would
// exceed the store's capacity budget.
var ErrCapacityExceeded = errors.New("counter: exceeds capacity")

// counterSeed is the counter store's placement seed (see
// internal/sharding): shard-local, never on the wire.
var counterSeed = maphash.MakeSeed()

// addSat is a saturating add for capacity accounting: actor-controlled
// lengths must not wrap the budget check.
func addSat(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

// BillableSize is the billable size: key plus every actor id plus its two
// component slots. Deterministic and cheap; recomputed on merge rather
// than cached per write.
func (p PNCounter) BillableSize(key string) uint64 {
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

// PersistHook observes a complete post-mutation counter state. remove=true
// records a local removal. It runs under the shard lock and must not call
// back into the counter store.
type PersistHook func(key string, state PNCounter, remove bool) error

// Store is a sharded counter map. Keys stripe over shards (one per
// 64 KiB, clamped to [1,16]); reads copy out.
type Store struct {
	capacity    atomic.Uint64
	shards      []shard
	persistMu   sync.RWMutex
	persistHook PersistHook
	persistGate sync.RWMutex
}

// NewStore returns a store with the given byte budget.
func NewStore(capacityBytes uint64) *Store {
	n := sharding.CountFor(capacityBytes)
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

// SetPersistence installs or removes the durable mutation hook.
func (s *Store) SetPersistence(fn PersistHook) {
	s.persistMu.Lock()
	s.persistHook = fn
	s.persistMu.Unlock()
}

// WithPersistenceBarrier prevents mutations from crossing a persistence
// snapshot/WAL cutover. Mutations already in progress complete first.
func (s *Store) WithPersistenceBarrier(fn func() error) error {
	s.persistGate.Lock()
	defer s.persistGate.Unlock()
	return fn()
}

func (s *Store) persist(key string, state PNCounter, remove bool) error {
	s.persistMu.RLock()
	fn := s.persistHook
	s.persistMu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(key, state.Clone(), remove)
}

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

	s.persistGate.RLock()
	defer s.persistGate.RUnlock()
	// Actors are node IDs (bounded vocabulary, unlike user keys), so
	// canonicalizing converges every counter's component map on one
	// shared backing per actor. The Merge path needs no equivalent:
	// wire actors replace-on-max and never accumulate.
	actor = unique.Make(actor).Value()
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
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

		sliceCap := s.shardCapacity()
		if growth > 0 && (growth > sliceCap || sh.current > sliceCap-growth) {
			return ErrCapacityExceeded
		}
		if positive {
			if err := it.Pos.Increment(actor, delta); err != nil {
				if errors.Is(err, ErrOverflow) {
					return ErrCapacityExceeded
				}
				return err
			}
		} else {
			if err := it.Neg.Increment(actor, delta); err != nil {
				if errors.Is(err, ErrOverflow) {
					return ErrCapacityExceeded
				}
				return err
			}
		}

		sh.current += growth
		sh.index[key] = it
		return s.persist(key, it, false)
	}

	var fresh PNCounter
	var err error
	if positive {
		err = fresh.Increment(actor, delta)
	} else {
		err = fresh.Decrement(actor, delta)
	}

	if err != nil {
		if errors.Is(err, ErrOverflow) {
			return ErrCapacityExceeded
		}
		return err
	}

	need := fresh.BillableSize(key)
	sliceCap := s.shardCapacity()
	if need > sliceCap || sh.current > sliceCap-need {
		return ErrCapacityExceeded
	}

	sh.current += need
	sh.index[key] = fresh
	return s.persist(key, fresh, false)
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

	s.persistGate.RLock()
	defer s.persistGate.RUnlock()
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if it, ok := sh.index[key]; ok {
		merged := it.Clone()
		merged.Merge(remote)
		if merged.Equal(it) {
			return nil
		}

		oldSize, newSize := it.BillableSize(key), merged.BillableSize(key)
		// Merge only adds components, so the size never shrinks.
		growth := newSize - oldSize
		sliceCap := s.shardCapacity()
		if growth > 0 && (growth > sliceCap || sh.current > sliceCap-growth) {
			return ErrCapacityExceeded
		}

		sh.current += growth
		sh.index[key] = merged
		return s.persist(key, merged, false)
	}

	need := remote.BillableSize(key)
	sliceCap := s.shardCapacity()
	if need > sliceCap || sh.current > sliceCap-need {
		return ErrCapacityExceeded
	}

	sh.current += need
	sh.index[key] = remote.Clone()
	return s.persist(key, remote, false)
}

// Value returns the current value, or false when absent. Counters have
// neither tombstones nor expiry.
func (s *Store) Value(key string) (int64, bool) {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
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
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
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
	removed, _ := s.RemoveWithError(key)
	return removed
}

// RemoveWithError removes local state and persists the removal. The in-memory
// removal is not rolled back if persistence fails.
func (s *Store) RemoveWithError(key string) (bool, error) {
	s.persistGate.RLock()
	defer s.persistGate.RUnlock()
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	it, ok := sh.index[key]
	if !ok {
		return false, nil
	}

	removed := it.Clone()
	sh.current -= it.BillableSize(key)
	delete(sh.index, key)
	return true, s.persist(key, removed, true)
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
	slices.SortFunc(out, func(a, b BundleEntry) int { return cmp.Compare(a.Key, b.Key) })
	return out
}

// SortedLiveCounterKeys returns a point-in-time key list without cloning CRDT
// actor maps. Call Get for states selected for transfer.
func (s *Store) SortedLiveCounterKeys() []string {
	keys := make([]string, 0, s.Size())
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for key := range sh.index {
			keys = append(keys, key)
		}
		sh.mu.RUnlock()
	}

	slices.Sort(keys)
	return keys
}

// BundleRecordSize reports the encoded bundle record size without cloning
// actor maps or sorting actors. It returns false if key has no state.
func (s *Store) BundleRecordSize(key string) (int, bool) {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
	sh.mu.RLock()
	state, ok := sh.index[key]
	size, valid := state.BundleRecordSize(key)
	sh.mu.RUnlock()
	return size, ok && valid
}

// Encode returns a canonical copy of one counter's CRDC encoding while
// holding the shard read lock. It avoids cloning both actor maps through
// Get when a caller only needs serialized state.
func (s *Store) Encode(key string) ([]byte, bool) {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), counterSeed)]
	sh.mu.RLock()
	state, ok := sh.index[key]
	if !ok {
		sh.mu.RUnlock()
		return nil, false
	}

	encoded := state.Encode()
	sh.mu.RUnlock()
	return encoded, len(encoded) > 0
}
