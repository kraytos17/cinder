// Package store implements a sharded in-memory cache with LRU/LFU eviction,
// expiry scheduling, LWW versioned writes, and tombstones. Nodes and links
// live in slices and use integer indices; values use byte slices to keep the
// object graph small on hot paths.
package store

import (
	"errors"
	"fmt"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
	"unique"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/sharding"
)

// ErrCapacityExceeded rejects an entry larger than its destination
// shard's capacity, even if it would fit the aggregate store capacity.
var ErrCapacityExceeded = errors.New("store: value exceeds capacity")

// Policy selects the capacity-eviction discipline. It is fixed at
// construction: shards are sized from capacity alone, but victim choice,
// access bookkeeping, and per-node accounting differ per policy.
type Policy int

const (
	// LRUPolicy evicts the least-recently-used live entry.
	LRUPolicy Policy = iota
	// LFUPolicy evicts the lowest-frequency live entry, breaking ties by
	// least-recently-incremented order.
	LFUPolicy
)

// ParsePolicy parses the canonical lowercase configuration name for an
// eviction policy.
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "lru":
		return LRUPolicy, nil
	case "lfu":
		return LFUPolicy, nil
	default:
		return 0, fmt.Errorf("store: unknown eviction policy %q", s)
	}
}

// String renders the policy name for admin JSON and logs.
func (p Policy) String() string {
	if p == LFUPolicy {
		return "lfu"
	}
	return "lru"
}

// NodeOverhead is the per-entry capacity-accounting estimate for LRU
// entries. LFU uses a larger estimate for its frequency metadata.
const NodeOverhead = 120

// lfuNodeOverhead is the LFU per-entry capacity-accounting estimate,
// including frequency and bucket-index metadata.
const lfuNodeOverhead = 136

const noLink = -1

// MutationSink observes every accepted write, remove, and eviction while
// the caller holds the shard lock, so it must be leaf-only: never call
// back into the store, never block. Removal passes removed=true.
type MutationSink func(key string, versionAndTTL, writer uint64, removed bool)

type node struct {
	key   string
	entry Entry
	// LRU linkage (indices into the shard's node slice, noLink = none).
	// Maintained only under LRUPolicy; LFU orders victims by frequency.
	lruPrev int
	lruNext int
	// LFU bookkeeping. freq counts accepted writes and live reads;
	// freqIdx is the node's position in its shard.freqBuckets[freq] vector
	// for O(1) swap-remove. It is unused (zero)
	// under LRUPolicy.
	freq    uint64
	freqIdx int
}

type shard struct {
	mu       sync.Mutex
	nodes    []node
	free     []int
	head     int // MRU
	tail     int // LRU victim end
	index    map[string]int
	wheel    *Wheel
	current  uint64
	lastTick time.Time
	// freqBuckets orders LFU victims: freq -> node indices in
	// least-recently-incremented order. Nil under LRUPolicy.
	freqBuckets map[uint64][]int
}

// Store is a sharded eviction store with scheduled expiry, versioned LWW
// writes, and tombstones. Capacity is enforced per shard; tombstones are
// exempt from capacity eviction, expired entries are removed on read or by
// the expiry wheel, and versions use a store-wide Lamport clock.
type Store struct {
	clock     Clock
	policy    Policy
	shards    []shard
	capacity  atomic.Uint64
	nextVer   atomic.Uint64
	tombstone atomic.Int64 // GC grace in nanoseconds
	sink      atomic.Pointer[mutationSink]
	totalSize atomic.Uint64
	totalNum  atomic.Uint64
	met       atomic.Pointer[metrics.Collector]
	// nodeOverhead is the per-entry capacity-accounting estimate selected
	// for the policy: 120 bytes under LRU, 136 under LFU.
	nodeOverhead uint64

	persistMu   sync.RWMutex
	persistHook PersistHook
}

type mutationSink struct {
	fn MutationSink
}

// newStore builds a store with the given eviction policy. Shard count is
// one per 64 KiB of capacity, clamped to [1, 16], so small stores use a
// single shard.
func newStore(capacityBytes uint64, clock Clock, policy Policy) *Store {
	if clock == nil {
		clock = SystemClock{}
	}

	n := sharding.CountFor(capacityBytes)
	overhead := uint64(NodeOverhead)
	if policy == LFUPolicy {
		overhead = lfuNodeOverhead
	}

	s := &Store{clock: clock, policy: policy, shards: make([]shard, n), nodeOverhead: overhead}
	s.capacity.Store(capacityBytes)
	s.nextVer.Store(uint64(clock.Now().UnixNano()))
	s.tombstone.Store(int64(5 * time.Minute))
	for i := range s.shards {
		sh := &s.shards[i]
		sh.head = noLink
		sh.tail = noLink
		sh.index = make(map[string]int)
		sh.wheel = NewWheel()
		if policy == LFUPolicy {
			sh.freqBuckets = make(map[uint64][]int)
		}
		sh.lastTick = clock.Now()
	}
	return s
}

// NewLRUStore returns a store with capacityBytes total capacity and
// least-recently-used eviction.
func NewLRUStore(capacityBytes uint64, clock Clock) *Store {
	return newStore(capacityBytes, clock, LRUPolicy)
}

// NewLFUStore returns a store with capacityBytes total capacity and
// least-frequently-used eviction (ties broken by
// least-recently-incremented order).
func NewLFUStore(capacityBytes uint64, clock Clock) *Store {
	return newStore(capacityBytes, clock, LFUPolicy)
}

// Policy returns the immutable eviction discipline selected at construction.
func (s *Store) Policy() Policy { return s.policy }

// shardSeed is the store's placement seed (see internal/sharding): one
// seed per process keeps every Store on the same mapping, which keeps
// tests deterministic within a run.
var shardSeed = maphash.MakeSeed()

func (s *Store) entrySize(key string, e Entry) uint64 {
	return uint64(len(key)+len(e.Value)) + s.nodeOverhead
}

// SetMutationSink registers the anti-entropy incremental-digest feed.
func (s *Store) SetMutationSink(fn MutationSink) {
	if fn == nil {
		s.sink.Store(nil)
		return
	}
	s.sink.Store(&mutationSink{fn: fn})
}

// SetMetrics installs the observability collector; nil disables metrics.
// Each bump performs one atomic pointer load to check whether metrics
// are configured.
func (s *Store) SetMetrics(m *metrics.Collector) {
	s.met.Store(m)
}

func (s *Store) bumpHits() {
	obs.IfPresent(&s.met, func(m *metrics.Collector) { m.Hits.Add(1) })
}

func (s *Store) bumpMisses() {
	obs.IfPresent(&s.met, func(m *metrics.Collector) { m.Misses.Add(1) })
}

func (s *Store) bumpExpiredOnRead() {
	obs.IfPresent(&s.met, func(m *metrics.Collector) { m.ExpiresOnRead.Add(1) })
}

// bumpWrite records an accepted write: tombstones count as deletes, live
// values as writes, and replacing a resident also counts its removal.
func (s *Store) bumpWrite(tombstone, overwrote bool) {
	obs.IfPresent(&s.met, func(m *metrics.Collector) {
		if overwrote {
			m.Deletes.Add(1)
		}
		if tombstone {
			m.Deletes.Add(1)
		} else {
			m.Writes.Add(1)
		}
	})
}

func (s *Store) emit(key string, versionAndTTL, writer uint64, removed bool) {
	if ms := s.sink.Load(); ms != nil {
		ms.fn(key, versionAndTTL, writer, removed)
	}
}

// PersistHook observes every accepted write and every hard remove for
// the WAL. It runs under the shard lock, so it must be leaf-only: never
// call back into the store, never block. hardDelete replays as a hard
// remove; accepted writes replay as versioned applies (tombstones
// included). Expiry and capacity evictions are NOT reported: absolute
// expiries re-skip and capacity re-evicts deterministically on replay.
type PersistHook func(key string, e Entry, hardDelete bool)

// SetPersistence registers the WAL hook (nil unregisters).
func (s *Store) SetPersistence(fn PersistHook) {
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	s.persistHook = fn
}

func (s *Store) persistPut(key string, e Entry) {
	s.persistMu.RLock()
	fn := s.persistHook
	s.persistMu.RUnlock()
	if fn != nil {
		fn(key, e, false)
	}
}

func (s *Store) persistDel(key string, e Entry) {
	s.persistMu.Lock()
	fn := s.persistHook
	s.persistMu.Unlock()
	if fn != nil {
		fn(key, e, true)
	}
}

// RestoreVersion advances the Lamport clock past recovered versions so a
// restarted node wins LWW. Used by snapshot/WAL recovery.
func (s *Store) RestoreVersion(v uint64) {
	obs.RetryMax(s.nextVer.Load, s.nextVer.CompareAndSwap, v)
}

// SetTombstoneGCInterval sets how long a delete marker survives before the
// TTL wheel reaps it.
func (s *Store) SetTombstoneGCInterval(d time.Duration) {
	s.tombstone.Store(int64(d))
}

// SetCapacity changes capacity at runtime; the shard count stays fixed and
// only the per-shard slice changes.
func (s *Store) SetCapacity(capacityBytes uint64) {
	s.capacity.Store(capacityBytes)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		s.evictIfNeeded(sh)
		sh.mu.Unlock()
	}
}

// MintVersion returns a fresh monotonic Lamport version.
func (s *Store) MintVersion() uint64 {
	return s.nextVer.Add(1) - 1
}

func (s *Store) observeVersion(v uint64) {
	obs.RetryMax(s.nextVer.Load, s.nextVer.CompareAndSwap, v)
}

func (s *Store) shardCap() uint64 {
	return s.capacity.Load() / uint64(len(s.shards))
}

// Put stores value under key with a fresh minted version and no TTL.
func (s *Store) Put(key string, value []byte) error {
	e := Entry{Value: append([]byte(nil), value...), Version: s.MintVersion()}
	return s.PutVersioned(key, e)
}

// PutWithTTL stores value under key with a fresh version and a relative
// TTL. A zero TTL creates an immediately expired entry, removed on its
// next read or expiry-wheel pass.
func (s *Store) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	now := s.clock.Now()
	e := Entry{
		Value:   append([]byte(nil), value...),
		Version: s.MintVersion(),
		// At (not After): zero TTL is an immediately elapsed deadline.
		Expiry: expiry.At(now.Add(ttl)),
	}
	return s.putVersionedAt(key, e, now)
}

// PutVersioned applies entry idempotently: a strictly losing LWW stamp is
// rejected as a no-op success; everything else overwrites.
func (s *Store) PutVersioned(key string, e Entry) error {
	var at time.Time
	if !e.Expiry.IsNone() {
		at = s.clock.Now()
	}
	return s.putVersionedAt(key, e, at)
}

func (s *Store) putVersionedAt(key string, e Entry, at time.Time) error {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if idx, ok := sh.index[key]; ok {
		resident := &sh.nodes[idx]
		if CompareLWW(e.Stamp(), resident.entry.Stamp()) == LWWLess {
			return nil // stale replay: reject, no-op success
		}

		oldPacked := resident.entry.VersionAndTTL()
		oldWriter := resident.entry.WriterNodeHash
		s.observeVersion(e.Version + 1)
		oldSize := s.entrySize(key, resident.entry)
		sh.current -= oldSize
		subAtomic(&s.totalSize, oldSize)
		resident.entry = e.Clone()
		s.onAccess(sh, idx)
		newSize := s.entrySize(key, resident.entry)
		sh.current += newSize
		s.totalSize.Add(newSize)
		s.evictIfNeeded(sh)
		// evictIfNeeded may have taken this very node (it can be the
		// minimum); re-derive instead of touching a dead index.
		fresh, ok := sh.index[key]
		if !ok {
			s.emit(key, oldPacked, oldWriter, true)
			return nil
		}

		r := &sh.nodes[fresh]
		s.emit(key, oldPacked, oldWriter, true)
		s.emit(key, r.entry.VersionAndTTL(), r.entry.WriterNodeHash, false)
		s.updateWheel(sh, fresh, at)
		s.persistPut(key, r.entry)
		s.bumpWrite(e.Tombstone, true)
		return nil
	}

	size := s.entrySize(key, e)
	if size > s.shardCap() {
		return ErrCapacityExceeded
	}

	s.observeVersion(e.Version + 1)
	idx := s.allocNode(sh)
	n := &sh.nodes[idx]
	// Canonicalize the key: the index map copies only the string header,
	// but every admission otherwise parks a second copy of the bytes next
	// to the map's. unique's table is weak-backed (verified: dropped keys
	// are collected), so hot keys converge on one shared backing while
	// churned keys cost nothing long-term. Lookups above intentionally use
	// the raw param — equality needs no canonical form.
	key = unique.Make(key).Value()
	n.key = key
	n.entry = e.Clone()
	sh.index[key] = idx
	if s.policy == LFUPolicy {
		s.freqInit(sh, idx)
	} else {
		s.linkHead(sh, idx)
	}

	sh.current += size
	s.totalSize.Add(size)
	s.totalNum.Add(1)
	s.evictIfNeeded(sh)
	if _, ok := sh.index[key]; !ok {
		return nil // self-evicted under pressure: nothing to report
	}

	r := &sh.nodes[sh.index[key]]
	s.emit(key, r.entry.VersionAndTTL(), r.entry.WriterNodeHash, false)
	s.updateWheel(sh, sh.index[key], at)
	s.persistPut(key, r.entry)
	s.bumpWrite(e.Tombstone, false)
	return nil
}

// Get returns a copy of the live value, or false on miss, expiry, or
// tombstone. Expired entries are reaped eagerly; tombstones stay hidden.
func (s *Store) Get(key string) ([]byte, bool) {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	idx, ok := sh.index[key]
	if !ok {
		s.bumpMisses()
		return nil, false
	}

	n := &sh.nodes[idx]
	if n.entry.Expired(s.clock.Now()) {
		s.eraseNode(sh, idx)
		s.bumpExpiredOnRead()
		return nil, false
	}
	if n.entry.Tombstone {
		s.bumpMisses()
		return nil, false
	}

	s.onAccess(sh, idx)
	s.bumpHits()
	return append([]byte(nil), n.entry.Value...), true
}

// GetVersioned returns the resident entry (tombstones included), or false
// on miss or expiry. Unlike Get it never touches LRU order.
func (s *Store) GetVersioned(key string) (Entry, bool) {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	idx, ok := sh.index[key]
	if !ok {
		s.bumpMisses()
		return Entry{}, false
	}

	n := &sh.nodes[idx]
	if n.entry.Expired(s.clock.Now()) {
		s.eraseNode(sh, idx)
		s.bumpExpiredOnRead()
		return Entry{}, false
	}

	s.bumpHits()
	return n.entry.Clone(), true
}

// Remove hard-erases key, returning false when absent. Unlike
// RemoveVersioned it leaves no tombstone behind.
func (s *Store) Remove(key string) bool {
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	idx, ok := sh.index[key]
	if !ok {
		return false
	}

	old := sh.nodes[idx].entry.Clone()
	s.eraseNode(sh, idx)
	s.persistDel(key, old)
	obs.IfPresent(&s.met, func(m *metrics.Collector) { m.Deletes.Add(1) })
	return true
}

// RemoveVersioned plants a tombstone that wins or loses LWW exactly like a
// put. The marker carries a GC deadline on the TTL wheel so replicas
// observe the delete before it is reaped.
func (s *Store) RemoveVersioned(key string, version, writer uint64) error {
	now := s.clock.Now()
	tomb := Entry{
		Version:        version,
		WriterNodeHash: writer,
		Tombstone:      true,
		Expiry:         expiry.At(now.Add(time.Duration(s.tombstone.Load()))),
	}
	return s.putVersionedAt(key, tomb, now)
}

// Size counts index entries, tombstones included.
func (s *Store) Size() int {
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		total += len(sh.index)
		sh.mu.Unlock()
	}
	return total
}

// EvictExpired advances every shard wheel to now, reaping entries whose
// deadline passed and re-scheduling early firers (wheel wrap or
// sub-second drift) for their true expiry.
func (s *Store) EvictExpired() int {
	now := s.clock.Now()
	evicted := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		elapsed := now.Sub(sh.lastTick)
		var ticks uint64 = 1
		if elapsed > 0 {
			if t := uint64(elapsed / time.Second); t > 1 {
				ticks = t
			}
		}

		sh.lastTick = now
		for range ticks {
			sh.wheel.Tick(func(id int) {
				n, ok := s.nodeIfResident(sh, id)
				if !ok {
					return
				}
				if exp, ok := n.entry.Expiry.Time(); !ok || exp.After(now) {
					sh.wheel.Insert(id, expiryTicks(exp, now))
					return
				}

				s.eraseNode(sh, id)
				obs.IfPresent(&s.met, func(m *metrics.Collector) { m.EvictionsTTL.Add(1) })
				evicted++
			})
		}
		sh.mu.Unlock()
	}
	return evicted
}

// EntryPair is one live snapshot row. Freq carries the node's frequency
// for snapshot persistence (0 under LRU, which never tracks it).
type EntryPair struct {
	Key   string
	Entry Entry
	Freq  uint64
}

// LiveEntries returns a point-in-time copy of all present entries,
// tombstones included, expired entries skipped. The store may be mutated
// freely while iterating the result.
func (s *Store) LiveEntries() []EntryPair {
	now := s.clock.Now()
	var out []EntryPair
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		if s.policy == LFUPolicy {
			// Map order is fine: digests XOR-fold (order-independent)
			// and snapshot recovery is LWW order-independent.
			for _, idx := range sh.index {
				e := sh.nodes[idx].entry
				if e.Expired(now) {
					continue
				}

				out = append(out, EntryPair{Key: sh.nodes[idx].key, Entry: e.Clone(), Freq: sh.nodes[idx].freq})
			}
		} else {
			for idx := sh.head; idx != noLink; idx = sh.nodes[idx].lruNext {
				e := sh.nodes[idx].entry
				if e.Expired(now) {
					continue
				}
				out = append(out, EntryPair{Key: sh.nodes[idx].key, Entry: e.Clone()})
			}
		}
		sh.mu.Unlock()
	}
	return out
}

// ForEach visits a point-in-time snapshot, so the visitor may safely call
// back into the store.
func (s *Store) ForEach(fn func(key string, e Entry)) {
	for _, p := range s.LiveEntries() {
		fn(p.Key, p.Entry)
	}
}
