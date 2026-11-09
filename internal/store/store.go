package store

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
)

// ErrCapacityExceeded rejects a lone entry bigger than its shard slice,
// exactly like the C++ store (even when it would fit global capacity).
var ErrCapacityExceeded = errors.New("store: value exceeds capacity")

// Policy selects the capacity-eviction discipline. It is fixed at
// construction: shards are sized from capacity alone, but victim choice,
// access bookkeeping, and per-node accounting differ per policy.
type Policy int

const (
	// LRUPolicy evicts the least-recently-used live entry (C++ LruStore).
	LRUPolicy Policy = iota
	// LFUPolicy evicts the lowest-frequency live entry, ties broken by
	// least-recently-incremented (C++ LfuStore).
	LFUPolicy
)

// String renders the policy name for admin JSON and logs.
func (p Policy) String() string {
	if p == LFUPolicy {
		return "lfu"
	}
	return "lru"
}

// NodeOverhead is the per-entry accounting constant for the LRU policy.
// It matches C++ sizeof(LruNode) (120) byte-for-byte so capacity behavior,
// eviction thresholds, and tests stay identical across the two trees.
// LFU nodes carry freq/freqIdx (C++ sizeof(LfuNode) = 136); see nodeOverhead.
const NodeOverhead = 120

// lfuNodeOverhead matches C++ sizeof(LfuNode) (136).
const lfuNodeOverhead = 136

const (
	maxShards     = 16
	minShardBytes = 65_536
	noLink        = -1
)

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
	// LFU bookkeeping, mirroring C++ LfuNode. freq counts accepted
	// writes + live reads; freqIdx is the node's position in its
	// shard.freqBuckets[freq] vector for O(1) swap-remove. Unused (zero)
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

// Store is a sharded eviction store with TTL-wheel expiry, LWW
// versioned writes, and tombstones. It mirrors LruStore /
// EvictionStoreBase: per-shard capacity slices, tombstone exemption from
// capacity eviction, lazy expiry on read, and a global Lamport clock.
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
	// nodeOverhead is the per-entry accounting constant: 120 under LRU
	// (C++ sizeof(LruNode)), 136 under LFU (C++ sizeof(LfuNode)).
	nodeOverhead uint64

	persistMu   sync.RWMutex
	persistHook PersistHook
}

type mutationSink struct {
	fn MutationSink
}

// newStore builds a store with the given eviction policy. Sharding
// follows the C++ rule: one shard per 64 KiB, clamped to [1, 16], so tiny
// stores stay single-shard and behave exactly like a single-mutex store.
func newStore(capacityBytes uint64, clock Clock, policy Policy) *Store {
	if clock == nil {
		clock = SystemClock{}
	}
	n := shardCountFor(capacityBytes)
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
// least-recently-incremented), mirroring C++ LfuStore.
func NewLFUStore(capacityBytes uint64, clock Clock) *Store {
	return newStore(capacityBytes, clock, LFUPolicy)
}

// Policy returns the immutable eviction discipline selected at construction.
func (s *Store) Policy() Policy { return s.policy }

func shardCountFor(capacityBytes uint64) int {
	n := min(max(int(capacityBytes/minShardBytes), 1), maxShards)
	return n
}

func shardFor(key string, n int) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(n))
}

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

// SetMetrics wires the observability collector (nil disables). Like C++
// setMetrics: every bump is nil-checked, so unconfigured stores pay only
// a pointer load.
func (s *Store) SetMetrics(m *metrics.Collector) {
	s.met.Store(m)
}

func (s *Store) bumpHits() {
	if m := s.met.Load(); m != nil {
		m.Hits.Add(1)
	}
}

func (s *Store) bumpMisses() {
	if m := s.met.Load(); m != nil {
		m.Misses.Add(1)
	}
}

func (s *Store) bumpExpiredOnRead() {
	if m := s.met.Load(); m != nil {
		m.ExpiresOnRead.Add(1)
	}
}

// bumpWrite records an accepted write: tombstones count as deletes,
// live values as writes, and overwriting a resident also retires it
// (mirroring C++ noteRemoved/noteAdded + the tombstone/write tail).
func (s *Store) bumpWrite(tombstone, overwrote bool) {
	m := s.met.Load()
	if m == nil {
		return
	}
	if overwrote {
		m.Deletes.Add(1)
	}
	if tombstone {
		m.Deletes.Add(1)
	} else {
		m.Writes.Add(1)
	}
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
	for {
		cur := s.nextVer.Load()
		if v <= cur || s.nextVer.CompareAndSwap(cur, v) {
			return
		}
	}
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
	for {
		cur := s.nextVer.Load()
		if v <= cur || s.nextVer.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (s *Store) shardCap() uint64 {
	return s.capacity.Load() / uint64(len(s.shards))
}

// Put stores value under key with a fresh minted version and no TTL.
func (s *Store) Put(key string, value []byte) error {
	e := Entry{Value: append([]byte(nil), value...), Version: s.MintVersion()}
	return s.PutVersioned(key, e)
}

// PutWithTTL stores value under key with a fresh minted version and a
// relative TTL. A zero TTL expires on the next read, like the C++ store.
func (s *Store) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	now := s.clock.Now()
	e := Entry{
		Value:     append([]byte(nil), value...),
		Version:   s.MintVersion(),
		HasTTL:    true,
		ExpiresAt: now.Add(ttl),
	}
	return s.putVersionedAt(key, e, now)
}

// PutVersioned applies entry idempotently: a strictly losing LWW stamp is
// rejected as a no-op success; everything else overwrites.
func (s *Store) PutVersioned(key string, e Entry) error {
	var at time.Time
	if e.HasTTL {
		at = s.clock.Now()
	}
	return s.putVersionedAt(key, e, at)
}

func (s *Store) putVersionedAt(key string, e Entry, at time.Time) error {
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if idx, ok := sh.index[key]; ok {
		resident := &sh.nodes[idx]
		if CompareLWW(stampOf(e), stampOf(resident.entry)) == LWWLess {
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
	sh := &s.shards[shardFor(key, len(s.shards))]
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
	sh := &s.shards[shardFor(key, len(s.shards))]
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
	sh := &s.shards[shardFor(key, len(s.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()

	idx, ok := sh.index[key]
	if !ok {
		return false
	}
	old := sh.nodes[idx].entry.Clone()
	s.eraseNode(sh, idx)
	s.persistDel(key, old)
	if m := s.met.Load(); m != nil {
		m.Deletes.Add(1)
	}
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
		HasTTL:         true,
		ExpiresAt:      now.Add(time.Duration(s.tombstone.Load())),
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
				if !n.entry.HasTTL || n.entry.ExpiresAt.After(now) {
					sh.wheel.Insert(id, expiryTicks(n.entry.ExpiresAt, now))
					return
				}
				s.eraseNode(sh, id)
				if m := s.met.Load(); m != nil {
					m.EvictionsTTL.Add(1)
				}
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

// expiryTicks maps an absolute expiry to wheel ticks: at least 1 ahead, so
// sub-second TTLs fire on the very next evictExpired.
func expiryTicks(expiresAt, at time.Time) uint64 {
	rem := expiresAt.Sub(at)
	if rem <= 0 {
		return 1
	}
	ticks := uint64((rem + time.Second - time.Nanosecond) / time.Second)
	if ticks < 1 {
		return 1
	}
	return ticks
}

func (s *Store) nodeIfResident(sh *shard, idx int) (*node, bool) {
	if idx < 0 || idx >= len(sh.nodes) {
		return nil, false
	}
	n := &sh.nodes[idx]
	cur, ok := sh.index[n.key]
	if !ok || cur != idx {
		return nil, false
	}
	return n, true
}

func (s *Store) allocNode(sh *shard) int {
	if len(sh.free) > 0 {
		idx := sh.free[len(sh.free)-1]
		sh.free = sh.free[:len(sh.free)-1]
		sh.nodes[idx] = node{}
		return idx
	}
	idx := len(sh.nodes)
	sh.nodes = append(sh.nodes, node{})
	return idx
}

func (s *Store) linkHead(sh *shard, idx int) {
	n := &sh.nodes[idx]
	n.lruPrev = noLink
	n.lruNext = sh.head
	if sh.head != noLink {
		sh.nodes[sh.head].lruPrev = idx
	}
	sh.head = idx
	if sh.tail == noLink {
		sh.tail = idx
	}
}

func (s *Store) unlink(sh *shard, idx int) {
	n := &sh.nodes[idx]
	if n.lruPrev != noLink {
		sh.nodes[n.lruPrev].lruNext = n.lruNext
	} else {
		sh.head = n.lruNext
	}
	if n.lruNext != noLink {
		sh.nodes[n.lruNext].lruPrev = n.lruPrev
	} else {
		sh.tail = n.lruPrev
	}
	n.lruPrev, n.lruNext = noLink, noLink
}

func (s *Store) touch(sh *shard, idx int) {
	if sh.head == idx {
		return
	}
	s.unlink(sh, idx)
	s.linkHead(sh, idx)
}

// onAccess records a live read (Get) or an accepted overwrite for
// victim ordering: MRU re-link under LRU, frequency increment under LFU.
// GetVersioned intentionally never calls it in either policy, mirroring
// the C++ read paths. Caller holds sh.mu.
func (s *Store) onAccess(sh *shard, idx int) {
	if s.policy == LFUPolicy {
		s.incrementFreq(sh, idx)
		return
	}
	s.touch(sh, idx)
}

func (s *Store) updateWheel(sh *shard, idx int, at time.Time) {
	n := &sh.nodes[idx]
	if n.entry.HasTTL {
		sh.wheel.Insert(idx, expiryTicks(n.entry.ExpiresAt, at))
	} else {
		// Wheel removal is idempotent, so no scheduled flag is needed.
		sh.wheel.Remove(idx)
	}
}

// eraseNode drops index/list/wheel for a node. Caller holds sh.mu.
func (s *Store) eraseNode(sh *shard, idx int) {
	s.removeNode(sh, idx)
}

// removeNode unlinks one node and updates all accounting, returning the
// bytes freed. Caller holds sh.mu.
func (s *Store) removeNode(sh *shard, idx int) uint64 {
	n := &sh.nodes[idx]
	s.emit(n.key, n.entry.VersionAndTTL(), n.entry.WriterNodeHash, true)
	freed := s.entrySize(n.key, n.entry)
	s.onEvictExpired(sh, idx)
	sh.wheel.Remove(idx)
	delete(sh.index, n.key)
	if s.policy == LRUPolicy {
		s.unlink(sh, idx)
	}
	sh.free = append(sh.free, idx)
	sh.current -= freed
	subAtomic(&s.totalSize, freed)
	s.totalNum.Add(^uint64(0)) // -1
	return freed
}

// onEvictExpired runs on every node removal (expiry, eviction, hard
// remove): a no-op under LRU, and the freq-bucket unlink under LFU.
// Mirrors C++ onEvictExpired. Caller holds sh.mu.
func (s *Store) onEvictExpired(sh *shard, idx int) {
	if s.policy == LFUPolicy {
		s.removeFromFreqBucket(sh, idx, sh.nodes[idx].freq)
	}
}

// evictOne removes one live entry, skipping tombstones wherever they
// sit: evicting a live delete marker would resurrect the key on the next
// anti-entropy round. Returns bytes freed, or 0 when only tombstones
// remain (their GC deadline reaps them instead). Victim choice is
// tail-first under LRU and min-frequency-first under LFU.
func (s *Store) evictOne(sh *shard) uint64 {
	if s.policy == LFUPolicy {
		return s.lfuEvictOne(sh)
	}
	victim := noLink
	for idx := sh.tail; idx != noLink; idx = sh.nodes[idx].lruPrev {
		if !sh.nodes[idx].entry.Tombstone {
			victim = idx
			break
		}
	}
	if victim == noLink {
		return 0
	}
	if m := s.met.Load(); m != nil {
		m.EvictionsCap.Add(1)
	}
	return s.removeNode(sh, victim)
}

func (s *Store) evictIfNeeded(sh *shard) {
	sliceCap := s.shardCap()
	for sh.current > sliceCap && s.shardNonEmpty(sh) {
		if s.evictOne(sh) == 0 {
			break
		}
	}
}

// shardNonEmpty reports whether any node (live or tombstone) is resident.
// LRU tracks the list head; LFU leaves the linkage unmaintained and uses
// the index instead. Caller holds sh.mu (or the shard is quiescent).
func (s *Store) shardNonEmpty(sh *shard) bool {
	if s.policy == LFUPolicy {
		return len(sh.index) > 0
	}
	return sh.head != noLink
}

// subAtomic subtracts n from an atomic counter.
func subAtomic(u *atomic.Uint64, n uint64) {
	u.Add(^(n - 1))
}
