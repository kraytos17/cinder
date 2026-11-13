package ae

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/transport"

	"github.com/rs/zerolog"
)

// Transport is the shared inter-node send contract (see
// internal/transport); *client.Pool satisfies it.
type Transport = transport.Sender

// nopLogger is the shared disabled logger for managers constructed
// without SetLogger.
var nopLogger = log.Nop()

// DefaultRehashInterval bounds the silent-divergence window: accumulators
// are verified against a full scan at least this often. Decoupled from the
// round cadence. Zero Config.RehashInterval selects this default.
const DefaultRehashInterval = 5 * time.Minute

// Manager runs anti-entropy rounds: digest exchange, bucket refinement,
// and targeted synchronization. Digests are maintained incrementally:
// the store's mutation sink XOR-folds each accepted write/remove into its
// bucket accumulator at write time, so converged rounds cost a 2 KiB
// copy instead of a full scan. A wall-clock full-rehash backstop closes
// XOR cancellation and any missed observer path.
type Manager struct {
	store      *store.Store
	ring       *hashring.Ring
	self       string
	transport  Transport
	numBuckets uint32
	fetchBytes int

	mu         sync.Mutex
	partners   []string
	partnerIdx int
	journal    *Journal
	met        atomic.Pointer[metrics.Collector]
	logger     atomic.Pointer[zerolog.Logger]

	// Incremental digest state. digestMu is a leaf: the store's mutation
	// sink takes it read-shared under the shard lock (shard -> digestMu,
	// never the reverse), while a backstop rehash takes it exclusively.
	// Accumulators are atomic so concurrent sinks stay lock-free
	// relative to each other; the RWMutex only fences rehash publish.
	digestMu       sync.RWMutex
	acc            []atomic.Uint64
	accReady       atomic.Bool
	lastRehash     atomic.Int64 // unix nanos of last published rehash
	rehashInterval atomic.Int64 // nanos; DefaultRehashInterval when unset
	clock          func() time.Time

	counters   CounterStore
	counterMu  sync.Mutex
	counterCur string
}

// CounterStore is the subset of *counter.Store the AE manager needs.
// *counter.Store satisfies it; tests stub it.
type CounterStore interface {
	SortedLiveCounters() []BundleEntry
	SortedLiveCounterKeys() []string
	BundleRecordSize(key string) (int, bool)
	Get(key string) (PNCounter, bool)
	Merge(key string, remote PNCounter) error
	Size() int
	CapacityBytes() uint64
}

// SetCounterStore wires the CRDT counter store. Nil disables counter
// repair rounds; always set in production (counters live beside the LWW
// store on every node).
func (m *Manager) SetCounterStore(s CounterStore) {
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	m.counters = s
}

// Config defines the local store, membership ring, transport, and per-round
// anti-entropy budgets. Zero Buckets selects 256; non-positive FetchBytes
// selects a 1 MiB transfer budget. Zero RehashInterval selects
// DefaultRehashInterval; negative disables the wall-clock backstop (tests
// only — production must keep the bound). Nil Clock selects time.Now.
type Config struct {
	Store          *store.Store
	Ring           *hashring.Ring
	Self           string
	Transport      Transport
	Buckets        uint32
	FetchBytes     int
	RehashInterval time.Duration
	Clock          func() time.Time
}

// NewManager returns an anti-entropy manager configured by cfg. When
// Store is non-nil the manager registers its incremental-digest sink; a
// nil store leaves digests on the full-scan fallback (tests/benchmarks
// that bypass the constructor keep working).
func NewManager(cfg Config) *Manager {
	if cfg.Buckets == 0 {
		cfg.Buckets = 256
	}
	if cfg.FetchBytes <= 0 {
		cfg.FetchBytes = 1 << 20
	}

	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}

	interval := cfg.RehashInterval
	if interval == 0 {
		interval = DefaultRehashInterval
	}

	m := &Manager{
		store: cfg.Store, ring: cfg.Ring, self: cfg.Self,
		transport: cfg.Transport, numBuckets: cfg.Buckets, fetchBytes: cfg.FetchBytes,
		clock: clock,
		acc:   make([]atomic.Uint64, cfg.Buckets),
	}

	m.rehashInterval.Store(int64(interval))
	if cfg.Store != nil {
		cfg.Store.SetMutationSink(m.applySink)
	}
	return m
}

// SetHintJournal enables durable hint storage; nil disables journaling.
func (m *Manager) SetHintJournal(j *Journal) {
	m.journal = j
}

// SetMetrics wires the observability collector (nil disables).
func (m *Manager) SetMetrics(c *metrics.Collector) {
	m.met.Store(c)
}

// SetLogger installs the operational logger (nil selects Nop).
func (m *Manager) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	m.logger.Store(l)
}

// log returns the installed logger (Nop when unset).
func (m *Manager) log() *zerolog.Logger {
	if l := m.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// ComputeDigest XOR-folds entry hashes per bucket over a full scan. It
// is the ground truth the incremental accumulators are verified against
// (backstop rehash, tests, benchmarks); round paths use digestForRound.
func (m *Manager) ComputeDigest() Digest {
	digest := make(Digest, m.numBuckets)
	if m.store == nil {
		return digest
	}

	m.store.ForEach(func(key string, e store.Entry) {
		digest[BucketFor(key, m.numBuckets)] ^= EntryHash(key, e.VersionAndTTL(), e.WriterNodeHash)
	})
	return digest
}

// xorAcc folds v into u with a CAS loop (atomic.Uint64 has no Xor).
// XOR is self-inverse, so add and remove both fold the entry hash.
func xorAcc(u *atomic.Uint64, v uint64) {
	for {
		old := u.Load()
		if u.CompareAndSwap(old, old^v) {
			return
		}
	}
}

// applySink is the store mutation feed: exactly one call per accepted
// mutation outcome (overwrite emits old identity then new; self-evicted
// puts emit nothing; evictions/expiry emit the reaped identity).
// Leaf-only: never calls back into the store, never blocks.
func (m *Manager) applySink(key string, versionAndTTL, writer uint64) {
	if len(m.acc) == 0 || m.numBuckets == 0 {
		return
	}

	b := BucketFor(key, m.numBuckets)
	if int(b) >= len(m.acc) {
		return
	}

	m.digestMu.RLock()
	xorAcc(&m.acc[b], EntryHash(key, versionAndTTL, writer))
	m.digestMu.RUnlock()
}

// SetRehashInterval changes the wall-clock backstop; negative disables
// it (tests only). Zero selects DefaultRehashInterval.
func (m *Manager) SetRehashInterval(d time.Duration) {
	if d == 0 {
		d = DefaultRehashInterval
	}
	m.rehashInterval.Store(int64(d))
}

// DetachSink unregisters the incremental feed (tests that share a store
// between managers). Production managers never detach.
func (m *Manager) DetachSink() {
	if m.store != nil {
		m.store.SetMutationSink(nil)
	}
}

// digestForRoundOrScan is the responder-side digest: same incremental
// state as the initiator path, discarding the rehashed flag.
func (m *Manager) digestForRoundOrScan() Digest {
	d, _ := m.digestForRound()
	return d
}

// digestForRound returns the incremental digest, rehashing when cold or
// when the wall-clock backstop is due. The second result reports whether
// a full rehash ran (metric gate). Managers built without a store (or
// without accumulators) always fall back to a full scan without claiming
// a rehash.
func (m *Manager) digestForRound() (Digest, bool) {
	if len(m.acc) == 0 || m.store == nil || m.numBuckets == 0 {
		return m.ComputeDigest(), false
	}

	now := m.clock().UnixNano()
	interval := m.rehashInterval.Load()
	due := !m.accReady.Load() || (interval >= 0 && now-m.lastRehash.Load() >= interval)
	if !due {
		m.digestMu.RLock()
		out := make(Digest, len(m.acc))
		for i := range m.acc {
			out[i] = m.acc[i].Load()
		}

		m.digestMu.RUnlock()
		return out, false
	}

	scan := m.ComputeDigest()
	m.digestMu.Lock()
	mismatch := false
	if m.accReady.Load() {
		for i := range scan {
			if i < len(m.acc) && m.acc[i].Load() != scan[i] {
				mismatch = true
				break
			}
		}
	}
	for i := range scan {
		if i < len(m.acc) {
			m.acc[i].Store(scan[i])
		}
	}

	m.accReady.Store(true)
	m.lastRehash.Store(now)
	m.digestMu.Unlock()
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEFullRehashes.Add(1)
		if mismatch {
			mt.AERehashMismatch.Add(1)
		}
	})
	return scan, true
}

// RunRound runs one initiator round against a replica partner and returns
// the number of entries repaired locally. Missing partners, transport
// failures, and invalid replies leave state unchanged and return zero.
func (m *Manager) RunRound(replicaFactor hashring.ReplicaCount) int {
	partner, ok := m.PickPartner(replicaFactor)
	if !ok {
		return 0
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AERounds.Add(1)
	})
	if e := m.log().Debug(); e != nil {
		e.Str("partner", partner).Msg("anti-entropy round")
	}

	local, _ := m.digestForRound()
	res, err := m.transport.Send(partner, protocol.Request{
		Opcode: protocol.OpAntiEntropyDigest, Value: local.Fold().Encode(),
	})
	if err != nil || res.Status != protocol.StatusOK || !res.HasValue {
		return 0
	}

	groups := (int(m.numBuckets) + BucketsPerGroup - 1) / BucketsPerGroup
	remote, ok := DecodeResponse(res.Value, groups)
	if !ok {
		return 0
	}

	localGroups := local.Fold()
	divergent := false
	var divergentGroups int
	for g := range localGroups {
		if g >= len(remote.GroupHashes) || localGroups[g] != remote.GroupHashes[g] {
			divergent = true
			divergentGroups++
		}
	}
	if !divergent {
		return 0 // converged round ends here
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEGroupMismatch.Add(uint64(divergentGroups))
	})

	var buckets []uint32
	for g := range groups {
		if g >= len(remote.GroupHashes) || localGroups[g] != remote.GroupHashes[g] {
			for b := g * BucketsPerGroup; b < (g+1)*BucketsPerGroup && b < int(m.numBuckets); b++ {
				buckets = append(buckets, uint32(b))
			}
		}
	}

	req := Refinement{}
	for _, b := range buckets {
		req.BucketHashes = append(req.BucketHashes, BucketHash{Bucket: b, Hash: local[b]})
	}

	ref, err := m.transport.Send(partner, protocol.Request{
		Opcode: protocol.OpAntiEntropyDigest, Key: []byte(RefinementKey), Value: req.Encode(),
	})
	if err != nil || ref.Status != protocol.StatusOK || !ref.HasValue {
		return 0
	}

	refined, ok := DecodeResponse(ref.Value, groups)
	if !ok {
		return 0
	}

	n := m.finishRefinement(local, partner, refined)
	if e := m.log().Debug(); e != nil {
		e.Str("partner", partner).Int("entries", n).Msg("anti-entropy repaired")
	}
	return n
}

func (m *Manager) mustGet(key string) store.Entry {
	e, _ := m.store.GetVersioned(key)
	return e
}

// PickPartner round-robins over replica sharers, refreshing the set when
// empty. Nil when RF<=1 or nobody shares keys with this node.
func (m *Manager) PickPartner(replicaFactor hashring.ReplicaCount) (string, bool) {
	if replicaFactor <= 1 {
		return "", false
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.partners) == 0 {
		m.refreshPartnersLocked(replicaFactor)
	}
	if len(m.partners) == 0 {
		return "", false
	}

	p := m.partners[m.partnerIdx%len(m.partners)]
	m.partnerIdx++
	return p, true
}

func (m *Manager) refreshPartnersLocked(replicaFactor hashring.ReplicaCount) {
	seen := map[string]bool{}
	m.store.ForEach(func(key string, _ store.Entry) {
		nodes, err := m.ring.GetNodes(key, replicaFactor)
		if err != nil {
			return
		}
		for _, n := range nodes {
			if n != m.self {
				seen[n] = true
			}
		}
	})

	// Counter-only keys do not appear in the LWW scan. Include them so a
	// cluster using only CRDT counters still runs the repair exchange.
	m.counterMu.Lock()
	cs := m.counters
	m.counterMu.Unlock()
	if cs != nil {
		for _, e := range cs.SortedLiveCounters() {
			nodes, err := m.ring.GetNodes(e.Key, replicaFactor)
			if err != nil {
				continue
			}
			for _, n := range nodes {
				if n != m.self {
					seen[n] = true
				}
			}
			if len(seen) > 64 {
				break
			}
		}
	}

	m.partners = m.partners[:0]
	for n := range seen {
		m.partners = append(m.partners, n)
	}

	slices.Sort(m.partners)
	m.partnerIdx = 0
}
