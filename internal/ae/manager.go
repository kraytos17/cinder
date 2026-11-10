package ae

import (
	"slices"
	"sync"
	"sync/atomic"

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

// Manager runs anti-entropy rounds: digest exchange, bucket refinement,
// targeted sync. It mirrors AntiEntropyManager's protocol exactly; the
// rehash is a plain full scan every round (no divert-while-scanning),
// which is behaviorally identical.

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

	counters   CounterStore
	counterMu  sync.Mutex
	counterCur string
}

// CounterStore is the subset of *counter.Store the AE manager needs.
// *counter.Store satisfies it; tests stub it.

type CounterStore interface {
	SortedLiveCounters() []BundleEntry
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

// NewManager returns a manager. numBuckets 0 selects 256; fetchBytes 0
// selects 1 MiB.

func NewManager(st *store.Store, ring *hashring.Ring, self string, t Transport, numBuckets uint32, fetchBytes int) *Manager {
	if numBuckets == 0 {
		numBuckets = 256
	}
	if fetchBytes <= 0 {
		fetchBytes = 1 << 20
	}
	return &Manager{store: st, ring: ring, self: self, transport: t, numBuckets: numBuckets, fetchBytes: fetchBytes}
}

// SetHintJournal wires durable hint storage; implemented with journal.go.

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

// ComputeDigest XOR-folds entry hashes per bucket over a full scan.

func (m *Manager) ComputeDigest() []uint64 {
	digest := make([]uint64, m.numBuckets)
	m.store.ForEach(func(key string, e store.Entry) {
		digest[BucketFor(key, m.numBuckets)] ^= EntryHash(key, e.VersionAndTTL(), e.WriterNodeHash)
	})
	return digest
}

// RunRound runs one initiator round against a replica partner and
// returns the number of entries repaired locally. Transport and decode
// failures are best-effort no-ops (0), mirroring the C++ fire-and-log
// behavior.

func (m *Manager) RunRound(replicaFactor int) int {
	partner, ok := m.PickPartner(replicaFactor)
	if !ok {
		return 0
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AERounds.Add(1)
		mt.AEFullRehashes.Add(1)
	})
	if e := m.log().Debug(); e != nil {
		e.Str("partner", partner).Msg("anti-entropy round")
	}

	local := m.ComputeDigest()
	res, err := m.transport.Send(partner, protocol.Request{
		Opcode: protocol.OpAntiEntropyDigest, Value: EncodeGroupDigest(GroupFold(local)),
	})

	if err != nil || res.Status != protocol.StatusOK || !res.HasValue {
		return 0
	}

	groups := (int(m.numBuckets) + BucketsPerGroup - 1) / BucketsPerGroup
	remote, ok := DecodeResponse(res.Value, groups)
	if !ok {
		return 0
	}

	localGroups := GroupFold(local)
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
		Opcode: protocol.OpAntiEntropyDigest, Key: []byte(RefinementKey), Value: EncodeRefinement(req),
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

// finishRefinement resolves IBLT sketches (with P2 fallback), diffs
// metadata, pushes winners, fetches losers, and applies the result.

func mustGet(m *Manager, key string) store.Entry {
	e, _ := m.store.GetVersioned(key)
	return e
}

// PickPartner round-robins over replica sharers, refreshing the set when
// empty. Nil when RF<=1 or nobody shares keys with this node.

func (m *Manager) PickPartner(replicaFactor int) (string, bool) {
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

func (m *Manager) refreshPartnersLocked(replicaFactor int) {
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
