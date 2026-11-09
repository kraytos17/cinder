package ae

import (
	"slices"
	"sort"
	"sync"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// Transport sends one request; client.Pool satisfies it.
type Transport interface {
	Send(node string, req protocol.Request) (protocol.Response, error)
}

// Manager runs anti-entropy rounds: digest exchange, bucket refinement,
// targeted sync. It mirrors AntiEntropyManager's protocol exactly; the
// rehash is a plain full scan every round (no divert-while-scanning),
// which is behaviorally identical.
type Manager struct {
	store      *store.LRUStore
	ring       *hashring.Ring
	self       string
	transport  Transport
	numBuckets uint32
	fetchBytes int

	mu         sync.Mutex
	partners   []string
	partnerIdx int
	journal    *Journal

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
func NewManager(st *store.LRUStore, ring *hashring.Ring, self string, t Transport, numBuckets uint32, fetchBytes int) *Manager {
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
	for g := range localGroups {
		if g >= len(remote.GroupHashes) || localGroups[g] != remote.GroupHashes[g] {
			divergent = true
			break
		}
	}
	if !divergent {
		return 0 // converged round ends here
	}

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
	return m.finishRefinement(local, partner, refined)
}

// finishRefinement resolves IBLT sketches (with P2 fallback), diffs
// metadata, pushes winners, fetches losers, and applies the result.
func (m *Manager) finishRefinement(local []uint64, partner string, resp DigestResponse) int {
	var reconstructed []MetaRow
	var reconstructedCovered []uint32
	sketchHasDiff := map[uint32]bool{}
	coveredSet := map[uint32]bool{}
	for _, b := range resp.Covered {
		coveredSet[b] = true
		reconstructedCovered = append(reconstructedCovered, b)
	}

	var failed []uint32
	for _, sk := range resp.Sketches {
		if sk.Bucket >= m.numBuckets || !coveredSet[sk.Bucket] {
			failed = append(failed, sk.Bucket)
			continue
		}

		only := []uint32{sk.Bucket}
		blob, covered := m.CollectMetadata(only, MaxMetaBytes)
		localRows, ok := DecodeMetaRows(blob)
		remoteSketch := DecodeIBLT(sk.Blob)
		var localSketch []IBLTCell
		if ok && len(covered) == 1 {
			var items []IBLTItem
			for _, r := range localRows {
				items = append(items, IBLTItem{KeyHash: r.KeyHash, Version: r.Version, Writer: r.Writer, Tombstone: r.Tombstone})
			}
			localSketch = BuildIBLT(items, IBLTCells)
		}

		diff, ok := IBLTDiff{}, false
		if remoteSketch != nil && localSketch != nil {
			diff, ok = DifferenceIBLT(remoteSketch, localSketch)
		}
		if !ok {
			failed = append(failed, sk.Bucket)
			continue
		}

		// The remote bucket hash must differ when the diff is empty, or
		// the sketch told us nothing new (stale or dishonest peer).
		var remoteHash uint64
		foundHash := false
		for _, h := range resp.BucketHashes {
			if h.Bucket == sk.Bucket {
				remoteHash, foundHash = h.Hash, true
			}
		}
		if !foundHash || (local[sk.Bucket] == remoteHash && len(diff.RemoteOnly) == 0 && len(diff.LocalOnly) == 0) {
			failed = append(failed, sk.Bucket)
			continue
		}

		removed := map[uint64]bool{}
		for _, r := range diff.LocalOnly {
			removed[r.KeyHash] = true
		}
		for _, r := range localRows {
			if !removed[r.KeyHash] {
				reconstructed = append(reconstructed, r)
			}
		}
		for _, r := range diff.RemoteOnly {
			reconstructed = append(reconstructed, MetaRow{Bucket: sk.Bucket, KeyHash: r.KeyHash, Version: r.Version, Writer: r.Writer, Tombstone: r.Tombstone})
		}
		sketchHasDiff[sk.Bucket] = len(diff.RemoteOnly) > 0 || len(diff.LocalOnly) > 0
	}

	slices.Sort(failed)
	failed = uniqU32(failed)
	if len(failed) > 0 {
		fbReq := Refinement{ForceMeta: true}
		for _, b := range failed {
			if b < uint32(len(local)) {
				fbReq.BucketHashes = append(fbReq.BucketHashes, BucketHash{Bucket: b, Hash: local[b]})
			}
		}

		fb, err := m.transport.Send(partner, protocol.Request{
			Opcode: protocol.OpAntiEntropyDigest, Key: []byte(RefinementKey), Value: EncodeRefinement(fbReq),
		})

		if err != nil || fb.Status != protocol.StatusOK || !fb.HasValue {
			return 0
		}

		groups := (int(m.numBuckets) + BucketsPerGroup - 1) / BucketsPerGroup
		fbResp, ok := DecodeResponse(fb.Value, groups)
		if !ok {
			return 0
		}

		// Merge: keep successfully-sketched rows, replace failed buckets
		// with explicit fallback rows; covered = survivors + fallback.
		failedSet := map[uint32]bool{}
		for _, b := range failed {
			failedSet[b] = true
		}

		kept := reconstructed[:0]
		for _, r := range reconstructed {
			if !failedSet[r.Bucket] {
				kept = append(kept, r)
			}
		}

		reconstructed = append(kept, fbResp.Rows...)
		coveredSet := map[uint32]bool{}
		for _, b := range reconstructedCovered {
			if !failedSet[b] {
				coveredSet[b] = true
			}
		}
		for _, b := range fbResp.Covered {
			coveredSet[b] = true
		}

		reconstructedCovered = reconstructedCovered[:0]
		for b := range coveredSet {
			reconstructedCovered = append(reconstructedCovered, b)
		}
		slices.Sort(reconstructedCovered)
	}
	return m.finishSync(local, partner, resp, reconstructed, reconstructedCovered, sketchHasDiff)
}

func uniqU32(in []uint32) []uint32 {
	if len(in) == 0 {
		return in
	}

	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// finishSync validates coverage, diffs, pushes winners and fetches losers.
func (m *Manager) finishSync(local []uint64, partner string, resp DigestResponse, rows []MetaRow, covered []uint32, sketchHasDiff map[uint32]bool) int {
	remoteBuckets := map[uint32]uint64{}
	remoteSeen := map[uint32]bool{}
	for _, bh := range resp.BucketHashes {
		if bh.Bucket >= m.numBuckets || remoteSeen[bh.Bucket] {
			return 0
		}

		remoteSeen[bh.Bucket] = true
		remoteBuckets[bh.Bucket] = bh.Hash
	}

	sketched := map[uint32]bool{}
	for _, s := range resp.Sketches {
		if s.Bucket >= m.numBuckets || sketched[s.Bucket] {
			return 0
		}
		sketched[s.Bucket] = true
	}
	slices.Sort(covered)
	coveredSet := map[uint32]bool{}
	for _, b := range covered {
		coveredSet[b] = true
	}
	for _, r := range rows {
		if r.Bucket >= m.numBuckets || !coveredSet[r.Bucket] {
			return 0
		}
	}
	var divergent []uint32
	for b := uint32(0); b < m.numBuckets; b++ {
		if h, ok := remoteBuckets[b]; ok && h != local[b] {
			divergent = append(divergent, b)
		} else if sketched[b] && coveredSet[b] && sketchHasDiff[b] {
			divergent = append(divergent, b)
		}
	}
	if len(divergent) == 0 {
		return 0
	}
	for _, b := range divergent {
		if !coveredSet[b] {
			return 0
		}
	}
	for _, r := range rows {
		found := slices.Contains(divergent, r.Bucket)
		if !found {
			return 0
		}
	}
	diff := m.DiffRows(rows, divergent, covered)
	syncReq := SyncMessage{Needs: diff.Need}
	for _, h := range diff.Have {
		syncReq.Entries = append(syncReq.Entries, SyncItem{Key: h.Key, Entry: mustGet(m, h.Key)})
	}
	_ = syncReq
	encoded, err := EncodeSync(SyncMessage{Entries: syncReq.Entries, Needs: syncReq.Needs})
	if err != nil {
		return 0
	}
	sr, err := m.transport.Send(partner, protocol.Request{Opcode: protocol.OpAntiEntropySync, Value: encoded})
	if err != nil || sr.Status != protocol.StatusOK || !sr.HasValue {
		return 0
	}
	resp2, ok := DecodeSync(sr.Value)
	if !ok || len(resp2.Needs) != 0 {
		return 0
	}
	return m.ApplySync(resp2)
}

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
	sort.Strings(m.partners)
	m.partnerIdx = 0
}

// CollectRows gathers per-bucket metadata rows sorted by key hash.
func (m *Manager) CollectRows(buckets []uint32) [][]MetaRow {
	wanted := make([]bool, m.numBuckets)
	for _, id := range buckets {
		if id < m.numBuckets {
			wanted[id] = true
		}
	}
	perBucket := make([][]MetaRow, m.numBuckets)
	m.store.ForEach(func(key string, e store.Entry) {
		b := BucketFor(key, m.numBuckets)
		if b >= m.numBuckets || !wanted[b] {
			return
		}
		perBucket[b] = append(perBucket[b], MetaRow{
			Bucket: b, Tombstone: e.Tombstone, KeyHash: KeyHash(key),
			Version: e.Version, Writer: e.WriterNodeHash,
		})
	})
	for _, vec := range perBucket {
		sort.Slice(vec, func(i, j int) bool { return vec[i].KeyHash < vec[j].KeyHash })
	}
	return perBucket
}

// metaRowBytes is the on-disk metadata row width.
const metaRowBytes = 4 + 8 + 8 + 8 + 1

// CollectMetadata encodes rows for buckets, filling bucket-atomically up
// to maxBytes. Empty buckets still count as covered. Returns the blob and
// the covered set, mirroring the C++ byte-budget rules including the
// first-bucket-overflow exception.
func (m *Manager) CollectMetadata(bucketIDs []uint32, maxBytes int) ([]byte, []uint32) {
	perBucket := m.CollectRows(bucketIDs)
	wanted := make([]bool, m.numBuckets)
	for _, id := range bucketIDs {
		if id < m.numBuckets {
			wanted[id] = true
		}
	}
	var rows []MetaRow
	var covered []uint32
	bytes := 8 // outer row-count + covered-count u32s
	for b := uint32(0); b < m.numBuckets; b++ {
		if !wanted[b] {
			continue
		}
		vec := perBucket[b]
		if bytes+4 > maxBytes && len(covered) > 0 {
			break
		}
		bytes += 4
		covered = append(covered, b)
		need := len(vec) * metaRowBytes
		if bytes+need > maxBytes && len(covered) > 1 {
			covered = covered[:len(covered)-1]
			break
		}
		bytes += need
		rows = append(rows, vec...)
		if bytes > maxBytes {
			break
		}
	}
	return EncodeMetaRows(rows), covered
}

// HavePair sorts keys by identity hash for deterministic output.
type HavePair struct {
	Hash uint64
	Key  string
}

// Diff holds reconcile sets: hashes to fetch (remote wins) and local
// pairs to push (local wins by absence or LWW).
type Diff struct {
	Need []uint64
	Have []HavePair
}

// DiffRows compares remote metadata against the local store scoped to
// divergent buckets. See ae.Diff for the win/loss rules.
func (m *Manager) DiffRows(rows []MetaRow, divergent, covered []uint32) Diff {
	var diff Diff
	if len(divergent) == 0 {
		return diff
	}
	wanted := make([]bool, m.numBuckets)
	for _, id := range divergent {
		if id < m.numBuckets {
			wanted[id] = true
		}
	}
	byHash := make(map[uint64]*MetaRow, len(rows))
	for i := range rows {
		byHash[rows[i].KeyHash] = &rows[i] // duplicate hashes keep the last
	}
	coverAll := len(covered) == 0
	coveredSet := make(map[uint32]bool, len(covered))
	for _, b := range covered {
		if b < m.numBuckets {
			coveredSet[b] = true
		}
	}
	m.store.ForEach(func(key string, e store.Entry) {
		b := BucketFor(key, m.numBuckets)
		if b >= m.numBuckets || !wanted[b] {
			return
		}
		h := KeyHash(key)
		row, ok := byHash[h]
		if !ok {
			if coverAll || coveredSet[b] {
				diff.Have = append(diff.Have, HavePair{Hash: h, Key: key})
			}
			return
		}
		remote := store.Stamp{Version: row.Version, Writer: row.Writer, Tombstone: row.Tombstone}
		local := store.Stamp{Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone}
		if store.LWWWinsOver(remote, local) {
			diff.Need = append(diff.Need, h)
		} else if store.LWWWinsOver(local, remote) {
			diff.Have = append(diff.Have, HavePair{Hash: h, Key: key})
		}
		delete(byHash, h)
	})
	for h, row := range byHash {
		if row.Bucket < m.numBuckets && (coverAll || coveredSet[row.Bucket]) {
			diff.Need = append(diff.Need, h)
		}
	}
	slices.Sort(diff.Need)
	sort.Slice(diff.Have, func(i, j int) bool { return diff.Have[i].Hash < diff.Have[j].Hash })
	return diff
}

// ApplySync applies entries via putVersioned, counting actually-newer ones.
func (m *Manager) ApplySync(msg SyncMessage) int {
	applied := 0
	for _, item := range msg.Entries {
		e, ok := m.store.GetVersioned(item.Key)
		if !ok || store.LWWWinsOver(
			store.Stamp{Version: item.Entry.Version, Writer: item.Entry.WriterNodeHash, Tombstone: item.Entry.Tombstone},
			store.Stamp{Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone}) {
			applied++
		}
		_ = m.store.PutVersioned(item.Key, item.Entry)
	}
	return applied
}

// OnDigestRequest answers a group digest with our own group hashes.
func (m *Manager) OnDigestRequest(value []byte) (protocol.Status, []byte) {
	groups := (int(m.numBuckets) + BucketsPerGroup - 1) / BucketsPerGroup
	remote, ok := DecodeGroupDigest(value, groups)
	_ = remote
	if !ok {
		return protocol.StatusInvalidArgument, nil
	}
	resp := DigestResponse{GroupHashes: GroupFold(m.ComputeDigest())}
	return protocol.StatusOK, EncodeResponse(resp)
}

// OnBucketRefinement answers bucket hashes plus metadata rows (small
// buckets) or IBLT sketches (buckets with >= 32 rows).
func (m *Manager) OnBucketRefinement(value []byte) (protocol.Status, []byte) {
	req, ok := DecodeRefinement(value, m.numBuckets)
	if !ok {
		return protocol.StatusInvalidArgument, nil
	}
	local := m.ComputeDigest()
	resp := DigestResponse{GroupHashes: GroupFold(local)}
	requested := map[uint32]bool{}
	for _, bh := range req.BucketHashes {
		resp.BucketHashes = append(resp.BucketHashes, BucketHash{Bucket: bh.Bucket, Hash: local[bh.Bucket]})
		if bh.Hash != local[bh.Bucket] {
			requested[bh.Bucket] = true
		}
	}
	sort.Slice(resp.BucketHashes, func(i, j int) bool { return resp.BucketHashes[i].Bucket < resp.BucketHashes[j].Bucket })
	var requestedList []uint32
	for b := range requested {
		requestedList = append(requestedList, b)
	}
	slices.Sort(requestedList)
	if req.ForceMeta {
		blob, covered := m.CollectMetadata(requestedList, MaxMetaBytes)
		rows, ok := DecodeMetaRows(blob)
		if !ok {
			return protocol.StatusInternalError, nil
		}
		resp.Covered = covered
		resp.Rows = rows
		return protocol.StatusOK, EncodeResponse(resp)
	}
	return m.respondRefinement(resp, requestedList)
}

func (m *Manager) respondRefinement(resp DigestResponse, requested []uint32) (protocol.Status, []byte) {
	perBucket := m.CollectRows(requested)
	for _, b := range requested {
		resp.Covered = append(resp.Covered, b)
		vec := perBucket[b]
		if len(vec) >= IBLTMinRows {
			var items []IBLTItem
			for _, r := range vec {
				items = append(items, IBLTItem{KeyHash: r.KeyHash, Version: r.Version, Writer: r.Writer, Tombstone: r.Tombstone})
			}
			if sk := BuildIBLT(items, IBLTCells); sk != nil {
				resp.Sketches = append(resp.Sketches, SketchRef{Bucket: b, Blob: EncodeIBLT(sk)})
				continue
			}
		}
		resp.Rows = append(resp.Rows, vec...)
	}
	return protocol.StatusOK, EncodeResponse(resp)
}

// OnSyncRequest applies entries and serves the needlist.
func (m *Manager) OnSyncRequest(value []byte) (protocol.Status, []byte) {
	msg, ok := DecodeSync(value)
	if !ok {
		return protocol.StatusInvalidArgument, nil
	}
	needs := msg.Needs
	msg.Needs = nil
	m.ApplySync(msg)
	var resp SyncMessage
	if len(needs) > 0 {
		resp.Entries = m.collectEntriesForHashes(needs)
	}
	out, err := EncodeSync(resp)
	if err != nil {
		return protocol.StatusInternalError, nil
	}
	return protocol.StatusOK, out
}

// hashKeyPair sorts keys by identity hash for deterministic output.
type hashKeyPair struct {
	h   uint64
	key string
}

func (m *Manager) collectEntriesForHashes(hashes []uint64) []SyncItem {
	if len(hashes) == 0 {
		return nil
	}
	sorted := append([]uint64(nil), hashes...)
	slices.Sort(sorted)
	sorted = uniqU64(sorted)
	resolved := map[uint64]string{}
	m.store.ForEach(func(key string, _ store.Entry) {
		h := KeyHash(key)
		i := sort.Search(len(sorted), func(i int) bool { return sorted[i] >= h })
		if i < len(sorted) && sorted[i] == h {
			if _, ok := resolved[h]; !ok {
				resolved[h] = key
			}
		}
	})
	var ps []hashKeyPair
	for h, k := range resolved {
		ps = append(ps, hashKeyPair{h, k})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].h < ps[j].h })
	var keys []string
	for _, p := range ps {
		keys = append(keys, p.key)
	}
	return m.CollectEntriesForKeys(keys, m.fetchBytes)
}

// CollectEntriesForKeys fills entries for keys up to maxBytes; the first
// over-budget entry still rides (over-budget once beats stalling forever).
func (m *Manager) CollectEntriesForKeys(keys []string, maxBytes int) []SyncItem {
	ps := make([]hashKeyPair, 0, len(keys))
	for _, k := range keys {
		ps = append(ps, hashKeyPair{KeyHash(k), k})
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].h < ps[j].h })
	var items []SyncItem
	bytes := 0
	for _, p := range ps {
		e, ok := m.store.GetVersioned(p.key)
		if !ok {
			continue
		}
		cost := 4 + len(p.key) + 8 + 8 + 1 + 4 + len(e.Value)
		if e.HasTTL {
			cost += 8
		}
		if bytes+cost > maxBytes && len(items) > 0 {
			break
		}
		bytes += cost
		items = append(items, SyncItem{Key: p.key, Entry: e})
		if bytes > maxBytes {
			break
		}
	}
	return items
}

func uniqU64(in []uint64) []uint64 {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}
