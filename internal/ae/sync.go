package ae

import (
	"cmp"
	"slices"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/store"
)

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
	for b := range m.numBuckets {
		if h, ok := remoteBuckets[b]; ok && h != local[b] {
			divergent = append(divergent, b)
		} else if sketched[b] && coveredSet[b] && sketchHasDiff[b] {
			divergent = append(divergent, b)
		}
	}
	if len(divergent) == 0 {
		return 0
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEBucketMismatch.Add(uint64(len(divergent)))
	})
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
		syncReq.Entries = append(syncReq.Entries, SyncItem{Key: h.Key, Entry: m.mustGet(h.Key)})
	}

	encoded, err := SyncMessage{Entries: syncReq.Entries, Needs: syncReq.Needs}.Encode()
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

// CollectRows gathers per-bucket metadata rows sorted by key hash.
func (m *Manager) CollectRows(buckets []uint32) [][]MetaRow {
	wanted := make([]bool, m.numBuckets)
	for _, id := range buckets {
		if id < m.numBuckets {
			wanted[id] = true
		}
	}

	perBucket := make([][]MetaRow, m.numBuckets)
	for _, item := range m.store.MetadataSnapshot() {
		key, e := item.Key, item.Entry
		b := BucketFor(key, m.numBuckets)
		if b >= m.numBuckets || !wanted[b] {
			continue
		}

		perBucket[b] = append(perBucket[b], MetaRow{
			Bucket: b, Tombstone: e.Tombstone, KeyHash: KeyHash(key),
			Version: e.Version, Writer: e.WriterNodeHash,
		})
	}
	for _, vec := range perBucket {
		slices.SortFunc(vec, func(a, b MetaRow) int { return cmp.Compare(a.KeyHash, b.KeyHash) })
	}
	return perBucket
}

// metaRowBytes is the on-disk metadata row width.
const metaRowBytes = 4 + 8 + 8 + 8 + 1

// CollectMetadata encodes rows for buckets, filling bucket-atomically up
// to maxBytes. Empty buckets still count as covered. Returns the blob and
// the covered set. The first bucket may exceed maxBytes so that a bucket
// larger than the budget can still make progress.
func (m *Manager) CollectMetadata(bucketIDs []uint32, maxBytes int) ([]byte, []uint32) {
	requested := make([]bool, m.numBuckets)
	for _, b := range bucketIDs {
		if b < m.numBuckets {
			requested[b] = true
		}
	}

	wanted := make([]bool, m.numBuckets)
	var rows []MetaRow
	rowCounts := make([]int, m.numBuckets)
	m.store.ForEachMetadata(func(key string, e store.Entry) {
		b := BucketFor(key, m.numBuckets)
		if b >= m.numBuckets || !requested[b] {
			return
		}

		rows = append(rows, MetaRow{Bucket: b, Tombstone: e.Tombstone, KeyHash: KeyHash(key), Version: e.Version, Writer: e.WriterNodeHash})
		rowCounts[b]++
	})
	slices.SortFunc(rows, func(a, b MetaRow) int {
		return cmp.Or(cmp.Compare(a.Bucket, b.Bucket), cmp.Compare(a.KeyHash, b.KeyHash))
	})

	var covered []uint32
	bytes := 8 // outer row-count + covered-count u32s
	for b := range m.numBuckets {
		if !requested[b] {
			continue
		}
		if bytes+4 > maxBytes && len(covered) > 0 {
			break
		}

		bytes += 4
		covered = append(covered, b)
		wanted[b] = true
		need := rowCounts[b] * metaRowBytes
		if bytes+need > maxBytes && len(covered) > 1 {
			covered = covered[:len(covered)-1]
			wanted[b] = false
			break
		}

		bytes += need
		if bytes > maxBytes {
			break
		}
	}

	rowEnd := 0
	for _, b := range covered {
		rowEnd += rowCounts[b]
	}
	return EncodeMetaRows(rows[:rowEnd]), covered
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
	for _, item := range m.store.MetadataSnapshot() {
		key, e := item.Key, item.Entry
		b := BucketFor(key, m.numBuckets)
		if b >= m.numBuckets || !wanted[b] {
			continue
		}

		h := KeyHash(key)
		row, ok := byHash[h]
		if !ok {
			if coverAll || coveredSet[b] {
				diff.Have = append(diff.Have, HavePair{Hash: h, Key: key})
			}
			continue
		}

		remote := row.Stamp()
		local := e.Stamp()
		if store.LWWWinsOver(remote, local) {
			diff.Need = append(diff.Need, h)
		} else if store.LWWWinsOver(local, remote) {
			diff.Have = append(diff.Have, HavePair{Hash: h, Key: key})
		}
		delete(byHash, h)
	}
	for h, row := range byHash {
		if row.Bucket < m.numBuckets && (coverAll || coveredSet[row.Bucket]) {
			diff.Need = append(diff.Need, h)
		}
	}

	slices.Sort(diff.Need)
	slices.SortFunc(diff.Have, func(a, b HavePair) int { return cmp.Compare(a.Hash, b.Hash) })
	return diff
}

// ApplySync applies entries via putVersioned, counting actually-newer ones.
func (m *Manager) ApplySync(msg SyncMessage) int {
	applied := 0
	for _, item := range msg.Entries {
		e, ok := m.store.GetVersioned(item.Key)
		if !ok || store.LWWWinsOver(
			item.Entry.Stamp(),
			e.Stamp()) {
			applied++
		}
		_ = m.store.PutVersioned(item.Key, item.Entry)
	}

	// Single funnel for repair accounting (initiator + responder +
	// direct callers): every applied entry converged here.
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEKeysRepaired.Add(uint64(applied))
		mt.AEFetched.Add(uint64(len(msg.Entries)))
	})
	return applied
}
