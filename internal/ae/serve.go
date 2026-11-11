package ae

import (
	"cmp"
	"slices"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
)

func (m *Manager) OnDigestRequest(value []byte) (protocol.Status, []byte) {
	groups := (int(m.numBuckets) + BucketsPerGroup - 1) / BucketsPerGroup
	if _, ok := DecodeGroupDigest(value, groups); !ok {
		return protocol.StatusInvalidArgument, nil
	}

	resp := DigestResponse{GroupHashes: m.digestForRoundOrScan().Fold()}
	return protocol.StatusOK, resp.Encode()
}

// OnBucketRefinement returns hashes and either explicit metadata rows or
// an IBLT sketch for each requested bucket, depending on bucket size.
func (m *Manager) OnBucketRefinement(value []byte) (protocol.Status, []byte) {
	req, ok := DecodeRefinement(value, m.numBuckets)
	if !ok {
		return protocol.StatusInvalidArgument, nil
	}

	local := m.digestForRoundOrScan()
	resp := DigestResponse{GroupHashes: local.Fold()}
	requested := map[uint32]bool{}
	for _, bh := range req.BucketHashes {
		resp.BucketHashes = append(resp.BucketHashes, BucketHash{Bucket: bh.Bucket, Hash: local[bh.Bucket]})
		if bh.Hash != local[bh.Bucket] {
			requested[bh.Bucket] = true
		}
	}

	slices.SortFunc(resp.BucketHashes, func(a, b BucketHash) int { return cmp.Compare(a.Bucket, b.Bucket) })

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
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.AEMetaSent.Add(uint64(len(rows)))
		})
		return protocol.StatusOK, resp.Encode()
	}
	return m.respondRefinement(resp, requestedList)
}

func (m *Manager) respondRefinement(resp DigestResponse, requested []uint32) (protocol.Status, []byte) {
	perBucket := m.CollectRows(requested)
	for _, b := range requested {
		resp.Covered = append(resp.Covered, b)
		vec := perBucket[b]
		if len(vec) >= IBLTMinRows {
			items := make([]IBLTItem, 0, len(vec))
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

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEMetaSent.Add(uint64(len(resp.Rows)))
	})
	return protocol.StatusOK, resp.Encode()
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

	out, err := resp.Encode()
	if err != nil {
		return protocol.StatusInternalError, nil
	}
	return protocol.StatusOK, out
}
