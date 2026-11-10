package ae

// Bucket refinement: the peeled-sketch round that narrows
// divergent buckets before the metadata exchange.

import (
	"slices"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
)

func (m *Manager) finishRefinement(local []uint64, partner string, resp DigestResponse) int {
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.AEMetaRecv.Add(uint64(len(resp.Rows)))
	})
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
	failed = slices.Compact(failed)
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		// Sketches that survived peeling vs buckets falling back to
		// explicit metadata.
		mt.AEIBLTFallback.Add(uint64(len(failed)))
		mt.AEIBLTOk.Add(uint64(len(resp.Sketches) - len(failed)))
	})
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

		reconstructed = append(reconstructed[:len(kept)], fbResp.Rows...)
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

// finishSync validates coverage, diffs, pushes winners and fetches losers.
