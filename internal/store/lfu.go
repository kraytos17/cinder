// LFU eviction: per-shard frequency buckets, mirroring C++ LfuStore.
//
// Each shard maps frequency -> node indices in least-recently-incremented
// order. A node's freqIdx tracks its position for O(1) swap-remove.
// Victim choice scans buckets ascending and takes the first live entry,
// so ties resolve least-recently-incremented-first. Tombstones are exempt
// (GC reaps them); an all-tombstone store evicts nothing.
//
// Eviction scans the small per-shard frequency map to find the lowest
// bucket containing a live entry. This keeps tombstone-only buckets intact
// (their entries still need consistent freqIdx bookkeeping) and makes the
// selection rule explicit; frequency cardinality is bounded by accesses,
// while eviction is not the request hot path.
package store

import (
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/sharding"
)

// freqInit plants a fresh node at frequency 1. Mirrors C++ insertNew's
// freq setup (freq 1, appended to bucket 1). Caller holds sh.mu; the
// caller must have already assigned sh.index[key] = idx.
func (s *Store) freqInit(sh *shard, idx int) {
	n := &sh.nodes[idx]
	n.freq = 1
	n.freqIdx = len(sh.freqBuckets[1])
	sh.freqBuckets[1] = append(sh.freqBuckets[1], idx)
}

// incrementFreq moves idx from its current frequency bucket to freq+1,
// appending at the end (least-recently-incremented order within a
// bucket). Mirrors C++ incrementFreq. Caller holds sh.mu.
func (s *Store) incrementFreq(sh *shard, idx int) {
	n := &sh.nodes[idx]
	old := n.freq
	n.freq = old + 1
	s.removeFromFreqBucket(sh, idx, old)
	n.freqIdx = len(sh.freqBuckets[n.freq])
	sh.freqBuckets[n.freq] = append(sh.freqBuckets[n.freq], idx)
}

// removeFromFreqBucket unlinks idx from the given bucket via swap-remove,
// fixing the swapped node's index. No-op when the bucket is gone.
// Mirrors C++ removeFromFreqBucket. Caller holds sh.mu.
func (s *Store) removeFromFreqBucket(sh *shard, idx int, freq uint64) {
	vec, ok := sh.freqBuckets[freq]
	if !ok {
		return
	}
	pos := sh.nodes[idx].freqIdx
	last := len(vec) - 1
	if pos != last {
		vec[pos] = vec[last]
		sh.nodes[vec[pos]].freqIdx = pos
	}
	if vec = vec[:last]; len(vec) == 0 {
		delete(sh.freqBuckets, freq)
	} else {
		sh.freqBuckets[freq] = vec
	}
}

// lfuEvictOne removes the lowest-frequency live entry, scanning buckets
// ascending and taking the first non-tombstone in
// least-recently-incremented order. Returns bytes freed, or 0 when only
// tombstones remain (their GC deadline reaps them instead). Mirrors C++
// LfuStore::evictOne. Caller holds sh.mu.
func (s *Store) lfuEvictOne(sh *shard) uint64 {
	var minFreq uint64
	victim := -1
	for freq, vec := range sh.freqBuckets {
		for i, idx := range vec {
			if sh.nodes[idx].entry.Tombstone {
				continue
			}
			if victim < 0 || freq < minFreq {
				minFreq, victim = freq, i
			}
			break // bucket is ordered by last increment; first live wins ties
		}
	}
	if victim >= 0 {
		vec := sh.freqBuckets[minFreq]
		idx := vec[victim]
		obs.IfPresent(&s.met, func(m *metrics.Collector) { m.EvictionsCap.Add(1) })
		return s.removeNode(sh, idx)
	}
	return 0
}

// RestoreFreq raises a resident's frequency to freq (snapshot recovery).
// Never demotes: absent keys and newer residents keep their count, and it
// is a no-op under LRUPolicy. Mirrors C++ restoreFreq.
func (s *Store) RestoreFreq(key string, freq uint64) {
	if s.policy != LFUPolicy || freq <= 1 {
		return
	}
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	idx, ok := sh.index[key]
	if !ok || sh.nodes[idx].freq >= freq {
		return
	}
	s.removeFromFreqBucket(sh, idx, sh.nodes[idx].freq)
	sh.nodes[idx].freq = freq
	sh.nodes[idx].freqIdx = len(sh.freqBuckets[freq])
	sh.freqBuckets[freq] = append(sh.freqBuckets[freq], idx)
}

// EntryFreq returns a resident's LFU frequency, or 0 when absent or when
// the store uses LRUPolicy. Primarily useful to persistence and tests.
func (s *Store) EntryFreq(key string) uint64 {
	if s.policy != LFUPolicy {
		return 0
	}
	sh := &s.shards[sharding.ForKey(key, len(s.shards), shardSeed)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if idx, ok := sh.index[key]; ok {
		return sh.nodes[idx].freq
	}
	return 0
}
