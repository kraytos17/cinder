package store

import (
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/obs"
)

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

// onAccess records a live read (Get) or accepted overwrite for victim
// ordering: MRU re-link under LRU, frequency increment under LFU.
// GetVersioned deliberately does not affect eviction order. Caller holds
// sh.mu.
func (s *Store) onAccess(sh *shard, idx int) {
	if s.policy == LFUPolicy {
		s.incrementFreq(sh, idx)
		return
	}
	s.touch(sh, idx)
}

func (s *Store) updateWheel(sh *shard, idx int, at time.Time) {
	n := &sh.nodes[idx]
	if exp, ok := n.entry.Expiry.Time(); ok {
		sh.wheel.Insert(idx, expiryTicks(exp, at))
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
	s.emit(n.key, n.entry.VersionAndTTL(), n.entry.WriterNodeHash)
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
// remove): it is a no-op under LRU and unlinks the frequency bucket under
// LFU. Caller holds sh.mu.
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

	obs.IfPresent(&s.met, func(m *metrics.Collector) { m.EvictionsCap.Add(1) })
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
