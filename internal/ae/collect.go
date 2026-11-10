package ae

// Repair collection: resolving divergent hashes to keys and
// filling sync items up to the fetch budget.

import (
	"cmp"
	"maps"
	"slices"
	"strings"

	"github.com/kraytos17/cinder/internal/store"
)

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
	sorted = slices.Compact(sorted)
	resolved := map[uint64]string{}
	m.store.ForEach(func(key string, _ store.Entry) {
		h := KeyHash(key)
		i, _ := slices.BinarySearch(sorted, h)
		if i < len(sorted) && sorted[i] == h {
			if _, ok := resolved[h]; !ok {
				resolved[h] = key
			}
		}
	})
	// maps.Values is an iter.Seq: no temp pair slice, no second loop.
	// The key tie-break keeps collision order deterministic (the old
	// unstable pair sort left it to chance).
	keys := slices.SortedFunc(maps.Values(resolved), func(a, b string) int {
		return cmp.Or(cmp.Compare(KeyHash(a), KeyHash(b)), strings.Compare(a, b))
	})
	return m.CollectEntriesForKeys(keys, m.fetchBytes)
}

// CollectEntriesForKeys fills entries for keys up to maxBytes; the first
// over-budget entry still rides (over-budget once beats stalling forever).

func (m *Manager) CollectEntriesForKeys(keys []string, maxBytes int) []SyncItem {
	ps := make([]hashKeyPair, 0, len(keys))
	for _, k := range keys {
		ps = append(ps, hashKeyPair{KeyHash(k), k})
	}
	slices.SortFunc(ps, func(a, b hashKeyPair) int { return cmp.Compare(a.h, b.h) })
	items := make([]SyncItem, 0, len(ps))
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
