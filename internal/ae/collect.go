package ae

import (
	"cmp"
	"maps"
	"slices"
	"strings"
)

type hashKeyPair struct {
	h   uint64
	key string
}

func (m *Manager) collectEntriesForHashes(hashes []uint64) []SyncItem {
	if len(hashes) == 0 {
		return nil
	}

	sorted := slices.Clone(hashes)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	resolved := make(map[uint64]string, len(sorted))
	for _, item := range m.store.MetadataSnapshot() {
		h := KeyHash(item.Key)
		i, _ := slices.BinarySearch(sorted, h)
		if i < len(sorted) && sorted[i] == h {
			if _, ok := resolved[h]; !ok {
				resolved[h] = item.Key
			}
		}
	}

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
		if !e.Expiry.IsNone() {
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
