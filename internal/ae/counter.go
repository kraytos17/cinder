package ae

import (
	"cmp"
	"slices"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
)

// Aliases expose counter package types used by the AE manager interface.
// BundleEntry represents one serialized record; PNCounter stores positive
// and negative per-actor components.
type (
	BundleEntry = counter.BundleEntry
	PNCounter   = counter.PNCounter
)

// RunCounterRound exchanges counter state with one replica partner. It
// skips work when the counter store is absent, empty, or no partner is
// available.
func (m *Manager) RunCounterRound(replicaFactor hashring.ReplicaCount) {
	m.counterMu.Lock()
	cs := m.counters
	m.counterMu.Unlock()
	if cs == nil || cs.Size() == 0 {
		return
	}

	partner, ok := m.PickPartner(replicaFactor)
	if !ok {
		return
	}
	m.exchangeCounterState(partner)
}

// exchangeCounterState sends a bounded batch to partner and merges a valid
// reply. Transport and decode failures leave local state unchanged.
func (m *Manager) exchangeCounterState(partner string) {
	m.counterMu.Lock()
	cs := m.counters
	m.counterMu.Unlock()
	if cs == nil {
		return
	}

	encoded := m.EncodeCounterBatch(m.fetchBytes)
	if encoded == nil {
		return
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.CounterAERounds.Add(1)
	})
	res, err := m.transport.Send(partner, protocol.Request{
		Opcode: protocol.OpCounterAntiEntropy, Value: encoded,
	})
	if err != nil || res.Status != protocol.StatusOK || !res.HasValue {
		return
	}

	remote, err := counter.DecodeCounterBundle(res.Value)
	if err != nil {
		return
	}
	for _, e := range remote {
		cs.Merge(e.Key, e.Counter)
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.CounterAERepaired.Add(uint64(len(remote)))
	})
	if e := m.log().Debug(); e != nil {
		e.Str("partner", partner).Int("entries", len(remote)).Msg("counter anti-entropy repaired")
	}
}

// EncodeCounterBatch encodes a sorted-key batch beginning after the saved
// cursor, then advances the cursor. It returns a header-only empty bundle
// when no records are selected; it returns nil if the store is absent, the
// byte budget is too small, or the first record cannot fit. A later call
// wraps the scan to keys before the cursor.
func (m *Manager) EncodeCounterBatch(maxBytes int) []byte {
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	if m.counters == nil || maxBytes < 8 {
		return nil
	}

	all := m.counters.SortedLiveCounterKeys()
	start := 0
	if m.counterCur != "" {
		idx, found := slices.BinarySearch(all, m.counterCur)
		start = idx
		if found {
			start++
		}
		if start >= len(all) {
			start = 0
		}
	}

	selected := make([]BundleEntry, 0, min(len(all), 1024))
	lastSelected := ""
	bytes := 8 // magic + count u32s
	if len(all) > 0 {
		for offset := range all {
			idx := (start + offset) % len(all)
			key := all[idx]
			n, ok := m.counters.BundleRecordSize(key)
			if !ok || n > maxBytes-bytes {
				if len(selected) == 0 {
					return nil
				}
				break
			}

			state, ok := m.counters.Get(key)
			if !ok {
				continue
			}

			actualSize, valid := state.BundleRecordSize(key)
			if !valid || actualSize > maxBytes-bytes {
				if len(selected) == 0 {
					return nil
				}
				break
			}

			bytes += actualSize
			selected = append(selected, BundleEntry{Key: key, Counter: state})
			lastSelected = key
		}
	}
	// Advance the cursor past the last selected key so arbitrarily large
	// stores converge over several bounded rounds (wrap-around scan).
	if len(selected) > 0 {
		m.counterCur = lastSelected
		slices.SortFunc(selected, func(a, b BundleEntry) int { return cmp.Compare(a.Key, b.Key) })
	}
	return counter.EncodeCounterBundle(selected)
}

// OnCounterAntiEntropy merges a bounded remote batch and returns a bounded
// local batch. It rejects oversized or malformed bundles and preflights
// capacity using a staged store before applying any entries, so a batch is
// accepted atomically. Peers continue exchanging remaining keys in later
// rounds.
func (m *Manager) OnCounterAntiEntropy(bundle []byte) (protocol.Status, []byte) {
	m.counterMu.Lock()
	cs := m.counters
	m.counterMu.Unlock()
	if cs == nil {
		return protocol.StatusNotSupported, nil
	}
	if len(bundle) > m.fetchBytes {
		return protocol.StatusCapacityExceeded, nil
	}

	remote, err := counter.DecodeCounterBundle(bundle)
	if err != nil {
		return protocol.StatusInvalidArgument, nil
	}

	staged := counter.NewStore(cs.CapacityBytes())
	for _, e := range cs.SortedLiveCounters() {
		if err := staged.Merge(e.Key, e.Counter); err != nil {
			return protocol.StatusCapacityExceeded, nil
		}
	}
	for _, e := range remote {
		if err := staged.Merge(e.Key, e.Counter); err != nil {
			return protocol.StatusCapacityExceeded, nil
		}
	}
	for _, e := range remote {
		if err := cs.Merge(e.Key, e.Counter); err != nil {
			return protocol.StatusCapacityExceeded, nil
		}
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.CounterAERepaired.Add(uint64(len(remote)))
	})
	resp := m.EncodeCounterBatch(m.fetchBytes)
	if resp == nil {
		return protocol.StatusCapacityExceeded, nil
	}
	return protocol.StatusOK, resp
}
