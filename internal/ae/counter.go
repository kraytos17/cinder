package ae

import (
	"sort"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// Aliases so Manager's CounterStore interface reads without a prefix.
type BundleEntry = counter.BundleEntry
type PNCounter = counter.PNCounter

// RunCounterRound runs one CRDT repair round against a replica partner.
// Skipped when counter state is absent or partnerless, mirroring C++
// runCounterRound (including the empty-store skip).
func (m *Manager) RunCounterRound(replicaFactor int) {
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

// exchangeCounterState pushes a bounded batch to partner and merges the
// reply, mirroring C++ exchangeCounterState. Transport and decode
// failures are best-effort no-ops.
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
		_ = cs.Merge(e.Key, e.Counter)
	}
}

// EncodeCounterBatch encodes a bounded sorted-key batch starting after
// the persistent cursor, advancing it. Empty selection still encodes the
// header (count 0); over-budget-first-record or missing store yields nil,
// mirroring C++ encodeCounterBatch (including the first-record-overflow
// exception and the wrap-around scan).
func (m *Manager) EncodeCounterBatch(maxBytes int) []byte {
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	if m.counters == nil || maxBytes < 8 {
		return nil
	}
	all := m.counters.SortedLiveCounters()
	start := 0
	if m.counterCur != "" {
		start = sort.Search(len(all), func(i int) bool { return all[i].Key > m.counterCur })
		if start >= len(all) {
			start = 0
		}
	}
	var selected []BundleEntry
	bytes := 8 // magic + count u32s
	if len(all) > 0 {
		for offset := range all {
			idx := (start + offset) % len(all)
			n, ok := counter.RecordBytes(all[idx].Key, all[idx].Counter)
			if !ok || n > maxBytes-bytes {
				if len(selected) == 0 {
					return nil
				}
				break
			}
			bytes += n
			selected = append(selected, all[idx])
		}
	}
	// Advance the cursor past the last selected key so arbitrarily large
	// stores converge over several bounded rounds (wrap-around scan).
	if len(selected) > 0 {
		m.counterCur = selected[len(selected)-1].Key
	}
	return counter.EncodeCounterBundle(selected)
}

// OnCounterAntiEntropy folds a bounded remote batch into the local store
// and replies with the merged local batch, mirroring C++
// onCounterAntiEntropy: oversize bundles are rejected, malformed ones
// fail, capacity is preflighted against a staged copy before anything is
// applied (all-or-nothing per batch), and the reply carries the merged
// local state (itself bounded; the peer continues in later rounds).
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
	// Staged all-or-nothing preflight: clone local state into a scratch
	// budget store and merge the batch there first.
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
	resp := m.EncodeCounterBatch(m.fetchBytes)
	if resp == nil {
		return protocol.StatusCapacityExceeded, nil
	}
	return protocol.StatusOK, resp
}
