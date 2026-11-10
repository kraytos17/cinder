// Per-opcode request handlers: the read/write bodies called by
// dispatchInner. One method per opcode keeps the router readable and
// each path independently testable through Dispatch.
package tcp

import (
	"errors"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
)

// handleGet serves reads: quorum fan-out when replicated, else the local
// copy, redirecting to the ring owner only on a miss.
func (s *Server) handleGet(req protocol.Request) protocol.Response {
	if m, rf, _ := s.replication(); m != nil && rf > 1 {
		e, err := m.Read(string(req.Key), s.replicaNodes(string(req.Key)), rf, req.TraceID, req.SpanID)
		if err != nil {
			if errors.Is(err, repl.ErrNotFound) {
				return protocol.Response{Status: protocol.StatusNotFound}
			}
			return protocol.Response{Status: protocol.StatusNotReady}
		}
		if e.Tombstone {
			return protocol.Response{Status: protocol.StatusNotFound}
		}
		return protocol.Response{Status: protocol.StatusOK, Value: e.Value, HasValue: true}
	}
	val, ok := s.store.Get(string(req.Key))
	if !ok {
		// Reads serve from the local copy when present; only a miss
		// consults ownership, redirecting to the ring owner.
		if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
			return s.notReady(owner)
		}
		return protocol.Response{Status: protocol.StatusNotFound}
	}
	return protocol.Response{Status: protocol.StatusOK, Value: val, HasValue: true}
}

// handleGetVersioned serves a versioned read for quorum voters and
// anti-entropy, redirecting to the ring owner only on a miss.
func (s *Server) handleGetVersioned(req protocol.Request) protocol.Response {
	e, ok := s.store.GetVersioned(string(req.Key))
	if !ok {
		if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
			return s.notReady(owner)
		}
		return protocol.Response{Status: protocol.StatusNotFound}
	}
	res := protocol.Response{
		Status:         protocol.StatusOK,
		Version:        e.Version,
		WriterNodeHash: e.WriterNodeHash,
		Tombstone:      e.Tombstone,
		HasExpiresAt:   e.HasTTL,
		ExpiresAt:      e.ExpiresAt,
	}
	if !e.Tombstone {
		res.Value = e.Value
		res.HasValue = true
	}
	return res
}

// handleSet applies a write: ownership-gated, quorum fan-out when
// replicated, else a local versioned put.
func (s *Server) handleSet(req protocol.Request) protocol.Response {
	if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
		return s.notReady(owner)
	}
	if m, rf, mode := s.replication(); m != nil && rf > 1 {
		var err error
		if req.HasTTL {
			err = m.Write(string(req.Key), req.Value, req.TTL, true, s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID)
		} else {
			err = m.Write(string(req.Key), req.Value, 0, false, s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID)
		}
		return writeResult(err)
	}
	var err error
	if req.HasTTL {
		err = s.store.PutWithTTL(string(req.Key), req.Value, req.TTL)
	} else {
		err = s.store.Put(string(req.Key), req.Value)
	}
	if err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// handleDelete plants a versioned tombstone: ownership-gated, quorum
// fan-out when replicated, else a local minted-version remove.
func (s *Server) handleDelete(req protocol.Request) protocol.Response {
	if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
		return s.notReady(owner)
	}
	if m, rf, mode := s.replication(); m != nil && rf > 1 {
		return writeResult(m.Delete(string(req.Key), s.replicaNodes(string(req.Key)), mode, req.TraceID, req.SpanID))
	}
	// Single-node delete plants a versioned tombstone with a minted
	// version and writer 0, exactly like CacheOperations::deleteAsync
	// without replication: deletes always succeed, even on missing keys.
	if err := s.store.RemoveVersioned(string(req.Key), s.store.MintVersion(), 0); err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// handleGossip folds a membership view into the table. No membership
// configured: accept and ignore, like C++ with a nil gossip manager.
func (s *Server) handleGossip(req protocol.Request) protocol.Response {
	s.ringMu.RLock()
	gossip := s.gossip
	s.ringMu.RUnlock()
	if gossip != nil {
		gossip.HandleMessage("", req.Value)
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// handleAntiEntropy routes digest/sync/refinement frames to the AE
// manager, answering default-OK with no manager (the C++ fallthrough).
func (s *Server) handleAntiEntropy(req protocol.Request) protocol.Response {
	s.aeMu.RLock()
	mgr := s.ae
	s.aeMu.RUnlock()
	if mgr == nil {
		// No anti-entropy manager: the C++ fallthrough answers default-OK.
		return protocol.Response{Status: protocol.StatusOK}
	}
	if req.Opcode == protocol.OpAntiEntropyDigest {
		if string(req.Key) == ae.RefinementKey {
			st, val := mgr.OnBucketRefinement(req.Value)
			return protocol.Response{Status: st, Value: val, HasValue: val != nil}
		}
		st, val := mgr.OnDigestRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}
	}
	st, val := mgr.OnSyncRequest(req.Value)
	return protocol.Response{Status: st, Value: val, HasValue: val != nil}
}

// handleCounterAntiEntropy serves one CRDT repair round against a replica
// partner, or NotSupported with no counter state.
func (s *Server) handleCounterAntiEntropy(req protocol.Request) protocol.Response {
	s.aeMu.RLock()
	mgr := s.ae
	s.aeMu.RUnlock()
	if mgr == nil {
		return protocol.Response{Status: protocol.StatusNotSupported}
	}
	st, val := mgr.OnCounterAntiEntropy(req.Value)
	return protocol.Response{Status: st, Value: val, HasValue: val != nil}
}

func (s *Server) handleCounterAdjust(req protocol.Request) protocol.Response {
	if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
		return s.notReady(owner)
	}
	if req.CounterDelta == 0 {
		return protocol.Response{Status: protocol.StatusOK}
	}
	var err error
	if req.CounterDecrement {
		err = s.counters.Decrement(string(req.Key), s.nodeID, req.CounterDelta)
	} else {
		err = s.counters.Increment(string(req.Key), s.nodeID, req.CounterDelta)
	}
	if err != nil {
		return protocol.Response{Status: counterStatusOf(err)}
	}
	st, ok := s.counters.Get(string(req.Key))
	if !ok {
		return protocol.Response{Status: protocol.StatusInternalError}
	}
	if m, rf, _ := s.replication(); m != nil && rf > 1 {
		m.ReplicateCounter(string(req.Key), counter.EncodePNCounter(st), s.replicaNodes(string(req.Key)), req.TraceID, req.SpanID)
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// handleCounterMerge folds replicated state in. No ownership check: any
// replica accepts merges (this is the fan-out target, like OpReplicate).

func (s *Server) handleCounterMerge(req protocol.Request) protocol.Response {
	st, err := counter.DecodePNCounter(req.Value)
	if err != nil {
		return protocol.Response{Status: protocol.StatusInvalidArgument}
	}
	if err := s.counters.Merge(string(req.Key), st); err != nil {
		return protocol.Response{Status: counterStatusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// handleCounterGet serves the local copy when present; only a miss
// consults ownership, redirecting to the ring owner like other reads.

func (s *Server) handleCounterGet(req protocol.Request) protocol.Response {
	if st, ok := s.counters.Get(string(req.Key)); ok {
		enc := counter.EncodePNCounter(st)
		if len(enc) == 0 {
			return protocol.Response{Status: protocol.StatusInternalError}
		}
		return protocol.Response{Status: protocol.StatusOK, Value: enc, HasValue: true}
	}
	if owner := s.ownerOf(string(req.Key)); owner != s.nodeID {
		return s.notReady(owner)
	}
	return protocol.Response{Status: protocol.StatusNotFound}
}

// counterStatusOf maps counter errors to wire statuses, mirroring the
// C++ counter paths (overflow surfaces as CapacityExceeded).
