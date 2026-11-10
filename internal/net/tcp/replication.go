package tcp

// Replication helpers: replica-set lookup, status conversion, and applying
// inbound replicated entries.

import (
	"errors"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

func (s *Server) ReplayHints() int {
	s.replMu.RLock()
	m := s.repl
	s.replMu.RUnlock()
	if m == nil {
		return 0
	}
	return m.ReplayHints()
}

func (s *Server) replication() (*repl.Manager, hashring.ReplicaCount, repl.Mode) {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.repl, s.rf, s.mode
}

// replicaNodes returns the ring-selected replica set excluding this node.
func (s *Server) replicaNodes(key string) []string {
	s.ringMu.RLock()
	ring := s.ring
	s.ringMu.RUnlock()
	if ring == nil {
		return nil
	}
	rf := s.replicaFactor()
	if rf < 1 || rf > hashring.MaxReplicas {
		return nil
	}
	nodes, err := ring.GetNodes(key, rf)
	if err != nil || len(nodes) == 0 {
		return nil
	}
	out := nodes[:0:0]
	for _, n := range nodes {
		if n != s.nodeID {
			out = append(out, n)
		}
	}
	return out
}

func (s *Server) replicaFactor() hashring.ReplicaCount {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.rf
}

// StatusOf maps an error to the protocol status returned by dispatch.
func StatusOf(err error) protocol.Status {
	return statusOf(err)
}

// counterStatusOf maps counter-store failures to protocol statuses.
func counterStatusOf(err error) protocol.Status {
	switch {
	case err == nil:
		return protocol.StatusOK
	case errors.Is(err, counter.ErrCapacityExceeded):
		return protocol.StatusCapacityExceeded
	case errors.Is(err, counter.ErrInvalidActor):
		return protocol.StatusInvalidArgument
	default:
		return protocol.StatusInternalError
	}
}

// applyReplicated applies a replicated entry. A tombstone without an
// absolute expiry receives the local tombstone-GC deadline through the
// versioned-remove path.
func (s *Server) applyReplicated(req protocol.Request) protocol.Response {
	if req.Tombstone && req.ExpiresAt.IsNone() {
		if err := s.store.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash); err != nil {
			return protocol.Response{Status: statusOf(err)}
		}
		return protocol.Response{Status: protocol.StatusOK}
	}
	e := store.Entry{
		Value:          append([]byte(nil), req.Value...),
		Version:        req.Version,
		WriterNodeHash: req.WriterNodeHash,
		Tombstone:      req.Tombstone,
	}
	if !req.ExpiresAt.IsNone() {
		e.Expiry = req.ExpiresAt
	}
	if err := s.store.PutVersioned(string(req.Key), e); err != nil {
		return protocol.Response{Status: statusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// statusOf maps store and replication failures to protocol statuses.
func statusOf(err error) protocol.Status {
	switch {
	case err == nil:
		return protocol.StatusOK
	case errors.Is(err, store.ErrCapacityExceeded), errors.Is(err, counter.ErrCapacityExceeded):
		return protocol.StatusCapacityExceeded
	case errors.Is(err, repl.ErrQuorum):
		return protocol.StatusNotReady
	case errors.Is(err, repl.ErrNotFound):
		return protocol.StatusNotFound
	default:
		return protocol.StatusInternalError
	}
}

// writeResult wraps an operation error in a status-only response.
func writeResult(err error) protocol.Response {
	return protocol.Response{Status: statusOf(err)}
}

// adminJSON types define the JSON documents returned by admin operations.
