package tcp

// Replication helpers: replica-set computation, quorum result
// mapping, and the inbound replicate/hint apply path.

import (
	"errors"

	"github.com/kraytos17/cinder/internal/counter"
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

func (s *Server) replication() (*repl.Manager, int, repl.Mode) {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.repl, s.rf, s.mode
}

// replicaNodes mirrors CacheOperations::replicaNodesFor: ring owners minus
// self.

func (s *Server) replicaNodes(key string) []string {
	s.ringMu.RLock()
	ring := s.ring
	s.ringMu.RUnlock()
	if ring == nil {
		return nil
	}
	nodes, err := ring.GetNodes(key, s.replicaFactor())
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

func (s *Server) replicaFactor() int {
	s.replMu.RLock()
	defer s.replMu.RUnlock()
	return s.rf
}

// NewServer returns a server over an existing store.

func StatusOf(err error) protocol.Status {
	return statusOf(err)
}

// DeleteNoRedirect deletes without the ownership redirect, mirroring the
// C++ gateway Delete, which bypasses checkWriteOwnership at the
// connection layer and deletes through the operations path directly. Like
// Dispatch it performs no auth check: callers (the gateway) enforce their
// credential first.

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

// applyReplicated ports TcpConnection::handleReplicate: absolute wall-clock
// expiry rides the request; a tombstone without a sender deadline falls
// back to the local GC deadline via the versioned remove path.

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

func writeResult(err error) protocol.Response {
	return protocol.Response{Status: statusOf(err)}
}

// adminJSON shapes mirror formatNodeInfoJson/formatClusterJson/formatRingJson.
