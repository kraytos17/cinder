package tcp

import "time"

// Ready reports whether this node should receive traffic. A node is ready
// when it is serving, not draining, past its own startup, and — for
// clustered nodes — past the rejoin warming window. Orchestrators should
// gate rolling restarts on this (wait for ready before moving to the next
// node); clients route around unready members via ownership redirects.
//
// A draining node is never ready even though it still answers reads: new
// writes already route elsewhere, so readiness would only attract traffic
// the drain is trying to shed. A standalone node is ready as soon as it
// serves, since recovery (when configured) completes synchronously inside
// ConfigurePersistence, before the node ever advertises itself.
func (s *Server) Ready() bool {
	if s.shut.Load() {
		return false
	}
	if s.isLeaving() {
		return false
	}

	s.ringMu.RLock()
	configured, started := s.clusterConfigured, s.clusterStarted
	s.ringMu.RUnlock()
	if !configured {
		return true
	}
	if !started {
		return false
	}
	return !s.isQuarantinedSelf()
}

// isQuarantinedSelf reports whether this process joined or rejoined
// inside the warming window. Zero quarantine disables the check; an
// unknown join time counts as warmed. Unlike the membership table's
// JoinedAt (meaningless for self: a fresh table starts at zero and a
// continuously-alive self never refreshes it), the process join time
// captures exactly the restart-and-refill window the quarantine exists
// for.
func (s *Server) isQuarantinedSelf() bool {
	s.shardMu.Lock()
	quarantine := s.quarantineInterval
	s.shardMu.Unlock()
	if quarantine <= 0 {
		return false
	}

	joined := s.joinedAtUnix.Load()
	if joined == 0 {
		return false
	}
	return s.clockNow().Sub(time.Unix(0, joined)) < quarantine
}
