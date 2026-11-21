package tcp

import (
	"errors"
	"time"

	"github.com/kraytos17/cinder/internal/cluster"
)

// Drain states reported by DrainStatus. A node is either not draining,
// actively handing off, finished, or past its deadline.
const (
	DrainIdle     = "idle"
	DrainDraining = "draining"
	DrainDrained  = "drained"
	DrainTimedOut = "timed-out"
)

// DrainStatus is a point-in-time view of the drain state machine.
type DrainStatus struct {
	State         string `json:"state"`
	RemainingKeys int    `json:"remaining_keys"`
}

// drainTick is the rebalance cadence inside the drain loop: frequent
// enough to track migration, cheap enough to be a no-op scan.
const drainTick = 200 * time.Millisecond

// StartDrain marks this node Leaving and migrates all local data to the
// new owners in the background; poll DrainStatus for progress. The drain
// does not shut the server down: when the status turns drained, finish
// with the regular admin shutdown path. Starting a drain is idempotent:
// a second call while draining reports the current status.
//
// Draining is refused when replication cannot hand the data off (RF<2 or
// unconfigured): a lone copy has nowhere safe to go.
func (s *Server) StartDrain(timeout time.Duration) (DrainStatus, error) {
	_, rf, _ := s.replication()
	if rf < 2 {
		return DrainStatus{State: DrainIdle}, errors.New("tcp: drain requires replication (RF>1)")
	}

	s.drainMu.Lock()
	if s.drainState == DrainDraining {
		st := s.drainStatusLocked()
		s.drainMu.Unlock()
		return st, nil
	}
	if s.drainState == DrainDrained {
		st := s.drainStatusLocked()
		s.drainMu.Unlock()
		return st, nil
	}

	s.drainState = DrainDraining
	s.drainDeadline = s.clock.Now().Add(timeout)
	s.drainMu.Unlock()

	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table == nil {
		return DrainStatus{State: DrainIdle}, errors.New("tcp: drain requires configured cluster")
	}

	table.MarkLeaving(s.nodeID)
	go s.drainLoop()
	return s.DrainStatus(), nil
}

// DrainStatus reports the drain state machine and the local keys still
// awaiting migration.
func (s *Server) DrainStatus() DrainStatus {
	s.drainMu.Lock()
	defer s.drainMu.Unlock()
	return s.drainStatusLocked()
}

func (s *Server) drainStatusLocked() DrainStatus {
	state := s.drainState
	if state == "" {
		state = DrainIdle
	}
	return DrainStatus{State: state, RemainingKeys: s.store.Size()}
}

// drainLoop runs rebalance passes until the local store is empty or the
// deadline passes. It also replays hints so failed sends to the new
// holders converge without waiting for the background tick.
func (s *Server) drainLoop() {
	t := time.NewTimer(drainTick)
	defer t.Stop()
	for {
		<-t.C
		if s.shut.Load() {
			return
		}

		s.RebalanceNow()
		s.ReplayHints()
		s.drainMu.Lock()
		done := s.shardDrainedLocked()
		timedOut := !done && !s.clock.Now().Before(s.drainDeadline)
		if done {
			s.drainState = DrainDrained
		} else if timedOut {
			s.drainState = DrainTimedOut
		}

		finished := done || timedOut
		s.drainMu.Unlock()
		if finished {
			return
		}
		t.Reset(drainTick)
	}
}

// shardDrainedLocked reports whether no local data remains. Caller holds
// drainMu; the shard manager is read under its own lock.
func (s *Server) shardDrainedLocked() bool {
	s.shardMu.Lock()
	m := s.shard
	s.shardMu.Unlock()
	if m == nil {
		return s.store.Size() == 0
	}
	return m.Drained()
}

// isLeaving reports whether this node has marked itself Leaving.
func (s *Server) isLeaving() bool {
	s.ringMu.RLock()
	table := s.table
	s.ringMu.RUnlock()
	if table == nil {
		return false
	}

	info, ok := table.Get(s.nodeID)
	return ok && info.State == cluster.Leaving
}
