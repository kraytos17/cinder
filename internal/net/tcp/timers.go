// Background tick loops: gossip, failure-detector probes, anti-entropy
// rounds, hint replay, and snapshot compaction. Each loop owns one
// reused timer (NewTimer+Reset): time.After would allocate a timer per
// tick that lingers until it fires. Intervals are re-read per tick, so
// runtime retuning still applies.
//
// The loops run under Serve's WaitGroup and exit on stopBg; see Serve.
package tcp

import (
	"sync"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
)

// runGossipLoop ticks membership gossip until stopBg closes.
func (s *Server) runGossipLoop(wg *sync.WaitGroup, gossip *cluster.GossipManager, stop <-chan struct{}) {
	defer wg.Done()
	t := time.NewTimer(s.currentGossipInterval())
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		gossip.Tick()
		t.Reset(s.currentGossipInterval())
	}
}

// runProbeLoop ticks the failure detector until stopBg closes.
func (s *Server) runProbeLoop(wg *sync.WaitGroup, fd *cluster.FailureDetector, stop <-chan struct{}) {
	defer wg.Done()
	t := time.NewTimer(s.currentProbeInterval())
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		fd.Tick()
		t.Reset(s.currentProbeInterval())
	}
}

// runAELoop runs anti-entropy and counter repair on the same interval.
// Managers with no eligible partner treat a round as a no-op.
func (s *Server) runAELoop(mgr *ae.Manager, stop <-chan struct{}) {
	t := time.NewTimer(s.currentAEInterval())
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		mgr.RunRound(s.replicaFactor())
		mgr.RunCounterRound(s.replicaFactor())
		t.Reset(s.currentAEInterval())
	}
}

// runReplayLoop retries queued hints with backoff: the base delay while
// rounds make progress or the queue is empty, doubling per consecutive
// all-failed round up to the max. The loop runs under Serve's WaitGroup
// and ReplayHints is synchronous, so shutdown drains the in-flight batch
// before returning. Failed sends stay queued for a later round.
func (s *Server) runReplayLoop(replMgr *repl.Manager, stop <-chan struct{}) {
	t := time.NewTimer(s.currentReplayInterval())
	defer t.Stop()
	var fails int
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		baseDelay, maxDelay := s.currentReplayInterval(), s.currentReplayMaxInterval()
		switch {
		case replMgr.HintCount() == 0:
			fails = 0
		case replMgr.ReplayHints() > 0:
			fails = 0
		default:
			fails++
		}
		t.Reset(replayBackoff(baseDelay, maxDelay, fails))
	}
}

// replayBackoff returns the delay after a round: base on progress or an
// empty queue (fails == 0), doubling per consecutive all-failed round and
// capped at max. No jitter: one loop per node, nothing to de-synchronize.
func replayBackoff(baseDelay, maxDelay time.Duration, fails int) time.Duration {
	if maxDelay <= 0 {
		return baseDelay
	}
	if fails <= 0 {
		return min(baseDelay, maxDelay)
	}

	d := baseDelay
	for range fails {
		d *= 2
		if d <= 0 || d >= maxDelay {
			return maxDelay
		}
	}
	return d
}

// runCompactLoop ticks snapshot compaction on its fixed cadence.
func (s *Server) runCompactLoop(pm *persist.Manager, stop <-chan struct{}) {
	t := time.NewTimer(compactTickInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}

		t.Reset(compactTickInterval)
		pm.MaybeCompact()
	}
}
