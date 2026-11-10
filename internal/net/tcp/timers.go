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
func (s *Server) runGossipLoop(wg *sync.WaitGroup, gossip *cluster.GossipManager) {
	defer wg.Done()
	t := time.NewTimer(s.currentGossipInterval())
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		gossip.Tick()
		t.Reset(s.currentGossipInterval())
	}
}

// runProbeLoop ticks the failure detector until stopBg closes.
func (s *Server) runProbeLoop(wg *sync.WaitGroup, fd *cluster.FailureDetector) {
	defer wg.Done()
	t := time.NewTimer(s.currentProbeInterval())
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		fd.Tick()
		t.Reset(s.currentProbeInterval())
	}
}

// runAELoop runs anti-entropy plus counter repair every interval.
// Counter repair rides the same tick, mirroring C++ CacheNodeServer
// arming runCounterRound alongside runRound (skipped while the view is
// degraded, same gate as below in spirit: partnerless rounds no-op
// inside PickPartner).
func (s *Server) runAELoop(mgr *ae.Manager) {
	t := time.NewTimer(s.currentAEInterval())
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		mgr.RunRound(s.replicaFactor())
		mgr.RunCounterRound(s.replicaFactor())
		t.Reset(s.currentAEInterval())
	}
}

// runReplayLoop drains queued hints every interval. Best-effort like
// C++ scheduleReplay: ticks that fire while the target is still down
// fail silently and the next tick retries.
func (s *Server) runReplayLoop(replMgr *repl.Manager) {
	t := time.NewTimer(s.currentReplayInterval())
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		t.Reset(s.currentReplayInterval())
		if replMgr.HintCount() > 0 {
			replMgr.ReplayHints()
		}
	}
}

// runCompactLoop ticks snapshot compaction on its fixed cadence.
func (s *Server) runCompactLoop(pm *persist.Manager) {
	t := time.NewTimer(compactTickInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		t.Reset(compactTickInterval)
		pm.MaybeCompact()
	}
}
