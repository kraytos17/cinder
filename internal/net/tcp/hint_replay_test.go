package tcp_test

import (
	"fmt"
	"testing"
	"time"
)

// Background hint replay: hints queued while a peer is down drain
// automatically on the replay tick once it returns, without a manual
// ReplayHints call. The test shortens the interval for faster execution.
func TestHintReplayTickHealsRestartedPeer(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	for _, n := range c.nodes {
		n.srv.SetReplayInterval(100 * time.Millisecond)
	}
	n1 := c.nodes["node1"]
	writeToOwner(t, c, "rk-sanity", "v")
	if _, ok := c.nodes["node2"].mgr.Local().GetVersioned("rk-sanity"); !ok {
		t.Fatal("replication path broken before kill")
	}
	// Crash node2; writes pile up as hints (async fan-outs fail).
	p2port := portOf(t, c.nodes["node2"].addr)
	c.kill("node2")
	var divergent []string
	for i := 0; len(divergent) < 10; i++ {
		if k := fmt.Sprintf("rk-key-%d", i); ringOwner(t, k) == "node1" {
			writeToOwner(t, c, k, "v")
			divergent = append(divergent, k)
		}
	}
	// Quiesce: every fan-out must have finished (hinted) before restart,
	// so only the replay tick can deliver what follows.
	waitForHint(t, n1.mgr, len(divergent))
	c.restart("node2", p2port, []string{"node1", "node2"})
	// No manual ReplayHints: the tick must converge node2 and drain hints.
	deadline := time.Now().Add(15 * time.Second)
	for {
		n2 := c.nodes["node2"]
		done := n1.mgr.HintCount() == 0
		for _, k := range divergent {
			if _, ok := n2.mgr.Local().GetVersioned(k); !ok {
				done = false
				break
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay tick did not heal node2 (hints=%d)", n1.mgr.HintCount())
		}
		time.Sleep(50 * time.Millisecond)
	}
}
