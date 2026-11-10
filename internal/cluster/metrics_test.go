package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/testkit"
)

// Marks, probes, gossip rounds, and refutations bump the wired collector.
func TestClusterMetrics(t *testing.T) {
	now := time.Now()
	cur := now
	clk := func() time.Time { return cur }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	c := metrics.NewCollector()
	tb.SetMetrics(c)
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
	fd.SetMetrics(c)
	fd.Start()
	g := NewGossipManager(clk, tr, tb, "n1", time.Second)
	g.SetMetrics(c)
	g.Start()

	tr.Fail("n2", errors.New("x"), errors.New("x"))
	fd.Tick()
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("not suspect: %+v", n)
	}
	g.Tick()
	// A non-alive rumor about self is refuted.
	tb.ApplyRumor(NodeInfo{ID: "n1", State: Suspect, Incarnation: 5})
	if got := c.SuspectMarked.Load(); got != 1 {
		t.Fatalf("suspect = %d, want 1", got)
	}
	if got := c.GossipRounds.Load(); got != 1 {
		t.Fatalf("gossip = %d, want 1", got)
	}
	if got := c.ProbeSent.Load(); got == 0 {
		t.Fatal("no probes recorded")
	}
	if got := c.RefutedRumors.Load(); got != 1 {
		t.Fatalf("refuted = %d, want 1", got)
	}
	if n, _ := tb.Get("n1"); n.State != Alive {
		t.Fatalf("self not alive after refute: %+v", n)
	}
	// Idempotent re-marks do not double-count.
	tb.MarkSuspect("n2")
	if got := c.SuspectMarked.Load(); got != 1 {
		t.Fatalf("suspect double-counted: %d", got)
	}
}
