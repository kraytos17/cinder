package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/testkit"
)

// A draining node advertises Leaving: excluded from rings for new
// placement, but still reachable and serving reads until it exits.
func TestLeavingWireTokenAndCodec(t *testing.T) {
	if Leaving.String() != "leaving" {
		t.Fatalf("Leaving.String() = %q, want leaving", Leaving.String())
	}
	view := EncodeView([]NodeInfo{{ID: "n2", Host: "h", Port: 2, State: Leaving, Incarnation: 7}})
	infos := DecodeView(view)
	if len(infos) != 1 || infos[0].ID != "n2" || infos[0].State != Leaving || infos[0].Incarnation != 7 {
		t.Fatalf("leaving did not survive gossip codec: %+v", infos)
	}
	// Unknown future tokens stay skippable: that is the contract old
	// binaries rely on when they meet a leaving member (they route
	// around the unknown entry, which is the safe direction).
	if _, ok := ParseNodeInfo("n9@h:1:frobnicate:2"); ok {
		t.Fatal("unknown state token must not parse")
	}
}

func TestMarkLeavingFiresOnce(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	changes := 0
	tb.OnChange(func() { changes++ })
	tb.MarkLeaving("n2")
	if n, ok := tb.Get("n2"); !ok || n.State != Leaving {
		t.Fatalf("leaving not recorded: %+v %v", n, ok)
	}
	if changes != 1 {
		t.Fatalf("MarkLeaving fired %d changes, want 1", changes)
	}
	tb.MarkLeaving("n2")
	if changes != 1 {
		t.Fatalf("second MarkLeaving fired, want idempotent: %d", changes)
	}
	tb.MarkLeaving("ghost")
	if _, ok := tb.Get("ghost"); ok {
		t.Fatal("leaving must not invent a node")
	}
}

func TestMarkSuspectPreservesLeaving(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tb.MarkLeaving("n2")
	changes := 0
	tb.OnChange(func() { changes++ })
	tb.MarkSuspect("n2")
	if n, _ := tb.Get("n2"); n.State != Leaving {
		t.Fatalf("suspect overwrote drain intent: %+v", n)
	}
	if changes != 0 {
		t.Fatalf("suspect on leaving fired %d changes", changes)
	}
}

func TestRefutePreservesLeavingIntent(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.MarkLeaving("n1")
	for _, rumor := range []NodeInfo{
		{ID: "n1", State: Alive, Incarnation: 1},
		{ID: "n1", State: Dead, Incarnation: 1},
	} {
		tb.ApplyRumor(rumor)
		if got, _ := tb.Self(); got.State != Leaving || got.Incarnation != 2 {
			t.Fatalf("refute of %+v lost drain intent: %+v", rumor, got)
		}
	}
}

func TestMatchAliveIDIncludesLeaving(t *testing.T) {
	clk, _ := testClock()
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}})
	tb.MarkLeaving("n2")
	tb.MarkDead("n3")
	if !tb.MatchAliveID("n1", func(id string) bool { return id == "n2" }) {
		t.Fatal("leaving member excluded from node-token auth")
	}
	if tb.MatchAliveID("n1", func(id string) bool { return id == "n3" }) {
		t.Fatal("dead member matched")
	}
}

func TestFailureDetectorSkipsLeavingPeers(t *testing.T) {
	now := time.Now()
	clk := func() time.Time { return now }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}})
	tb.MarkLeaving("n2")
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 30*time.Second)
	fd.Start()
	// Control: the alive peer still convicts on consecutive failures.
	tr.Fail("n3", errors.New("timeout"))
	fd.Tick()
	tr.Fail("n3", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n3"); n.State != Suspect {
		t.Fatalf("alive peer must still convict: %+v", n)
	}
	// The draining peer is never probed, so failures can't touch it.
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Leaving {
		t.Fatalf("detector overwrote drain intent: %+v", n)
	}
}
