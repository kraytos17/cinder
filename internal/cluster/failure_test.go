package cluster

import (
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/testkit"
)

func TestFailureThresholdTwo(t *testing.T) {
	now := time.Now()
	clk := func() time.Time { return now }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 30*time.Second)
	fd.Start()
	// One transient failure is tolerated: no suspect yet.
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("transient failure flapped: %+v", n)
	}
	// Second consecutive failure suspects.
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("streak of 2 must suspect: %+v", n)
	}
	// Success recovers and resets the streak.
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("ack must recover: %+v", n)
	}
	// One failure after recovery is tolerated again.
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("streak reset broken: %+v", n)
	}
}

func TestFailureTimeoutSweep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		var cur atomic.Int64
		cur.Store(now.UnixNano())
		clk := func() time.Time { return time.Unix(0, cur.Load()) }
		advance := func(d time.Duration) { cur.Add(int64(d)) }
		tb := NewMembershipTable("n1", clk)
		tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}})
		// n2 blackholes; n3 fails fast. The sweep must suspect n2 while its
		// probe is still outstanding, without the tick blocking on it.
		hang := make(chan struct{})
		tr := testkit.Func(func(node string, req protocol.Request) (protocol.Response, error) {
			if node == "n3" {
				return protocol.Response{}, errors.New("refused")
			}
			<-hang // blackhole: never acks (no lock held while parked)
			return protocol.Response{}, errors.New("late")
		})
		fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
		fd.Start()
		done := make(chan struct{})
		// First tick probes n2 (sorted round-robin) and blocks in Send.
		go func() { fd.Tick(); close(done) }()
		// Wait for the probe to park instead of sleeping 50ms: exact,
		// and immune to slow-CI flakes.
		synctest.Wait()
		advance(200 * time.Millisecond)
		// Second tick: sweep sees the overdue n2 probe and suspects it, then
		// probes n3 (which fails fast, no block).
		fd.Tick()
		close(hang)
		<-done
		if n, _ := tb.Get("n2"); n.State != Suspect {
			t.Fatalf("overdue probe must suspect: %+v", n)
		}
	})
}

func TestFailureEscalatesToDead(t *testing.T) {
	now := time.Now()
	cur := now
	clk := func() time.Time { return cur }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
	fd.Start()
	tr.Fail("n2", errors.New("x"), errors.New("x"))
	fd.Tick()
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("not suspect: %+v", n)
	}
	// Suspect older than the timeout escalates on the next tick.
	cur = now.Add(200 * time.Millisecond)
	tr.Fail("n2", errors.New("x"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Dead {
		t.Fatalf("not dead: %+v", n)
	}
	// Dead nodes are no longer probed.
	before := tr.Count("n2")
	cur = now.Add(10 * time.Second)
	fd.Tick()
	if tr.Count("n2") != before {
		t.Fatal("dead node probed again")
	}
}

func TestFailureRecoveryViaGossip(t *testing.T) {
	now := time.Now()
	cur := now
	clk := func() time.Time { return cur }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
	fd.Start()
	tr.Fail("n2", errors.New("x"), errors.New("x"))
	fd.Tick()
	fd.Tick()
	cur = now.Add(time.Second)
	tr.Fail("n2", errors.New("x"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Dead {
		t.Fatalf("not dead: %+v", n)
	}
	// Dead nodes are no longer probed.
	before := tr.Count("n2")
	cur = now.Add(10 * time.Second)
	fd.Tick()
	if tr.Count("n2") != before {
		t.Fatal("dead node probed again")
	}
	// Recovery arrives as gossip: the restarted node refutes with a fresh
	// incarnation, which the table applies over the death rumor.
	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "h", Port: 2, State: Alive, Incarnation: 9})
	if n, _ := tb.Get("n2"); n.State != Alive || n.Incarnation != 9 {
		t.Fatalf("not revived: %+v", n)
	}
}
