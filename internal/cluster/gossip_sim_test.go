package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/testkit"
)

// recordingSender captures every request per node for content assertions.
// testkit.Script counts sends but drops the payloads.
type recordingSender struct {
	mu    sync.Mutex
	sends map[string][]protocol.Request
}

func newRecordingSender() *recordingSender {
	return &recordingSender{sends: map[string][]protocol.Request{}}
}

func (s *recordingSender) Send(node string, req protocol.Request) (protocol.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends[node] = append(s.sends[node], req)
	return protocol.Response{Status: protocol.StatusOK}, nil
}

func (s *recordingSender) values(node string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := s.sends[node]
	out := make([][]byte, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Value)
	}
	return out
}

// A Dead rumor derived by one node's failure detector must be re-gossiped
// by a third node: n1 suspects n2, n3 applies n1's view, and n3's own
// gossip carries the Suspect state onward.
func TestGossipPropagatesDeadRumorToThirdNode(t *testing.T) {
	clk := time.Now
	tb1 := NewMembershipTable("n1", clk)
	tb1.Seed([]NodeInfo{{ID: "n2"}})
	tr1 := testkit.NewScript()
	fd1 := NewFailureDetector(clk, tr1, tb1, "n1", 100*time.Millisecond)
	fd1.Start()
	tr1.Fail("n2", errors.New("x"), errors.New("x"))
	fd1.Tick()
	fd1.Tick()
	if n, _ := tb1.Get("n2"); n.State != Suspect {
		t.Fatalf("n1 must suspect n2: %+v", n)
	}

	tb3 := NewMembershipTable("n3", clk)
	tb3.Seed([]NodeInfo{{ID: "n1"}, {ID: "n2"}})
	rec := newRecordingSender()
	g3 := NewGossipManager(clk, rec, tb3, "n3", time.Second)
	g3.Start()
	g3.HandleMessage("n1", []byte(EncodeView(tb1.Snapshot())))
	if n, _ := tb3.Get("n2"); n.State != Suspect {
		t.Fatalf("n3 must apply third-party suspect rumor: %+v", n)
	}
	g3.Tick()
	found := false
	for _, v := range rec.values("n1") {
		for _, r := range DecodeView(string(v)) {
			if r.ID == "n2" && r.State == Suspect {
				found = true
			}
		}
	}
	// Tick targets a random peer; n2's view may have gone there instead.
	// Either way the rumor must be on the wire in at least one send.
	if !found {
		for _, v := range rec.values("n2") {
			for _, r := range DecodeView(string(v)) {
				if r.ID == "n2" && r.State == Suspect {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("suspect rumor never re-gossiped")
	}
}

// Snapshot inside an OnChange callback must not deadlock: fire() drops
// the table lock before invoking callbacks, and the server depends on it
// (rebuildRingFromTable). The goroutine + timeout turns a deadlock into
// a failure instead of a hung suite.
func TestOnChangeReentrantSnapshot(t *testing.T) {
	tb := NewMembershipTable("n1", time.Now)
	done := make(chan []NodeInfo, 1)
	tb.OnChange(func() {
		done <- tb.Snapshot()
	})
	go tb.ApplyRumor(NodeInfo{ID: "n2", State: Alive})
	select {
	case snap := <-done:
		found := false
		for _, n := range snap {
			if n.ID == "n2" {
				found = true
			}
		}
		if !found {
			t.Fatalf("snapshot missing n2: %+v", snap)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnChange+Snapshot deadlocked")
	}
}

// A node that knows only itself learns the cluster purely through an
// incoming gossip view: members are adopted, gossip and FD peer sets
// rebuild via OnChange, and probes keep them Alive.
func TestLateJoinerLearnsClusterViaGossip(t *testing.T) {
	clk := time.Now
	tb := NewMembershipTable("late", clk)
	tr := testkit.NewScript()
	g := NewGossipManager(clk, tr, tb, "late", time.Second)
	g.Start()
	fd := NewFailureDetector(clk, tr, tb, "late", time.Hour)
	fd.Start()
	view := EncodeView([]NodeInfo{
		{ID: "n2", Host: "h2", Port: 2, State: Alive, Incarnation: 1},
		{ID: "n3", Host: "h3", Port: 3, State: Alive, Incarnation: 1},
	})
	g.HandleMessage("n2", []byte(view))
	for _, id := range []string{"n2", "n3"} {
		if n, ok := tb.Get(id); !ok || n.State != Alive {
			t.Fatalf("%s not adopted alive: %+v %v", id, n, ok)
		}
	}
	// Peer sets rebuilt: gossip emits and the detector probes learned
	// peers, which ack (scripted OK) and stay Alive.
	for range 4 {
		g.Tick()
		fd.Tick()
	}
	if tr.Count("n2")+tr.Count("n3") == 0 {
		t.Fatal("no traffic to learned peers")
	}
	for _, id := range []string{"n2", "n3"} {
		if n, _ := tb.Get(id); n.State != Alive {
			t.Fatalf("%s flapped: %+v", id, n)
		}
	}
}

func TestHandleMessageCoalescesMembershipNotifications(t *testing.T) {
	tb := NewMembershipTable("self", time.Now)
	tr := testkit.NewScript()
	g := NewGossipManager(time.Now, tr, tb, "self", time.Second)
	g.Start()
	changes := 0
	tb.OnChange(func() { changes++ })

	view := EncodeView([]NodeInfo{
		{ID: "n2", Host: "h2", Port: 2, State: Alive, Incarnation: 1},
		{ID: "n3", Host: "h3", Port: 3, State: Alive, Incarnation: 1},
		{ID: "n4", Host: "h4", Port: 4, State: Alive, Incarnation: 1},
	})
	g.HandleMessage("peer", []byte(view))
	if changes != 1 {
		t.Fatalf("changed gossip view fired %d observer notifications, want 1", changes)
	}
	if got := len(g.peers); got != 3 {
		t.Fatalf("gossip peer list has %d peers, want all 3 applied before notification", got)
	}

	g.HandleMessage("peer", []byte(view))
	if changes != 1 {
		t.Fatalf("replayed unchanged view fired %d notifications, want still 1", changes)
	}
}

// The same rumor-learned peer, probed down immediately: two failed
// probes suspect it (failure threshold, not instant).
func TestLateJoinerGoesDownImmediately(t *testing.T) {
	clk := time.Now
	tb := NewMembershipTable("late", clk)
	tr := testkit.NewScript()
	fd := NewFailureDetector(clk, tr, tb, "late", time.Hour)
	fd.Start()
	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "h2", Port: 2, State: Alive, Incarnation: 1})
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("transient failure flapped rumor-learned peer: %+v", n)
	}
	tr.Fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("second failure must suspect: %+v", n)
	}
}

// A gossip tick with no peers sends nothing.
func TestGossipTickWithNoPeersSendsNothing(t *testing.T) {
	tb := NewMembershipTable("n1", time.Now)
	tr := testkit.NewScript()
	g := NewGossipManager(time.Now, tr, tb, "n1", time.Second)
	g.Start()
	for range 3 {
		g.Tick()
	}
	if got := tr.Count("n1") + tr.Count("n2"); got != 0 {
		t.Fatalf("tick without peers sent %d", got)
	}
}

// Leave marks self Dead (overriding stale Alive rumors) and broadcasts
// the view to every peer exactly once.
func TestGracefulLeaveBroadcastsDead(t *testing.T) {
	tb := NewMembershipTable("n1", time.Now)
	tb.Seed([]NodeInfo{{ID: "n2"}, {ID: "n3"}})
	tr := testkit.NewScript()
	g := NewGossipManager(time.Now, tr, tb, "n1", time.Second)
	g.Start()
	g.Leave()
	if n, _ := tb.Get("n1"); n.State != Dead {
		t.Fatalf("self not dead after leave: %+v", n)
	}
	if got := tr.Count("n2"); got != 1 {
		t.Fatalf("n2 got %d broadcasts, want 1", got)
	}
	if got := tr.Count("n3"); got != 1 {
		t.Fatalf("n3 got %d broadcasts, want 1", got)
	}
}
