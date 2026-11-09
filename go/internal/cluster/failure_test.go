package cluster

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kraytos17/cinder/go/internal/net/protocol"
)

// scriptTransport replays canned results per node.
type scriptTransport struct {
	mu      sync.Mutex
	results map[string][]error
	sends   map[string]int
}

func newScript() *scriptTransport {
	return &scriptTransport{results: map[string][]error{}, sends: map[string]int{}}
}

func (s *scriptTransport) fail(node string, errs ...error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[node] = append(s.results[node], errs...)
}

func (s *scriptTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends[node]++
	if len(s.results[node]) > 0 {
		err := s.results[node][0]
		s.results[node] = s.results[node][1:]
		if err != nil {
			return protocol.Response{}, err
		}
	}
	return protocol.Response{Status: protocol.StatusOK}, nil
}

func (s *scriptTransport) count(node string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sends[node]
}

func TestFailureThresholdTwo(t *testing.T) {
	now := time.Now()
	clk := func() time.Time { return now }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := newScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 30*time.Second)
	fd.Start()
	// One transient failure is tolerated: no suspect yet.
	tr.fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("transient failure flapped: %+v", n)
	}
	// Second consecutive failure suspects.
	tr.fail("n2", errors.New("timeout"))
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
	tr.fail("n2", errors.New("timeout"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Alive {
		t.Fatalf("streak reset broken: %+v", n)
	}
}

func TestFailureTimeoutSweep(t *testing.T) {
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
	tr := SenderFunc(func(node string, req protocol.Request) (protocol.Response, error) {
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
	time.Sleep(50 * time.Millisecond)
	advance(200 * time.Millisecond)
	// Second tick: sweep sees the overdue n2 probe and suspects it, then
	// probes n3 (which fails fast, no block).
	fd.Tick()
	close(hang)
	<-done
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("overdue probe must suspect: %+v", n)
	}
}

func TestFailureEscalatesToDead(t *testing.T) {
	now := time.Now()
	cur := now
	clk := func() time.Time { return cur }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := newScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
	fd.Start()
	tr.fail("n2", errors.New("x"), errors.New("x"))
	fd.Tick()
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Suspect {
		t.Fatalf("not suspect: %+v", n)
	}
	// Suspect older than the timeout escalates on the next tick.
	cur = now.Add(200 * time.Millisecond)
	tr.fail("n2", errors.New("x"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Dead {
		t.Fatalf("not dead: %+v", n)
	}
	// Dead nodes are no longer probed.
	before := tr.count("n2")
	cur = now.Add(10 * time.Second)
	fd.Tick()
	if tr.count("n2") != before {
		t.Fatal("dead node probed again")
	}
}

// SenderFunc adapts a function to the Sender interface.
type SenderFunc func(node string, req protocol.Request) (protocol.Response, error)

func (f SenderFunc) Send(node string, req protocol.Request) (protocol.Response, error) {
	return f(node, req)
}

func TestFailureRecoveryViaGossip(t *testing.T) {
	now := time.Now()
	cur := now
	clk := func() time.Time { return cur }
	tb := NewMembershipTable("n1", clk)
	tb.Seed([]NodeInfo{{ID: "n2"}})
	tr := newScript()
	fd := NewFailureDetector(clk, tr, tb, "n1", 100*time.Millisecond)
	fd.Start()
	tr.fail("n2", errors.New("x"), errors.New("x"))
	fd.Tick()
	fd.Tick()
	cur = now.Add(time.Second)
	tr.fail("n2", errors.New("x"))
	fd.Tick()
	if n, _ := tb.Get("n2"); n.State != Dead {
		t.Fatalf("not dead: %+v", n)
	}
	// Dead nodes are no longer probed.
	before := tr.count("n2")
	cur = now.Add(10 * time.Second)
	fd.Tick()
	if tr.count("n2") != before {
		t.Fatal("dead node probed again")
	}
	// Recovery arrives as gossip: the restarted node refutes with a fresh
	// incarnation, which the table applies over the death rumor.
	tb.ApplyRumor(NodeInfo{ID: "n2", Host: "h", Port: 2, State: Alive, Incarnation: 9})
	if n, _ := tb.Get("n2"); n.State != Alive || n.Incarnation != 9 {
		t.Fatalf("not revived: %+v", n)
	}
}
