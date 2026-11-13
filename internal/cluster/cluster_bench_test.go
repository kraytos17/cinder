package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

type benchSender struct {
	sends int
}

func (s *benchSender) Send(node string, req protocol.Request) (protocol.Response, error) {
	s.sends++
	return protocol.Response{Status: protocol.StatusOK}, nil
}

func benchSeedTable(b *testing.B, self string, n int) *MembershipTable {
	b.Helper()
	t := NewMembershipTable(self, time.Now)
	peers := make([]NodeInfo, 0, n)
	for i := range n {
		peers = append(peers, NodeInfo{
			ID:          fmt.Sprintf("node-%04d", i),
			Host:        "127.0.0.1",
			Port:        netport.Port(7000 + i%1000),
			State:       Alive,
			Incarnation: uint64(i + 1),
		})
	}
	t.Seed(peers)
	t.SetSelfAddress("127.0.0.1", 7001)
	return t
}

func benchView(n int) []NodeInfo {
	view := make([]NodeInfo, 0, n)
	for i := range n {
		view = append(view, NodeInfo{
			ID:          fmt.Sprintf("node-%04d", i),
			Host:        "127.0.0.1",
			Port:        netport.Port(7000 + i%1000),
			State:       Alive,
			Incarnation: uint64(i + 1),
		})
	}
	return view
}

// BenchmarkEncodeView measures text view rendering per gossip tick.
func BenchmarkEncodeView(b *testing.B) {
	for _, n := range []int{10, 50, 200} {
		view := benchView(n)
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = EncodeView(view)
			}
		})
	}
}

// BenchmarkDecodeView measures view parsing per received gossip message.
func BenchmarkDecodeView(b *testing.B) {
	for _, n := range []int{10, 50, 200} {
		encoded := EncodeView(benchView(n))
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = DecodeView(encoded)
			}
		})
	}
}

// BenchmarkSnapshot measures the per-tick table copy (also used by
// rebuildLocked on every membership change).
func BenchmarkSnapshot(b *testing.B) {
	for _, n := range []int{50, 200} {
		t := benchSeedTable(b, "self", n)
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = t.Snapshot()
			}
		})
	}
}

// BenchmarkApplyRumor compares join / stale / no-op / update merge costs.
func BenchmarkApplyRumor(b *testing.B) {
	t := benchSeedTable(b, "self", 50)
	join := NodeInfo{ID: "node-new", Host: "127.0.0.1", Port: 7999, State: Alive, Incarnation: 1}
	stale := NodeInfo{ID: "node-0001", Host: "127.0.0.1", Port: 7001, State: Alive, Incarnation: 0}
	noop := NodeInfo{ID: "node-0001", Host: "127.0.0.1", Port: netport.Port(7000 + 1%1000), State: Alive, Incarnation: 2}
	update := NodeInfo{ID: "node-0001", Host: "127.0.0.2", Port: 7999, State: Suspect, Incarnation: 3}
	for _, tc := range []struct {
		name  string
		rumor NodeInfo
		reset func()
	}{
		{"join", join, func() { t.mu.Lock(); delete(t.nodes, join.ID); t.mu.Unlock() }},
		{"stale", stale, func() {}},
		{"noop", noop, func() {
			t.ApplyRumor(NodeInfo{ID: noop.ID, Host: noop.Host, Port: noop.Port, State: Alive, Incarnation: 2})
		}},
		{"update", update, func() {
			t.mu.Lock()
			if n, ok := t.nodes[update.ID]; ok {
				n.State = Alive
				n.Incarnation = 2
			}
			t.mu.Unlock()
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tc.reset()
				t.ApplyRumor(tc.rumor)
			}
		})
	}
}

// BenchmarkGossipTick measures Tick end-to-end (peer pick, Snapshot,
// EncodeView, Send) with an in-memory transport.
func BenchmarkGossipTick(b *testing.B) {
	for _, n := range []int{10, 50} {
		t := benchSeedTable(b, "self", n)
		s := &benchSender{}
		g := NewGossipManager(time.Now, s, t, "self", time.Second)
		g.Start()
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				g.Tick()
			}
		})
	}
}

// BenchmarkFailureTick measures the probe sweep (timed-out scan, suspect
// escalation, round-robin pick) with an always-OK transport.
func BenchmarkFailureTick(b *testing.B) {
	for _, n := range []int{10, 50} {
		t := benchSeedTable(b, "self", n)
		s := &benchSender{}
		fd := NewFailureDetector(time.Now, s, t, "self", 100*time.Millisecond)
		fd.Start()
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				fd.Tick()
			}
		})
	}
}
