package client

import (
	"fmt"
	"net"
	"slices"
	"testing"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

func benchPortFromListener(tb testing.TB, addr string) netport.Port {
	tb.Helper()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		tb.Fatal(err)
	}
	var n uint16
	if _, err := fmt.Sscanf(portStr, "%d", &n); err != nil {
		tb.Fatal(err)
	}
	return netport.Port(n)
}

func newBenchCluster(b *testing.B, n int) *Cluster {
	b.Helper()
	nodes := make([]NodeAddr, n)
	for i := range nodes {
		nodes[i] = NodeAddr{ID: fmt.Sprintf("node%d", i+1)}
	}
	c, err := NewCluster(ClusterConfig{Nodes: nodes, MaxRetries: 2})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(c.Close)
	return c
}

// BenchmarkClusterPreferredNodeHit measures the learned-owner fast path:
// map hit under mutex. Miss path falls through to ring lookup.
func BenchmarkClusterPreferredNodeHit(b *testing.B) {
	c := newBenchCluster(b, 4)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
		c.learnOwner(keys[i], "node1")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_ = c.preferredNode(keys[i%len(keys)])
	}
}

// BenchmarkClusterPreferredNodeMiss measures the ring-lookup path through
// Cluster (mutex miss + ring.GetNode at 150 vnodes x 4 nodes).
func BenchmarkClusterPreferredNodeMiss(b *testing.B) {
	c := newBenchCluster(b, 4)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-miss-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_ = c.preferredNode(keys[i%len(keys)])
	}
}

// BenchmarkTryFollowRedirect measures redirect validation: status check,
// target parse, hop + visited-set bounds, AddAddr/learnOwner on follow.
func BenchmarkTryFollowRedirect(b *testing.B) {
	c := newBenchCluster(b, 4)
	req := protocol.Response{Status: protocol.StatusNotReady, Value: []byte("moved to node2@127.0.0.1:7002")}
	visited := []string{"node1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_, _ = c.tryFollowRedirect("k", req, "node1", visited, 1)
		// Restore learned state so every iteration takes the follow path.
		c.forgetOwner("k")
	}
}

// BenchmarkTryFollowRedirectNoAddr isolates the id-only path (no AddAddr,
// no IP normalize): parse + hop/visited checks + learnOwner only.
func BenchmarkTryFollowRedirectNoAddr(b *testing.B) {
	c := newBenchCluster(b, 4)
	req := protocol.Response{Status: protocol.StatusNotReady, Value: []byte("moved to node2")}
	visited := []string{"node1"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		_, _ = c.tryFollowRedirect("k", req, "node1", visited, 1)
		c.forgetOwner("k")
	}
}

// BenchmarkClusterPreferredNodeParallel measures learned-hit throughput under
// goroutine contention (Cluster.mu shared).
func BenchmarkClusterPreferredNodeParallel(b *testing.B) {
	c := newBenchCluster(b, 4)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
		c.learnOwner(keys[i], "node1")
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = c.preferredNode(keys[i%len(keys)])
			i++
		}
	})
}

// BenchmarkVisitedCheck compares the production slices.Contains visited
// check against a fixed-array linear scan prototype (hops <= 4, no alloc).
// Benchmark-only: does not change cluster.go.
func BenchmarkVisitedCheck(b *testing.B) {
	visited := []string{"node1", "node2", "node3"}
	var arr [4]string
	arr[0], arr[1], arr[2] = "node1", "node2", "node3"
	b.Run("slices-contains", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_ = slicesContains(visited, "node4")
		}
	})
	b.Run("fixed-array", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; b.Loop(); i++ {
			_ = fixedVisitedContains(arr, 3, "node4")
		}
	})
}

func slicesContains(hay []string, needle string) bool {
	return slices.Contains(hay, needle)
}

func fixedVisitedContains(arr [4]string, n int, needle string) bool {
	for i := range n {
		if arr[i] == needle {
			return true
		}
	}
	return false
}

// BenchmarkPoolSendPing measures one persistent-conn Ping through Pool.Send
// (conn reuse, buffer reuse, per-RPC deadline) against a stub server.
func BenchmarkPoolSendPing(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	stubServer(b, l)
	p := NewPool([]NodeAddr{{ID: "n1", Host: "127.0.0.1", Port: benchPortFromListener(b, l.Addr().String())}}, 0)
	defer p.Close()
	req := protocol.Request{Opcode: protocol.OpPing}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := p.Send("n1", req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClientDoPing measures Client.Do Ping directly (buffer reuse path).
func BenchmarkClientDoPing(b *testing.B) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	stubServer(b, l)
	c, err := Dial(l.Addr().String(), 0)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := c.Ping(); err != nil {
			b.Fatal(err)
		}
	}
}
