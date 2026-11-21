package tcp_test

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

func counterValue(t *testing.T, res protocol.Response) int64 {
	t.Helper()
	if res.Status != protocol.StatusOK || !res.HasValue {
		t.Fatalf("res = %+v, want OK with value", res)
	}
	c, err := counter.DecodePNCounter(res.Value)
	if err != nil {
		t.Fatal(err)
	}
	return c.Value()
}

func TestCounterAdjustGetRoundTrip(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	adj := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte("k"), CounterDelta: 5})
	if adj.Status != protocol.StatusOK {
		t.Fatalf("adjust = %v, want OK", adj.Status)
	}
	if got := counterValue(t, srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("k")})); got != 5 {
		t.Fatalf("value = %d, want 5", got)
	}
	// Decrement path.
	adj = srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte("k"), CounterDelta: 2, CounterDecrement: true})
	if adj.Status != protocol.StatusOK {
		t.Fatalf("decrement = %v, want OK", adj.Status)
	}
	if got := counterValue(t, srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("k")})); got != 3 {
		t.Fatalf("value = %d, want 3", got)
	}
	// Zero delta is a no-op OK that creates nothing.
	adj = srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte("fresh"), CounterDelta: 0})
	if adj.Status != protocol.StatusOK {
		t.Fatalf("zero adjust = %v, want OK", adj.Status)
	}
	if res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("fresh")}); res.Status != protocol.StatusNotFound {
		t.Fatalf("fresh get = %v, want NotFound", res.Status)
	}
}

func TestCounterAdjustResultReturnsAtomicPostState(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	for _, tc := range []struct {
		delta     uint64
		decrement bool
		want      int64
	}{
		{delta: 5, want: 5},
		{delta: 2, decrement: true, want: 3},
		{delta: 0, want: 3},
	} {
		res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjustResult, Key: []byte("atomic"), CounterDelta: tc.delta, CounterDecrement: tc.decrement})
		if res.Status != protocol.StatusOK || !res.HasValue || len(res.Value) != 8 {
			t.Fatalf("adjust-result = %+v, want OK + int64 value", res)
		}
		got := int64(binary.BigEndian.Uint64(res.Value))
		if got != tc.want {
			t.Fatalf("post-state = %d, want %d", got, tc.want)
		}
	}
}

func TestCounterAdjustResultConcurrentReturnsUniquePostStates(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	const n = 32
	values := make(chan int64, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjustResult, Key: []byte("race"), CounterDelta: 1})
			if res.Status != protocol.StatusOK || !res.HasValue || len(res.Value) != 8 {
				t.Errorf("adjust-result = %+v", res)
				return
			}
			values <- int64(binary.BigEndian.Uint64(res.Value))
		})
	}
	wg.Wait()
	close(values)
	seen := make(map[int64]bool, n)
	for v := range values {
		if seen[v] {
			t.Fatalf("duplicate post-state %d: response was not atomic", v)
		}
		seen[v] = true
	}
	if len(seen) != n || !seen[n] {
		t.Fatalf("unique post-states = %v, want 1..%d", seen, n)
	}
}

func TestCounterAdjustResultClientTCPRoundTrip(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Store: store.NewLRUStore(1<<20, nil)})
	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	t.Cleanup(func() {
		srv.Shutdown()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve did not stop")
		}
	})
	addr := l.Addr().String()
	c, err := client.Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.CounterAdjustResult("wire-result", 11, false)
	if err != nil {
		t.Fatal(err)
	}
	value, err := protocol.DecodeCounterAdjustResult(res)
	if err != nil || value != 11 {
		t.Fatalf("TCP adjust-result = %d, %v; want 11", value, err)
	}
	// Legacy opcode 17 remains bodyless and byte/behavior compatible.
	legacy, err := c.CounterAdjust("wire-result", 1, false)
	if err != nil || legacy.Status != protocol.StatusOK || legacy.HasValue {
		t.Fatalf("legacy adjust response changed: %+v %v", legacy, err)
	}
}

func TestCounterMergeApplies(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	var remote counter.PNCounter
	_ = remote.Increment("peer", 9)
	enc := remote.Encode()
	if res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterMerge, Key: []byte("k"), Value: enc}); res.Status != protocol.StatusOK {
		t.Fatalf("merge = %v, want OK", res.Status)
	}
	if got := counterValue(t, srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("k")})); got != 9 {
		t.Fatalf("value = %d, want 9", got)
	}
	// Malformed bodies fail closed.
	if res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterMerge, Key: []byte("k"), Value: []byte("bad")}); res.Status != protocol.StatusInvalidArgument {
		t.Fatalf("bad merge = %v, want InvalidArgument", res.Status)
	}
}

func TestCounterPersistenceAcrossServerRestart(t *testing.T) {
	dir := t.TempDir()
	start := func() (*tcp.Server, string) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := tcp.NewServer(tcp.ServerConfig{NodeID: "persist-node", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
		if err := srv.ConfigurePersistence(dir); err != nil {
			t.Fatal(err)
		}
		go srv.Serve(l)
		t.Cleanup(srv.Shutdown)
		return srv, l.Addr().String()
	}

	srv, addr := start()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []protocol.Request{
		{Opcode: protocol.OpCounterAdjust, Key: []byte("count"), CounterDelta: 11},
		{Opcode: protocol.OpCounterAdjust, Key: []byte("count"), CounterDelta: 4, CounterDecrement: true},
	} {
		if res, err := c.Do(req); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("counter write: %+v %v", res, err)
		}
	}
	c.Close()
	if err := srv.Compact(); err != nil {
		t.Fatal(err)
	}
	srv.Shutdown()

	_, addr = start()
	c, err = client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("count")})
	if err != nil {
		t.Fatal(err)
	}
	if got := counterValue(t, res); got != 7 {
		t.Fatalf("counter after restart = %d, want 7", got)
	}
}

func TestCounterOwnership(t *testing.T) {
	c := newTestCluster(t, []string{"n1", "n2"}, 2, 0)
	defer func() {
		for _, n := range c.nodes {
			n.srv.Shutdown()
		}
	}()
	// Find a key owned by n2 (from n1's ring view).
	n1 := c.nodes["n1"].srv
	var key string
	for i := range 1000 {
		cand := "counter-key-" + string([]byte{byte(i), byte(i >> 8)})
		// Owner check via Get miss: non-owner answers NotReady.
		res := n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(cand)})
		if res.Status == protocol.StatusNotReady {
			key = cand
			break
		}
	}
	if key == "" {
		t.Skip("no non-owned key found in 1000 tries")
	}
	// Adjust to a non-owned key redirects; merge applies anywhere (it is
	// the replication target, like OpReplicate).
	if res := n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterAdjust, Key: []byte(key), CounterDelta: 1}); res.Status != protocol.StatusNotReady {
		t.Fatalf("non-owner adjust = %v, want NotReady", res.Status)
	}
	var remote counter.PNCounter
	_ = remote.Increment("n2", 4)
	if res := n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterMerge, Key: []byte(key), Value: remote.Encode()}); res.Status != protocol.StatusOK {
		t.Fatalf("non-owner merge = %v, want OK", res.Status)
	}
	if got := counterValue(t, n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(key)})); got != 4 {
		t.Fatalf("merged value = %d, want 4", got)
	}
}

func TestCounterAntiEntropyNoManager(t *testing.T) {
	// A standalone server has no AE manager, so counter anti-entropy is
	// unsupported.
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "n1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAntiEntropy, Value: []byte("x")})
	if res.Status != protocol.StatusNotSupported {
		t.Fatalf("ae = %v, want NotSupported", res.Status)
	}
}

func TestClusterCounterFollowsRedirect(t *testing.T) {
	c := newTestCluster(t, []string{"n1", "n2"}, 2, 0)
	defer func() {
		for _, n := range c.nodes {
			n.srv.Shutdown()
		}
	}()
	addrs := make([]client.NodeAddr, 0, len(c.nodes))
	for id, n := range c.nodes {
		_, portStr, err := net.SplitHostPort(n.addr)
		if err != nil {
			t.Fatal(err)
		}
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, client.NodeAddr{ID: id, Host: "127.0.0.1", Port: netport.Port(port)})
	}
	cl, err := client.NewCluster(client.ClusterConfig{Nodes: addrs, MaxRetries: 3, BaseBackoff: time.Millisecond, RPCTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if res, err := cl.CounterAdjust("shared", 4, false); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("cluster adjust = %v %v, want OK", res.Status, err)
	}
	res, err := cl.CounterGet("shared")
	if err != nil {
		t.Fatal(err)
	}
	if st, err := counter.DecodePNCounter(res.Value); err != nil || st.Value() != 4 {
		t.Fatalf("cluster get = %v %v, want 4", st.Value(), err)
	}
}
