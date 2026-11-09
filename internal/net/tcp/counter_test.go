package tcp_test

import (
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
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
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
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

func TestCounterMergeApplies(t *testing.T) {
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
	var remote counter.PNCounter
	_ = remote.Increment("peer", 9)
	enc := counter.EncodePNCounter(remote)
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
	for i := 0; i < 1000; i++ {
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
	if res := n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterMerge, Key: []byte(key), Value: counter.EncodePNCounter(remote)}); res.Status != protocol.StatusOK {
		t.Fatalf("non-owner merge = %v, want OK", res.Status)
	}
	if got := counterValue(t, n1.Dispatch(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte(key)})); got != 4 {
		t.Fatalf("merged value = %d, want 4", got)
	}
}

func TestCounterAntiEntropyNoManager(t *testing.T) {
	// Standalone server has no AE manager: NotSupported, mirroring C++
	// with a nil anti_entropy_.
	srv := tcp.NewServer("n1", 0, store.NewLRUStore(1<<20, nil), nil)
	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpCounterAntiEntropy, Value: []byte("x")})
	if res.Status != protocol.StatusNotSupported {
		t.Fatalf("ae = %v, want NotSupported", res.Status)
	}
}
