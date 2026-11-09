package ae

import (
	"errors"
	"sync"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// counterTransport routes CounterAntiEntropy to per-node managers and
// CounterMerge into per-node stores; everything else is unsupported.
type counterTransport struct {
	mu   sync.Mutex
	mgrs map[string]*Manager
	fail map[string]bool
}

func (f *counterTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail[node] {
		return protocol.Response{}, errors.New("unreachable: " + node)
	}
	m, ok := f.mgrs[node]
	if !ok {
		return protocol.Response{}, errors.New("unknown node: " + node)
	}
	switch req.Opcode {
	case protocol.OpCounterAntiEntropy:
		st, val := m.OnCounterAntiEntropy(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, nil
	case protocol.OpCounterMerge:
		c, err := counter.DecodePNCounter(req.Value)
		if err != nil {
			return protocol.Response{Status: protocol.StatusInvalidArgument}, nil
		}
		if err := m.counterStore().Merge(string(req.Key), c); err != nil {
			return protocol.Response{Status: protocol.StatusCapacityExceeded}, nil
		}
		return protocol.Response{Status: protocol.StatusOK}, nil
	default:
		return protocol.Response{Status: protocol.StatusNotSupported}, nil
	}
}

func (m *Manager) counterStore() *counter.Store {
	m.counterMu.Lock()
	defer m.counterMu.Unlock()
	if cs, ok := m.counters.(*counter.Store); ok {
		return cs
	}
	return nil
}

func counterTestPair(t *testing.T, fetchBytes int) (*Manager, *Manager, *counter.Store, *counter.Store) {
	t.Helper()
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	ring.AddNode("n1")
	ring.AddNode("n2")
	ft := &counterTransport{mgrs: map[string]*Manager{}, fail: map[string]bool{}}
	st1, st2 := store.NewLRUStore(1<<20, nil), store.NewLRUStore(1<<20, nil)
	m1 := NewManager(st1, ring, "n1", ft, 0, fetchBytes)
	m2 := NewManager(st2, ring, "n2", ft, 0, fetchBytes)
	cs1, cs2 := counter.NewStore(1<<20), counter.NewStore(1<<20)
	m1.SetCounterStore(cs1)
	m2.SetCounterStore(cs2)
	ft.mgrs["n1"] = m1
	ft.mgrs["n2"] = m2
	return m1, m2, cs1, cs2
}

func TestCounterRoundConverges(t *testing.T) {
	m1, _, cs1, cs2 := counterTestPair(t, 1<<20)
	_ = cs1.Increment("k", "n1", 5)
	_ = cs2.Increment("k", "n2", 7)
	_ = cs2.Increment("only2", "n2", 3)
	m1.RunCounterRound(2)
	if v, _ := cs1.Value("k"); v != 12 {
		t.Fatalf("k = %d, want 12", v)
	}
	if v, ok := cs1.Value("only2"); !ok || v != 3 {
		t.Fatalf("only2 = %d %v, want 3", v, ok)
	}
}

func TestCounterRoundEmptySkips(t *testing.T) {
	m1, _, _, cs2 := counterTestPair(t, 1<<20)
	// m1's store is empty: no partner traffic, no panic.
	m1.RunCounterRound(2)
	if cs2.Size() != 0 {
		t.Fatal("empty initiator sent state")
	}
}

func TestCounterBatchCursorWraps(t *testing.T) {
	m1, _, cs1, _ := counterTestPair(t, 0)
	_ = cs1.Increment("a", "n1", 1)
	_ = cs1.Increment("b", "n1", 1)
	_ = cs1.Increment("c", "n1", 1)
	// Tiny budget: one record per batch; successive batches advance.
	first := m1.EncodeCounterBatch(64)
	dec1, err := counter.DecodeCounterBundle(first)
	if err != nil || len(dec1) != 1 || dec1[0].Key != "a" {
		t.Fatalf("batch1 = %v %v", dec1, err)
	}
	second := m1.EncodeCounterBatch(64)
	dec2, err := counter.DecodeCounterBundle(second)
	if err != nil || len(dec2) != 1 || dec2[0].Key != "b" {
		t.Fatalf("batch2 = %v %v", dec2, err)
	}
	third := m1.EncodeCounterBatch(64)
	dec3, err := counter.DecodeCounterBundle(third)
	if err != nil || len(dec3) != 1 || dec3[0].Key != "c" {
		t.Fatalf("batch3 = %v %v", dec3, err)
	}
	// Wrap: fourth batch restarts at the head.
	fourth := m1.EncodeCounterBatch(64)
	dec4, err := counter.DecodeCounterBundle(fourth)
	if err != nil || len(dec4) != 1 || dec4[0].Key != "a" {
		t.Fatalf("batch4 = %v %v, want wrap to a", dec4, err)
	}
}

func TestOnCounterRejectsOversizeAndMalformed(t *testing.T) {
	m1, _, cs1, _ := counterTestPair(t, 16)
	_ = cs1.Increment("k", "n1", 1)
	full := counter.EncodeCounterBundle(cs1.SortedLiveCounters())
	if st, _ := m1.OnCounterAntiEntropy(full); st != protocol.StatusCapacityExceeded {
		t.Fatalf("oversize bundle st = %v, want CapacityExceeded", st)
	}
	if st, _ := m1.OnCounterAntiEntropy([]byte("garbage")); st != protocol.StatusInvalidArgument {
		t.Fatalf("malformed bundle st = %v, want InvalidArgument", st)
	}
}
