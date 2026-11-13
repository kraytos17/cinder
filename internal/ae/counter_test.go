package ae

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/metrics"
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
	m1 := NewManager(Config{Store: st1, Ring: ring, Self: "n1", Transport: ft, FetchBytes: fetchBytes})
	m2 := NewManager(Config{Store: st2, Ring: ring, Self: "n2", Transport: ft, FetchBytes: fetchBytes})
	cs1, cs2 := counter.NewStore(1<<20), counter.NewStore(1<<20)
	m1.SetCounterStore(cs1)
	m2.SetCounterStore(cs2)
	ft.mgrs["n1"] = m1
	ft.mgrs["n2"] = m2
	return m1, m2, cs1, cs2
}

func TestCounterRoundConverges(t *testing.T) {
	m1, _, cs1, cs2 := counterTestPair(t, 1<<20)
	cs1.Increment("k", "n1", 5)
	cs2.Increment("k", "n2", 7)
	cs2.Increment("only2", "n2", 3)
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

// Partition healing with reordered and duplicate deliveries converges:
// merge newest, then stale, then newest-again; the LWW-free merge is
// idempotent and order-insensitive, so both sides land on 7-4=3.
func TestCounterPartitionHealingReorderedDuplicateConverge(t *testing.T) {
	sender := counter.NewStore(1 << 20)
	if err := sender.Increment("k", "n1", 7); err != nil {
		t.Fatal(err)
	}
	if err := sender.Decrement("k", "n2", 4); err != nil {
		t.Fatal(err)
	}
	var stale PNCounter
	if err := stale.Increment("n1", 2); err != nil {
		t.Fatal(err)
	}
	receiver := counter.NewStore(1 << 20)
	newest, ok := sender.Get("k")
	if !ok {
		t.Fatal("sender missing k")
	}
	for _, state := range []PNCounter{newest, stale, newest} {
		if err := receiver.Merge("k", state); err != nil {
			t.Fatal(err)
		}
	}
	if v, ok := receiver.Value("k"); !ok || v != 3 {
		t.Fatalf("k = %d %v, want 3 after reorder+duplicate", v, ok)
	}
	if v, _ := sender.Value("k"); v != 3 {
		t.Fatalf("sender k = %d, want 3", v)
	}
}

// A merge rejected for capacity leaves the resident untouched; after
// expansion the same serialized state applies and matches the sender.
func TestCounterCapacityRejectedRetryAfterExpansion(t *testing.T) {
	sender := counter.NewStore(1 << 20)
	if err := sender.Increment("big", "n1", 9); err != nil {
		t.Fatal(err)
	}
	state, ok := sender.Get("big")
	if !ok {
		t.Fatal("sender missing big")
	}
	receiver := counter.NewStore(1)
	if err := receiver.Merge("big", state); !errors.Is(err, counter.ErrCapacityExceeded) {
		t.Fatalf("merge = %v, want capacity rejection", err)
	}
	if _, ok := receiver.Get("big"); ok {
		t.Fatal("failed merge planted state")
	}
	receiver.SetCapacity(256)
	if err := receiver.Merge("big", state); err != nil {
		t.Fatal(err)
	}
	if v, _ := receiver.Value("big"); v != 9 {
		t.Fatalf("big = %d, want 9", v)
	}
}

// Dropped counter merges heal through the AE round with no LWW traffic:
// the peer is down for the first round, then converges once reachable.
func TestCounterAntiEntropyRepairsDroppedMerge(t *testing.T) {
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	ring.AddNode("n1")
	ring.AddNode("n2")
	ft := &counterTransport{mgrs: map[string]*Manager{}, fail: map[string]bool{}}
	st1, st2 := store.NewLRUStore(1<<20, nil), store.NewLRUStore(1<<20, nil)
	m1 := NewManager(Config{Store: st1, Ring: ring, Self: "n1", Transport: ft, FetchBytes: 1 << 20})
	m2 := NewManager(Config{Store: st2, Ring: ring, Self: "n2", Transport: ft, FetchBytes: 1 << 20})
	cs1, cs2 := counter.NewStore(1<<20), counter.NewStore(1<<20)
	m1.SetCounterStore(cs1)
	m2.SetCounterStore(cs2)
	ft.mgrs["n1"] = m1
	ft.mgrs["n2"] = m2
	if err := cs1.Increment("k", "n1", 5); err != nil {
		t.Fatal(err)
	}
	ft.fail["n2"] = true
	m1.RunCounterRound(2)
	if _, ok := cs2.Get("k"); ok {
		t.Fatal("peer healed while down")
	}
	ft.fail["n2"] = false
	m1.RunCounterRound(2)
	if v, ok := cs2.Value("k"); !ok || v != 5 {
		t.Fatalf("k = %d %v, want 5 after heal", v, ok)
	}
	if st1.Size() != 0 || st2.Size() != 0 {
		t.Fatal("counter repair must not touch LWW stores")
	}
}

// Bounded budgets converge across rounds without starving keys behind
// the cursor: with a 128-byte budget both keys land within 16 rounds.
func TestCounterCursorRepairsAcrossBoundedBatches(t *testing.T) {
	m1, _, cs1, cs2 := counterTestPair(t, 128)
	if err := cs1.Increment("a", "n1", 1); err != nil {
		t.Fatal(err)
	}
	if err := cs1.Increment("b", "n1", 3); err != nil {
		t.Fatal(err)
	}
	for range 16 {
		m1.RunCounterRound(2)
	}
	if v, ok := cs2.Value("a"); !ok || v != 1 {
		t.Fatalf("a = %d %v, want 1", v, ok)
	}
	if v, ok := cs2.Value("b"); !ok || v != 3 {
		t.Fatalf("b = %d %v, want 3", v, ok)
	}
}

// A batch budget smaller than one record defers the send: no round is
// counted, the cursor does not advance, and a later adequate round
// still delivers from the start (nothing stranded behind the cursor).
func TestCounterRejectsCounterLargerThanBatchBudget(t *testing.T) {
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	ring.AddNode("n1")
	ring.AddNode("n2")
	ft := &counterTransport{mgrs: map[string]*Manager{}, fail: map[string]bool{}}
	st1, st2 := store.NewLRUStore(1<<20, nil), store.NewLRUStore(1<<20, nil)
	// Sender budget too small for one record; responder adequate.
	m1 := NewManager(Config{Store: st1, Ring: ring, Self: "n1", Transport: ft, FetchBytes: 32})
	m2 := NewManager(Config{Store: st2, Ring: ring, Self: "n2", Transport: ft, FetchBytes: 1 << 20})
	cs1, cs2 := counter.NewStore(1<<20), counter.NewStore(1<<20)
	m1.SetCounterStore(cs1)
	m2.SetCounterStore(cs2)
	ft.mgrs["n1"] = m1
	ft.mgrs["n2"] = m2
	c := metrics.NewCollector()
	m1.SetMetrics(c)
	if err := cs1.Increment("k", "n1", 1); err != nil {
		t.Fatal(err)
	}
	m1.RunCounterRound(2)
	if got := c.CounterAERounds.Load(); got != 0 {
		t.Fatalf("rounds = %d, want 0 (deferred, not attempted)", got)
	}
	if _, ok := cs2.Get("k"); ok {
		t.Fatal("oversize budget sent state")
	}
	if m1.counterCur != "" {
		t.Fatalf("cursor advanced to %q on a deferred round", m1.counterCur)
	}
	m1.fetchBytes = 1 << 20
	m1.RunCounterRound(2)
	if v, ok := cs2.Value("k"); !ok || v != 1 {
		t.Fatalf("k = %d %v, want 1 after adequate round", v, ok)
	}
}

func TestCounterBatchEncodedSizeStaysWithinBudget(t *testing.T) {
	m1, _, cs1, _ := counterTestPair(t, 512)
	for i := range 100 {
		key := fmt.Sprintf("counter-%03d", i)
		actor := fmt.Sprintf("actor-%02d", i%7)
		if err := cs1.Increment(key, actor, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	for range 50 {
		batch := m1.EncodeCounterBatch(512)
		if len(batch) > 512 {
			t.Fatalf("encoded batch bytes=%d, budget=512", len(batch))
		}
		if len(batch) == 8 {
			break // canonical empty CAE1 bundle
		}
		entries, err := counter.DecodeCounterBundle(batch)
		if err != nil {
			t.Fatalf("decode batch: %v", err)
		}
		if len(entries) == 0 {
			t.Fatal("non-empty batch decoded to zero entries")
		}
	}
}

func TestCounterBatchCursorWraps(t *testing.T) {
	m1, _, cs1, _ := counterTestPair(t, 0)
	cs1.Increment("a", "n1", 1)
	cs1.Increment("b", "n1", 1)
	cs1.Increment("c", "n1", 1)
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
	cs1.Increment("k", "n1", 1)
	full := counter.EncodeCounterBundle(cs1.SortedLiveCounters())
	if st, _ := m1.OnCounterAntiEntropy(full); st != protocol.StatusCapacityExceeded {
		t.Fatalf("oversize bundle st = %v, want CapacityExceeded", st)
	}
	if st, _ := m1.OnCounterAntiEntropy([]byte("garbage")); st != protocol.StatusInvalidArgument {
		t.Fatalf("malformed bundle st = %v, want InvalidArgument", st)
	}
}
