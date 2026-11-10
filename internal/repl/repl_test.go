package repl

import (
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWriterHashIsFNV1a(t *testing.T) {
	m := NewManager(store.NewLRUStore(1024, nil), "foobar", nil, testkit.NewStores())
	if m.WriterHash() != 0x85944171f73967e8 {
		t.Fatalf("writer = %#x", m.WriterHash())
	}
	empty := NewManager(store.NewLRUStore(1024, nil), "", nil, testkit.NewStores())
	if empty.WriterHash() != 0xcbf29ce484222325 {
		t.Fatalf("empty writer = %#x", empty.WriterHash())
	}
}

func TestWriteAsyncFansOut(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Local().GetVersioned("k"); !ok {
		t.Fatal("local commit missing")
	}
	waitFor(t, "fan-out", func() bool { return ft.SendCount("n2") == 1 && ft.SendCount("n3") == 1 })
	for _, n := range []string{"n2", "n3"} {
		if e, ok := ft.Stores[n].GetVersioned("k"); !ok || string(e.Value) != "v" {
			t.Fatalf("%s missing replica: %+v", n, e)
		}
	}
}

func TestWriteAsyncHintOnFailure(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	ft.Fail["n3"] = true
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
}

func TestWriteQuorum(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	// RF=3, all up: W=2 (local + 1). Stragglers drain in the background,
	// Async fan-out returns before replica sends complete, so replica
	// assertions poll for completion.
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Quorum, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replicas", func() bool {
		for _, n := range []string{"n2", "n3"} {
			if e, ok := ft.Stores[n].GetVersioned("k"); !ok || string(e.Value) != "v" {
				return false
			}
		}
		return true
	})
	// One replica down: local + survivor = W. Succeeds, hint queued.
	ft.Fail["n3"] = true
	if err := m.Write("k2", []byte("v"), 0, []string{"n2", "n3"}, Quorum, 0, 0); err != nil {
		t.Fatalf("quorum with one failure: %v", err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
	// Two down: local alone cannot reach W=2.
	ft.Fail["n2"] = true
	if err := m.Write("k3", []byte("v"), 0, []string{"n2", "n3"}, Quorum, 0, 0); !errors.Is(err, ErrQuorum) {
		t.Fatalf("expected ErrQuorum, got %v", err)
	}
}

func TestQuorumEmptyReplicas(t *testing.T) {
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, testkit.NewStores())
	if err := m.Write("k", []byte("v"), 0, nil, Quorum, 0, 0); err != nil {
		t.Fatalf("W==1 must succeed on local: %v", err)
	}
}

func TestDeleteQuorum(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Quorum, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Delete("k", []string{"n2", "n3"}, Quorum, 0, 0); err != nil {
		t.Fatal(err)
	}
	// The quorum decision fires at W; straggler replicas drain in the
	// background, so convergence assertions poll.
	waitFor(t, "tombstone everywhere", func() bool {
		for _, st := range []*store.Store{m.Local(), ft.Stores["n2"], ft.Stores["n3"]} {
			e, ok := st.GetVersioned("k")
			if !ok || !e.Tombstone {
				return false
			}
			if _, ok := st.Get("k"); ok {
				return false
			}
		}
		return true
	})
}

func TestReadLocalOnly(t *testing.T) {
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, testkit.NewStores())
	if err := m.Local().Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	e, err := m.Read("k", nil, 1, 0, 0)
	if err != nil || string(e.Value) != "v" {
		t.Fatalf("got %+v, %v", e, err)
	}
	if _, err := m.Read("missing", nil, 1, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestReadQuorumLWWAndRepair(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	// Divergent versions: n2 ahead, local stale, n3 empty.
	putVer := func(st *store.Store, v string, ver uint64, w uint64) {
		t.Helper()
		if err := st.PutVersioned("k", store.Entry{Value: []byte(v), Version: ver, WriterNodeHash: w}); err != nil {
			t.Fatal(err)
		}
	}
	putVer(m.Local(), "old", 1, 1)
	putVer(ft.Stores["n2"], "new", 5, 2)
	e, err := m.Read("k", []string{"n2", "n3"}, 2, 0, 0)
	if err != nil || string(e.Value) != "new" || e.Version != 5 {
		t.Fatalf("got %+v, %v", e, err)
	}
	// Local healed.
	waitFor(t, "local heal", func() bool {
		le, ok := m.Local().GetVersioned("k")
		return ok && le.Version == 5
	})
	// Empty replica repaired with the winner.
	waitFor(t, "replica repair", func() bool {
		re, ok := ft.Stores["n3"].GetVersioned("k")
		return ok && re.Version == 5 && string(re.Value) == "new"
	})
}

func TestReadQuorumMiss(t *testing.T) {
	ft := testkit.NewStores("n2")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	if _, err := m.Read("ghost", []string{"n2"}, 2, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestReadIgnoresTransportFailure(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	ft.Fail["n3"] = true
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	if err := m.Local().Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := ft.Stores["n2"].Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	e, err := m.Read("k", []string{"n2", "n3"}, 2, 0, 0)
	if err != nil || string(e.Value) != "v" {
		t.Fatalf("got %+v, %v", e, err)
	}
}

// gateTransport blocks every send until released, forcing concurrent reads
// to overlap in one flight.
type gateTransport struct {
	inner   *testkit.Stores
	release chan struct{}
}

func (g *gateTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	<-g.release
	return g.inner.Send(node, req)
}

func TestReadSingleflight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := testkit.NewStores("n2")
		gt := &gateTransport{inner: inner, release: make(chan struct{})}
		m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, gt)
		if err := m.Local().Put("k", []byte("v")); err != nil {
			t.Fatal(err)
		}
		// Identical stamp on the replica: no divergence, so no repair
		// fan-out pollutes the send count.
		le, _ := m.Local().GetVersioned("k")
		if err := inner.Stores["n2"].PutVersioned("k", le); err != nil {
			t.Fatal(err)
		}
		const readers = 16
		var wg sync.WaitGroup
		errs := make([]error, readers)
		for i := range readers {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				_, errs[i] = m.Read("k", []string{"n2"}, 2, 0, 0)
			}(i)
		}
		// Park every reader first (leader blocks in the transport gate, the
		// rest in the flight) instead of sleeping a fixed 200ms: synctest
		// waits exactly until all bubble goroutines are durably blocked.
		synctest.Wait()
		close(gt.release)
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if n := inner.SendCount("n2"); n != 1 {
			t.Fatalf("singleflight: %d fan-outs for %d readers", n, readers)
		}
	})
}

func TestHintCoalesce(t *testing.T) {
	q := NewHintQueue()
	mk := func(v uint64, w uint64) protocol.Request {
		return protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"), Version: v, WriterNodeHash: w}
	}
	if !q.Push(Hint{Target: "n2", Req: mk(1, 1), ExpiresAt: expiry.At(time.Now().Add(time.Minute))}) {
		t.Fatal("first push")
	}
	if q.Len() != 1 {
		t.Fatal("coalesce must keep one entry")
	}
	// Identical stamp counts as newer (lwwCovers): replace, still one entry.
	if !q.Push(Hint{Target: "n2", Req: mk(1, 1), ExpiresAt: expiry.At(time.Now().Add(time.Minute))}) {
		t.Fatal("identical stamp must replace")
	}
	if q.Len() != 1 {
		t.Fatal("replace must not duplicate")
	}
	// Older stamp is dropped.
	old := mk(1, 1)
	old.Version = 0
	if q.Push(Hint{Target: "n2", Req: old, ExpiresAt: expiry.At(time.Now().Add(time.Minute))}) {
		t.Fatal("stale hint accepted")
	}
}

func TestHintQueueBounds(t *testing.T) {
	q := NewHintQueue()
	q.SetMaxBytes(500)
	mk := func(k string, n int) Hint {
		return Hint{Target: "n2", Req: protocol.Request{Opcode: protocol.OpReplicate, Key: []byte(k), Value: make([]byte, n)}, ExpiresAt: expiry.At(time.Now().Add(time.Minute))}
	}
	// Oversized single hint can never fit: dropped.
	if q.Push(mk("big", 1000)) {
		t.Fatal("oversized push accepted")
	}
	if q.Dropped() != 1 {
		t.Fatalf("dropped = %d", q.Dropped())
	}
	// Fill past the byte budget: oldest evicted first.
	for i := range 10 {
		q.Push(mk(string(rune('a'+i)), 10))
	}
	if q.Len() == 10 {
		t.Fatal("budget must evict oldest")
	}
	if q.Dropped() == 1 {
		t.Fatal("evictions must count as drops")
	}
}

func TestHintTTLExpiry(t *testing.T) {
	q := NewHintQueue()
	now := time.Now()
	q.Push(Hint{Target: "n2", Req: protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("k")}, ExpiresAt: expiry.At(now.Add(time.Second))})
	if len(q.Snapshot(now, 256)) != 1 {
		t.Fatal("live hint missing")
	}
	if len(q.Snapshot(now.Add(2*time.Second), 256)) != 0 {
		t.Fatal("expired hint survived")
	}
	if q.Len() != 0 {
		t.Fatal("expired hint not reaped")
	}
}

func TestReplayRemovesOnSuccess(t *testing.T) {
	ft := testkit.NewStores("n2")
	m := NewManager(store.NewLRUStore(1<<20, nil), "n1", nil, ft)
	ft.Fail["n2"] = true
	if err := m.Write("k", []byte("v"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
	ft.Fail["n2"] = false
	if n := m.ReplayHints(); n != 1 {
		t.Fatalf("replayed = %d", n)
	}
	if m.HintCount() != 0 {
		t.Fatal("replayed hint must be removed")
	}
	if e, ok := ft.Stores["n2"].GetVersioned("k"); !ok || string(e.Value) != "v" {
		t.Fatalf("replica missing: %+v", e)
	}
	// Empty replay returns zero.
	if n := m.ReplayHints(); n != 0 {
		t.Fatalf("replayed = %d", n)
	}
}
