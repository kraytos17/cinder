package repl

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/hashring"
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

func TestParseMode(t *testing.T) {
	for input, want := range map[string]Mode{"async": Async, "quorum": Quorum} {
		got, err := ParseMode(input)
		if err != nil || got != want {
			t.Errorf("ParseMode(%q) = %v, %v; want %v", input, got, err, want)
		}
	}
	if _, err := ParseMode("ASYNC"); err == nil {
		t.Fatal("ParseMode accepted non-canonical name")
	}
}

func TestReadRejectsInvalidReplicaCount(t *testing.T) {
	m := NewManager(Config{Local: store.NewLRUStore(1024, nil), Self: "n1"})
	for _, count := range []hashring.ReplicaCount{0, hashring.ReplicaCount(hashring.MaxReplicas + 1)} {
		if _, err := m.Read("key", []string{"n2"}, count, 0, 0); !errors.Is(err, ErrInvalidReplicaCount) {
			t.Errorf("Read replica count %d error = %v", count, err)
		}
	}
}

func TestWriteAndDeleteRejectInvalidModeBeforeMutation(t *testing.T) {
	local := store.NewLRUStore(1<<20, nil)
	m := NewManager(Config{Local: local, Self: "n1"})
	mode := Mode(99)
	if err := m.Write("key", []byte("value"), 0, nil, mode, 0, 0); !errors.Is(err, ErrInvalidMode) {
		t.Fatalf("Write error = %v", err)
	}
	if _, ok := local.GetVersioned("key"); ok {
		t.Fatal("invalid-mode Write mutated the local store")
	}
	if err := m.Delete("key", nil, mode, 0, 0); !errors.Is(err, ErrInvalidMode) {
		t.Fatalf("Delete error = %v", err)
	}
	if _, ok := local.GetVersioned("key"); ok {
		t.Fatal("invalid-mode Delete mutated the local store")
	}
}

func TestWriterHashIsFNV1a(t *testing.T) {
	m := NewManager(Config{Local: store.NewLRUStore(1024, nil), Self: "foobar", Clock: nil, Transport: testkit.NewStores()})
	if m.WriterHash() != 0x85944171f73967e8 {
		t.Fatalf("writer = %#x", m.WriterHash())
	}
	empty := NewManager(Config{Local: store.NewLRUStore(1024, nil), Self: "", Clock: nil, Transport: testkit.NewStores()})
	if empty.WriterHash() != 0xcbf29ce484222325 {
		t.Fatalf("empty writer = %#x", empty.WriterHash())
	}
}

func TestWriteAsyncFansOut(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	if err := m.Write("k", []byte("v"), 0, []string{"n2", "n3"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
}

func TestWriteQuorum(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: testkit.NewStores()})
	if err := m.Write("k", []byte("v"), 0, nil, Quorum, 0, 0); err != nil {
		t.Fatalf("W==1 must succeed on local: %v", err)
	}
}

func TestDeleteQuorum(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: testkit.NewStores()})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	if _, err := m.Read("ghost", []string{"n2"}, 2, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestReadIgnoresTransportFailure(t *testing.T) {
	ft := testkit.NewStores("n2", "n3")
	ft.Fail["n3"] = true
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
		m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: gt})
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

// Version 0 is the reserved no-version sentinel (StatusOK is the zero
// value, so a zero-value Response must never vote). The reducer drops
// version-0 OK votes; only a nonzero version counts as an ack.
func TestReducerIgnoresVersionZero(t *testing.T) {
	var d reducer
	d.r, d.pending = 2, 1
	d.noteRemote(protocol.Response{}, nil)
	if d.acks != 0 || d.hasBest {
		t.Fatalf("zero-value response voted: %+v", d)
	}
	if _, ok := d.winner(); ok {
		t.Fatal("phantom winner from zero-value response")
	}

	d.noteRemote(protocol.Response{Status: protocol.StatusOK, Version: 0, WriterNodeHash: 7}, nil)
	if d.acks != 0 || d.hasBest {
		t.Fatalf("version-0 vote counted: %+v", d)
	}

	d.noteRemote(protocol.Response{Status: protocol.StatusOK, Version: 5, WriterNodeHash: 7, Value: []byte("v"), HasValue: true}, nil)
	if d.acks != 1 || !d.hasBest {
		t.Fatalf("versioned vote lost: %+v", d)
	}
}

// End to end: a replica holding only an explicit version-0 entry must not
// produce a phantom winner; the read is a miss.
func TestReadVersionZeroIsMiss(t *testing.T) {
	ft := testkit.NewStores("n2")
	if err := ft.Stores["n2"].PutVersioned("k", store.Entry{Version: 0, WriterNodeHash: 1}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
	if _, err := m.Read("k", []string{"n2"}, 2, 0, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("version-0 read = %v, want ErrNotFound", err)
	}
}

// Hint coalescing applies the same tombstone-wins tie-break as reads: a
// live hint at a tied stamp must not replace a queued tombstone, while a
// tombstone at a tied stamp replaces a queued live hint.
func TestHintCoalesceTombstoneTie(t *testing.T) {
	mk := func(v, w uint64, tomb bool) protocol.Request {
		return protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("k"), Value: []byte("v"), Version: v, WriterNodeHash: w, Tombstone: tomb}
	}
	exp := expiry.At(time.Now().Add(time.Minute))

	q := NewHintQueue()
	if !q.Push(Hint{Target: "n2", Req: mk(5, 1, true), ExpiresAt: exp}) {
		t.Fatal("tombstone push")
	}
	if q.Push(Hint{Target: "n2", Req: mk(5, 1, false), ExpiresAt: exp}) {
		t.Fatal("live hint replaced a tied tombstone")
	}
	if q.Len() != 1 || !q.items[0].Req.Tombstone {
		t.Fatalf("tombstone lost: %+v", q.items)
	}

	q2 := NewHintQueue()
	if !q2.Push(Hint{Target: "n2", Req: mk(5, 1, false), ExpiresAt: exp}) {
		t.Fatal("live push")
	}
	if !q2.Push(Hint{Target: "n2", Req: mk(5, 1, true), ExpiresAt: exp}) {
		t.Fatal("tombstone must replace tied live hint")
	}
	if q2.Len() != 1 || !q2.items[0].Req.Tombstone {
		t.Fatalf("tombstone did not win: %+v", q2.items)
	}
}

// Flights are keyed by (key, R, replica set): concurrent reads for the
// same key and R but different replica sets must not share a flight.
// A straggler must not hold the decision: with local + one fast replica
// satisfying R=2, the read returns before the slow replica responds, and
// the background drain still consumes its late arrival (no leak —
// goleak-verified via the eventual send count).
func TestReadDoesNotWaitForStragglers(t *testing.T) {
	inner := testkit.NewStores("n2", "n3")
	st := &stragglerTransport{inner: inner, delay: map[string]time.Duration{"n3": 50 * time.Millisecond}}
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: st})
	if err := m.Local().Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	le, _ := m.Local().GetVersioned("k")
	for _, n := range []string{"n2", "n3"} {
		if err := inner.Stores[n].PutVersioned("k", le); err != nil {
			t.Fatal(err)
		}
	}
	e, err := m.Read("k", []string{"n2", "n3"}, 2, 0, 0)
	if err != nil || string(e.Value) != "v" {
		t.Fatalf("got %+v, %v", e, err)
	}
	if st.responded.Load() {
		t.Fatal("decision waited for the straggler")
	}
	waitFor(t, "straggler drain", func() bool { return inner.SendCount("n3") == 1 })
}

// stragglerTransport delays sends to listed nodes, marking when a delayed
// response is served.
type stragglerTransport struct {
	inner     *testkit.Stores
	delay     map[string]time.Duration
	responded atomic.Bool
}

func (s *stragglerTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	if d := s.delay[node]; d > 0 {
		time.Sleep(d)
		s.responded.Store(true)
	}
	return s.inner.Send(node, req)
}

func TestReadSingleflightSeparatesReplicaSets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		inner := testkit.NewStores("n2", "n3")
		gt := &gateTransport{inner: inner, release: make(chan struct{})}
		m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: gt})
		le2 := store.Entry{Value: []byte("from-n2"), Version: 3, WriterNodeHash: 1}
		le3 := store.Entry{Value: []byte("from-n3"), Version: 3, WriterNodeHash: 1}
		if err := inner.Stores["n2"].PutVersioned("k", le2); err != nil {
			t.Fatal(err)
		}
		if err := inner.Stores["n3"].PutVersioned("k", le3); err != nil {
			t.Fatal(err)
		}
		var va, vb store.Entry
		var erra, errb error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); va, erra = m.Read("k", []string{"n2"}, 2, 0, 0) }()
		go func() { defer wg.Done(); vb, errb = m.Read("k", []string{"n3"}, 2, 0, 0) }()
		synctest.Wait()
		close(gt.release)
		wg.Wait()
		if erra != nil || errb != nil {
			t.Fatalf("errs = %v, %v", erra, errb)
		}
		if string(va.Value) != "from-n2" || string(vb.Value) != "from-n3" {
			t.Fatalf("cross-talk: a=%q b=%q", va.Value, vb.Value)
		}
		// Both replica sets were contacted (repair fan-out also sends,
		// so counts are >= 1, not exactly 1). Before the fix the second
		// waiter shared the first flight and its set saw zero sends.
		waitFor(t, "both sets hit", func() bool {
			return inner.SendCount("n2") >= 1 && inner.SendCount("n3") >= 1
		})
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
	m := NewManager(Config{Local: store.NewLRUStore(1<<20, nil), Self: "n1", Clock: nil, Transport: ft})
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
