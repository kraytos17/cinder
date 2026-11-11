package repl

import (
	"errors"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/testkit"
)

// simStores builds a n1-local/n2-peer pair on one manual clock so TTL
// assertions are deterministic.
func simStores() (*store.Store, *store.Store, *store.ManualClock) {
	mc := store.NewManualClock(time.Now())
	local := store.NewLRUStore(1<<20, mc)
	peer := store.NewLRUStore(1<<20, mc)
	return local, peer, mc
}

func simManager(local *store.Store, mc *store.ManualClock, tr Transport) *Manager {
	return NewManager(Config{Local: local, Self: "n1", Clock: mc, Transport: tr})
}

// testkitStoresWith wraps one peer store so fault tests can share a
// manual clock between local and peer.
func testkitStoresWith(peer *store.Store) *testkit.Stores {
	ft := testkit.NewStores("n2")
	ft.Stores["n2"] = peer
	return ft
}

// putVer applies an explicit versioned entry (test helper).
func putVer(t *testing.T, st *store.Store, key, val string, ver, writer uint64) {
	t.Helper()
	if err := st.PutVersioned(key, store.Entry{Value: []byte(val), Version: ver, WriterNodeHash: writer}); err != nil {
		t.Fatal(err)
	}
}

// putExp applies an explicit versioned entry with an absolute expiry.
func putExp(t *testing.T, st *store.Store, key, val string, ver, writer uint64, exp time.Time) {
	t.Helper()
	e := store.Entry{Value: []byte(val), Version: ver, WriterNodeHash: writer}
	e.Expiry = expiry.At(exp)
	if err := st.PutVersioned(key, e); err != nil {
		t.Fatal(err)
	}
}

// Out-of-order redelivery must still converge on the highest version:
// the version rule, not arrival order, decides.
func TestSimOutOfOrderDeliveryHighestVersionWins(t *testing.T) {
	local, peer, mc := simStores()
	ft := testkit.NewFaultTransport(testkitStoresWith(peer), 11)
	ft.SetDelay(10 * time.Millisecond)
	ft.SetReorder(true)
	m := simManager(local, mc, ft)
	if err := m.Write("k", []byte("old"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Write("k", []byte("new"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both fan-outs queued", func() bool { return ft.Pending() == 2 })
	ft.Advance(10 * time.Millisecond)
	ft.Deliver()
	waitFor(t, "reordered converge", func() bool {
		e, ok := peer.GetVersioned("k")
		return ok && string(e.Value) == "new"
	})
	if ft.Delivered() != 2 {
		t.Fatalf("delivered = %d, want 2", ft.Delivered())
	}
}

// TTL rides the write to the replica and expires on both sides.
func TestSimTtlPropagatesToReplica(t *testing.T) {
	local, peer, mc := simStores()
	ft := testkit.NewFaultTransport(testkitStoresWith(peer), 12)
	m := simManager(local, mc, ft)
	if err := m.Write("k", []byte("v"), 100*time.Millisecond, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "fan-out queued", func() bool { return ft.Pending() == 1 })
	ft.Deliver()
	if _, ok := peer.Get("k"); !ok {
		t.Fatal("replica missing before TTL")
	}
	mc.Advance(200 * time.Millisecond)
	if _, ok := local.Get("k"); ok {
		t.Fatal("local survived its TTL")
	}
	if _, ok := peer.Get("k"); ok {
		t.Fatal("replica survived its TTL")
	}
}

// The wire carries an absolute expiry, so a delayed delivery still
// expires at the same instant on both sides.
func TestSimTtlAbsoluteExpirySharedAcrossReplicas(t *testing.T) {
	local, peer, mc := simStores()
	ft := testkit.NewFaultTransport(testkitStoresWith(peer), 13)
	ft.SetDelay(200 * time.Millisecond)
	m := simManager(local, mc, ft)
	if err := m.Write("k", []byte("v"), 500*time.Millisecond, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "fan-out queued", func() bool { return ft.Pending() == 1 })
	mc.Advance(200 * time.Millisecond) // the delay passes for everyone
	ft.Advance(200 * time.Millisecond)
	ft.Deliver()
	le, ok1 := local.GetVersioned("k")
	re, ok2 := peer.GetVersioned("k")
	if !ok1 || !ok2 {
		t.Fatal("entry missing after delayed delivery")
	}
	if le.Expiry.UnixMilli() != re.Expiry.UnixMilli() {
		t.Fatalf("expiry split: local %d vs replica %d", le.Expiry.UnixMilli(), re.Expiry.UnixMilli())
	}
	mc.Advance(310 * time.Millisecond)
	if _, ok := local.Get("k"); ok {
		t.Fatal("local survived past shared deadline")
	}
	if _, ok := peer.Get("k"); ok {
		t.Fatal("replica survived past shared deadline")
	}
}

// Fail-fast: two instant failures make quorum impossible (1 ack + 1
// pending < 3) while the straggler is still queued, so ErrQuorum fires
// before it — and the drain still consumes it and hints both failures.
func TestSimQuorumFailsFastOnImpossible(t *testing.T) {
	local, _, mc := simStores()
	inner := testkit.NewStores("n2", "n3", "n4")
	inner.Fail["n2"] = true
	inner.Fail["n3"] = true
	ft := testkit.NewFaultTransport(inner, 14)
	ft.SetDelayFor("n4", 200*time.Millisecond)
	m := simManager(local, mc, ft)
	c := metrics.NewCollector()
	m.SetMetrics(c)
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.Write("k", []byte("v"), 0, []string{"n2", "n3", "n4"}, Quorum, 0, 0)
	}()
	waitFor(t, "all sends queued", func() bool { return ft.Pending() == 3 })
	ft.Deliver() // instant fails land; the straggler stays queued
	if err := <-errCh; !errors.Is(err, ErrQuorum) {
		t.Fatalf("err = %v, want ErrQuorum", err)
	}
	// The decision fired while the straggler was still queued.
	if got := ft.Pending(); got != 1 {
		t.Fatalf("pending = %d, want 1 (straggler in flight at decision)", got)
	}
	ft.Advance(200 * time.Millisecond)
	ft.Deliver()
	waitFor(t, "late drain", func() bool { return m.HintCount() == 2 })
	if got := ft.Pending(); got != 0 {
		t.Fatalf("pending = %d, want drained straggler", got)
	}
	if got := c.QuorumWritesErr.Load(); got != 1 {
		t.Fatalf("quorum err = %d, want 1", got)
	}
	if got := c.QuorumLateFail.Load(); got != 0 {
		t.Fatalf("late fail = %d, want 0 (straggler succeeded)", got)
	}
	if got := c.HintsEnqueued.Load(); got != 2 {
		t.Fatalf("enqueued = %d, want 2", got)
	}
}

// A failed replay keeps the hint queued for the next round.
func TestSimHintRetriesOnFailedReplay(t *testing.T) {
	local, _, mc := simStores()
	inner := testkit.NewStores("n2")
	inner.Fail["n2"] = true
	m := simManager(local, mc, inner)
	if err := m.Write("k", []byte("v"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
	if n := m.ReplayHints(); n != 0 {
		t.Fatalf("replayed = %d, want 0 (peer still down)", n)
	}
	if got := m.HintCount(); got != 1 {
		t.Fatalf("hints = %d, want retained 1", got)
	}
	inner.Fail["n2"] = false
	if n := m.ReplayHints(); n != 1 {
		t.Fatalf("replayed = %d, want 1", n)
	}
}

// An empty replay must report zero without sticking the guard: the very
// first replay on a fresh manager precedes any real hint flow.
func TestSimEmptyReplayReleasesGuard(t *testing.T) {
	local, _, mc := simStores()
	inner := testkit.NewStores("n2")
	m := simManager(local, mc, inner)
	if n := m.ReplayHints(); n != 0 {
		t.Fatalf("empty replay = %d, want 0", n)
	}
	inner.Fail["n2"] = true
	if err := m.Write("k", []byte("v"), 0, []string{"n2"}, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "hint", func() bool { return m.HintCount() == 1 })
	inner.Fail["n2"] = false
	if n := m.ReplayHints(); n != 1 {
		t.Fatalf("replayed = %d, want 1 (guard released)", n)
	}
}

// Async with no replicas is a local-only commit.
func TestSimAsyncWriteNoReplicas(t *testing.T) {
	local, _, mc := simStores()
	m := simManager(local, mc, testkit.NewStores())
	if err := m.Write("k", []byte("v"), 0, nil, Async, 0, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := local.Get("k"); !ok {
		t.Fatal("local commit missing")
	}
}

// Quorum failure with a local winner still repairs: the value is
// served and the repair fan-out runs despite acks < R. Plain testkit
// answers immediately, so no fault pump is needed.
func TestSimQuorumFailureStillRepairsStaleReplica(t *testing.T) {
	local, peer, mc := simStores()
	putVer(t, local, "k", "new", 5, 1)
	putVer(t, peer, "k", "old", 3, 2)
	inner := testkitStoresWith(peer)
	inner.Fail["n3"] = true // R=2 unsatisfiable, winner present locally
	m := simManager(local, mc, inner)
	c := metrics.NewCollector()
	m.SetMetrics(c)
	e, err := m.Read("k", []string{"n2", "n3"}, 2, 0, 0)
	if err != nil || string(e.Value) != "new" {
		t.Fatalf("got %+v, %v", e, err)
	}
	if got := c.ReadRepairs.Load(); got != 1 {
		t.Fatalf("repairs = %d, want 1", got)
	}
}

// A NotFound vote marks repair: the missing replica is healed with the
// winner even though quorum failed.
func TestSimQuorumFailureNotFoundStillRepairs(t *testing.T) {
	local, peer, mc := simStores()
	putVer(t, local, "k", "v", 3, 1)
	inner := testkitStoresWith(peer) // n2 holds nothing -> NotFound
	m := simManager(local, mc, inner)
	c := metrics.NewCollector()
	m.SetMetrics(c)
	e, err := m.Read("k", []string{"n2"}, 2, 0, 0)
	if err != nil || string(e.Value) != "v" {
		t.Fatalf("got %+v, %v", e, err)
	}
	waitFor(t, "notfound repair", func() bool {
		re, ok := peer.GetVersioned("k")
		return ok && string(re.Value) == "v" && re.Version == 3
	})
	if got := c.ReadRepairs.Load(); got != 1 {
		t.Fatalf("repairs = %d, want 1", got)
	}
}

// Repair carries the winner's expiry: the healed replica expires with
// the original deadline, not a fresh one.
func TestSimReadRepairPreservesTtl(t *testing.T) {
	mc := store.NewManualClock(time.Now())
	local := store.NewLRUStore(1<<20, mc)
	peer := store.NewLRUStore(1<<20, mc)
	putExp(t, local, "k", "v", 5, 1, mc.Now().Add(time.Minute))
	putVer(t, peer, "k", "stale", 3, 2)
	inner := testkitStoresWith(peer)
	m := simManager(local, mc, inner)
	if _, err := m.Read("k", []string{"n2"}, 2, 0, 0); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "ttl repair", func() bool {
		re, ok := peer.GetVersioned("k")
		return ok && string(re.Value) == "v"
	})
	re, _ := peer.GetVersioned("k")
	le, _ := local.GetVersioned("k")
	if string(re.Value) != "v" {
		t.Fatalf("replica = %+v, want winner", re)
	}
	if re.Expiry.UnixMilli() != le.Expiry.UnixMilli() {
		t.Fatalf("repaired expiry %d != winner %d", re.Expiry.UnixMilli(), le.Expiry.UnixMilli())
	}
}
