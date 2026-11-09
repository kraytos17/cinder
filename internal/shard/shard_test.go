package shard

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// fakeTransport applies Replicate to per-node stores; named nodes fail.
type fakeTransport struct {
	mu     sync.Mutex
	fail   map[string]bool
	stores map[string]*store.Store
	sent   map[string]int
}

func newFakeTransport(nodes ...string) *fakeTransport {
	ft := &fakeTransport{fail: map[string]bool{}, stores: map[string]*store.Store{}, sent: map[string]int{}}
	for _, n := range nodes {
		ft.stores[n] = store.NewLRUStore(1<<20, nil)
	}
	return ft
}

func (f *fakeTransport) Send(node string, req protocol.Request) (protocol.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent[node]++
	if f.fail[node] {
		return protocol.Response{}, errors.New("unreachable: " + node)
	}
	st, ok := f.stores[node]
	if !ok {
		return protocol.Response{}, errors.New("unknown node: " + node)
	}
	if req.Opcode != protocol.OpReplicate {
		return protocol.Response{}, errors.New("unexpected opcode")
	}
	e := store.Entry{Value: append([]byte(nil), req.Value...), Version: req.Version, WriterNodeHash: req.WriterNodeHash, Tombstone: req.Tombstone}
	if req.HasExpiresAt {
		e.HasTTL = true
		e.ExpiresAt = req.ExpiresAt
	}
	if req.Tombstone {
		_ = st.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash)
		return protocol.Response{Status: protocol.StatusOK}, nil
	}
	if err := st.PutVersioned(string(req.Key), e); err != nil {
		return protocol.Response{Status: protocol.StatusCapacityExceeded}, nil
	}
	return protocol.Response{Status: protocol.StatusOK}, nil
}

func testSetup(t *testing.T, ids []string, self string, rf int, quarantine time.Duration) (*store.Store, *hashring.Ring, *cluster.MembershipTable, *fakeTransport) {
	t.Helper()
	st := store.NewLRUStore(1<<20, nil)
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	table := cluster.NewMembershipTable(self, time.Now)
	var seeds []cluster.NodeInfo
	for _, id := range ids {
		if id != self {
			seeds = append(seeds, cluster.NodeInfo{ID: id})
		}
	}
	table.Seed(seeds)
	ft := newFakeTransport(ids...)
	return st, ring, table, ft
}

func putVer(t *testing.T, st *store.Store, key, val string, ver, writer uint64) {
	t.Helper()
	if err := st.PutVersioned(key, store.Entry{Value: []byte(val), Version: ver, WriterNodeHash: writer}); err != nil {
		t.Fatal(err)
	}
}

// Staying members keep the key and push copies to the other set members.
func TestStayingPushesReplicas(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	if deferred := m.Rebalance(); deferred {
		t.Fatal("nothing quarantined")
	}
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("staying member dropped its copy")
	}
	got, ok := ft.stores["b"].GetVersioned("k")
	if !ok || string(got.Value) != "v" || got.Version != 5 {
		t.Fatalf("replica missing: %+v %v", got, ok)
	}
	if n := ft.sent["b"]; n != 1 {
		t.Fatalf("pushes to b = %d, want 1", n)
	}
}

// A node outside the desired set migrates everywhere, then drops local.
func TestLeavingMigratesAndRemoves(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	// Ring says {b c} but a still holds the key (pre-leave copy).
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	if deferred := m.Rebalance(); deferred {
		t.Fatal("nothing quarantined")
	}
	if _, ok := st.GetVersioned("k"); ok {
		t.Fatal("migrated copy not reclaimed")
	}
	for _, n := range []string{"b", "c"} {
		got, ok := ft.stores[n].GetVersioned("k")
		if !ok || string(got.Value) != "v" {
			t.Fatalf("%s missing migrated key: %+v %v", n, got, ok)
		}
	}
}

// Primary failure keeps the local failover copy and fires the retry hook.
func TestMigrationFailureKeepsLocal(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	ft.fail["b"] = true // primary down (c would ack)
	failed := 0
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	m.SetOnMigrationFailed(func() { failed++ })
	if deferred := m.Rebalance(); deferred {
		t.Fatal("nothing quarantined")
	}
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("failed migration dropped the failover copy")
	}
	if failed != 1 {
		t.Fatalf("retry callback fired %d times, want 1", failed)
	}
}

// Primary ack with an unconfirmed replica retains the copy for the next
// round (over-replication is safe, under-replication is not) without
// scheduling a migration retry.
func TestUnderreplicatedRetainsLocal(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	ft.fail["c"] = true // replica down, primary up
	failed := 0
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	m.SetOnMigrationFailed(func() { failed++ })
	m.Rebalance()
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("underreplicated migration dropped the copy")
	}
	if failed != 0 {
		t.Fatal("underreplicated path must not schedule a migration retry")
	}
	if _, ok := ft.stores["b"].GetVersioned("k"); !ok {
		t.Fatal("primary never got the key")
	}
}

// Quarantined targets defer: no send, local kept, deferred reported.
func TestQuarantineDefers(t *testing.T) {
	now := time.Now()
	cur := now
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, 10*time.Second)
	// b joined just now: inside the window.
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(st, ring, ft, table, "a", 2, 10*time.Second, func() time.Time { return cur })
	if deferred := m.Rebalance(); !deferred {
		t.Fatal("expected deferral for quarantined replica")
	}
	if n := ft.sent["b"]; n != 0 {
		t.Fatalf("sent %d pushes to quarantined node", n)
	}
	// Past the window the same scan sends.
	cur = now.Add(11 * time.Second)
	if deferred := m.Rebalance(); deferred {
		t.Fatal("quarantine should have cleared")
	}
	if _, ok := ft.stores["b"].GetVersioned("k"); !ok {
		t.Fatal("replica never pushed after quarantine cleared")
	}
}

// Dead targets are skipped at send time (not deferred).
func TestDeadTargetSkipped(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	table.MarkDead("b")
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	if deferred := m.Rebalance(); deferred {
		t.Fatal("dead-target skip is not a deferral")
	}
	if n := ft.sent["b"]; n != 0 {
		t.Fatalf("sent %d pushes to a dead node", n)
	}
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("local copy must stay while the replica is dead")
	}
}

// Tombstones migrate with their identity; TTLs ride along absolutely.
func TestTombstoneAndTTLMigrate(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	if err := st.PutVersioned("t", store.Entry{Value: []byte("x"), Version: 3, WriterNodeHash: 1}); err != nil {
		t.Fatal(err)
	}
	if err := st.PutVersioned("e", store.Entry{Value: []byte("y"), Version: 4, WriterNodeHash: 1, HasTTL: true, ExpiresAt: exp}); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveVersioned("t", 9, 2); err != nil {
		t.Fatal(err)
	}
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	m.Rebalance()
	tomb, ok := ft.stores["b"].GetVersioned("t")
	if !ok || !tomb.Tombstone || tomb.Version != 9 {
		t.Fatalf("tombstone lost: %+v %v", tomb, ok)
	}
	got, ok := ft.stores["c"].GetVersioned("e")
	if !ok || !got.HasTTL || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("TTL lost: %+v %v", got, ok)
	}
}

// RF=1: leaving nodes hand the single copy to the new owner.
func TestSingleCopyMigrates(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"b"}, "a", 1, 0)
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(st, ring, ft, table, "a", 1, 0, nil)
	m.Rebalance()
	if _, ok := st.GetVersioned("k"); ok {
		t.Fatal("migrated copy not reclaimed")
	}
	if got, ok := ft.stores["b"].GetVersioned("k"); !ok || string(got.Value) != "v" {
		t.Fatalf("new owner missing key: %+v %v", got, ok)
	}
}
