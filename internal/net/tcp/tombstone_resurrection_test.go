package tcp_test

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

func TestAntiEntropyRejectsReplicaPastTombstoneGrace(t *testing.T) {
	clock := store.NewManualClock(time.Now())
	st := store.NewLRUStore(1<<20, clock)
	st.SetTombstoneGCInterval(time.Hour)
	if err := st.RemoveVersioned("k", 2, 1); err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{
		NodeID: "node-a", Port: netport.Port(7001), Store: st,
		Clock: clock, ClockNow: clock.Now,
	})
	if err := srv.ConfigureCluster([]tcp.Peer{{ID: "node-b", Host: "127.0.0.1", Port: netport.Port(7002)}}); err != nil {
		t.Fatal(err)
	}
	srv.ConfigureAntiEntropy(16, 1<<20)

	// The tombstone expires while the old replica is offline.
	srv.Table().MarkAlive("node-b", 1)
	srv.Table().MarkDead("node-b")
	clock.Advance(2 * time.Hour)
	st.EvictExpired()
	if _, ok := st.GetVersioned("k"); ok {
		t.Fatal("tombstone should have expired before stale replica returns")
	}
	stale := store.Entry{Value: []byte("old-value"), Version: 1, WriterNodeHash: 9}
	syncBytes, err := (ae.SyncMessage{Entries: []ae.SyncItem{{Key: "k", Entry: stale}}}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	res := srv.Dispatch(protocol.Request{
		Opcode: protocol.OpAntiEntropySync, Value: syncBytes,
		AuthenticatedNode: "node-b",
	})
	if res.Status != protocol.StatusNotReady {
		t.Fatalf("stale peer AE status=%v, want NotReady", res.Status)
	}
	if got := srv.Metrics().TombstoneResurrectionBlocked.Load(); got != 1 {
		t.Fatalf("blocked metric=%d, want 1", got)
	}
	if e, ok := st.GetVersioned("k"); ok {
		t.Fatalf("stale value was applied: %+v", e)
	}

	// A newly rejoined peer is inside the retention window and its AE sync
	// is accepted normally.
	clock.Set(time.Now())
	srv.Table().ApplyRumor(cluster.NodeInfo{ID: "node-b", State: cluster.Alive, Incarnation: 2})
	clock.Advance(30 * time.Minute)
	res = srv.Dispatch(protocol.Request{
		Opcode: protocol.OpAntiEntropySync, Value: syncBytes,
		AuthenticatedNode: "node-b",
	})
	if res.Status != protocol.StatusOK {
		t.Fatalf("within-grace peer AE status=%v, want OK", res.Status)
	}
}

func TestTombstoneResurrectionReproducedAfterGC(t *testing.T) {
	clock := store.NewManualClock(time.Unix(100, 0))
	staleReplica := store.NewLRUStore(1<<20, clock)
	primary := store.NewLRUStore(1<<20, clock)
	staleReplica.SetTombstoneGCInterval(time.Second)
	primary.SetTombstoneGCInterval(time.Second)
	if err := staleReplica.PutVersioned("k", store.Entry{Value: []byte("old"), Version: 1, WriterNodeHash: 9}); err != nil {
		t.Fatal(err)
	}
	if err := primary.RemoveVersioned("k", 2, 1); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if n := primary.EvictExpired(); n != 1 {
		t.Fatalf("expired tombstones=%d, want 1", n)
	}
	if err := primary.PutVersioned("k", staleReplicaEntry(t, staleReplica, "k")); err != nil {
		t.Fatal(err)
	}
	got, ok := primary.Get("k")
	if !ok || string(got) != "old" {
		t.Fatalf("resurrection reproduction failed: value=%q present=%v", got, ok)
	}
}

func TestTombstoneGraceDefaultIsTwentyFourHours(t *testing.T) {
	st := store.NewLRUStore(1<<20, nil)
	if got := st.TombstoneGCInterval(); got != 24*time.Hour {
		t.Fatalf("default tombstone GC=%v, want 24h", got)
	}
}

func staleReplicaEntry(t *testing.T, st *store.Store, key string) store.Entry {
	t.Helper()
	e, ok := st.GetVersioned(key)
	if !ok {
		t.Fatalf("stale replica missing %q", key)
	}
	return e
}
