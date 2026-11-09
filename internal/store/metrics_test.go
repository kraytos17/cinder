package store

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/metrics"
)

// Metrics stay silent without a collector and count exactly with one.
func TestStoreMetrics(t *testing.T) {
	st := NewLRUStore(1<<20, nil)
	// No collector: all paths must not panic.
	if err := st.Put("a", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Get("a"); !ok {
		t.Fatal("missing")
	}
	m := metrics.NewCollector()
	st.SetMetrics(m)
	if err := st.Put("b", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Get("b"); !ok {
		t.Fatal("missing")
	}
	if _, ok := st.Get("nope"); ok {
		t.Fatal("phantom hit")
	}
	// Tombstone get counts as miss (present but not live).
	_ = st.RemoveVersioned("gone", st.MintVersion(), 0)
	if _, ok := st.Get("gone"); ok {
		t.Fatal("tombstone reads live")
	}
	if got := m.Writes.Load(); got != 1 {
		t.Fatalf("writes = %d, want 1", got)
	}
	if got := m.Hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if got := m.Misses.Load(); got != 2 {
		t.Fatalf("misses = %d, want 2 (absent + tombstone)", got)
	}
	// Deletes: the versioned remove plants a tombstone.
	if got := m.Deletes.Load(); got != 1 {
		t.Fatalf("deletes = %d, want 1", got)
	}
	// Hard remove retires the marker.
	if !st.Remove("gone") {
		t.Fatal("remove failed")
	}
	if got := m.Deletes.Load(); got != 2 {
		t.Fatalf("deletes = %d, want 2", got)
	}
	// Expire-on-read, not eviction.
	if err := st.PutWithTTL("ttl", []byte("v"), -time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Get("ttl"); ok {
		t.Fatal("expired reads live")
	}
	if got := m.ExpiresOnRead.Load(); got != 1 {
		t.Fatalf("expires_on_read = %d, want 1", got)
	}
	// Nil disables again (writes so far: b + ttl marker).
	st.SetMetrics(nil)
	if err := st.Put("z", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got := m.Writes.Load(); got != 2 {
		t.Fatalf("writes moved after disable: %d", got)
	}
}

func TestStoreMetricsCapacityEviction(t *testing.T) {
	st := NewLRUStore(256, nil)
	m := metrics.NewCollector()
	st.SetMetrics(m)
	for i := 0; i < 64; i++ {
		_ = st.Put(string(rune('a'+i%26))+string(rune('0'+i/26)), make([]byte, 32))
	}
	if got := m.EvictionsCap.Load(); got == 0 {
		t.Fatal("no capacity evictions recorded")
	}
}
