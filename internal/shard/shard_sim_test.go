package shard

import (
	"testing"
	"time"
)

func TestRebalanceIdempotent(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, time.Minute)
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(Config{Store: st, Ring: ring, Transport: ft, Membership: table, Self: "a", ReplicaFactor: 2, Quarantine: time.Minute, Clock: nil})
	// Quarantine is fresh (joined now), so every round defers.
	for i := range 3 {
		if deferred := m.Rebalance(); !deferred {
			t.Fatalf("round %d: expected deferral", i)
		}
	}
	if n := ft.Sends["b"]; n != 0 {
		t.Fatalf("deferred rounds sent %d, want 0", n)
	}
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("deferred round dropped the local copy")
	}
}

// Once converged, further rounds stay clean: the same outcome every
// round, no flapping between deferred and clean. (Copies are re-pushed
// every round by design — idempotent applies — so sends are not
// asserted, only the outcome.)
func TestRebalanceStableAfterConvergence(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(Config{Store: st, Ring: ring, Transport: ft, Membership: table, Self: "a", ReplicaFactor: 2, Quarantine: 0, Clock: nil})
	for i := range 3 {
		if deferred := m.Rebalance(); deferred {
			t.Fatalf("round %d: converged store went deferred", i)
		}
	}
	if _, ok := st.GetVersioned("k"); !ok {
		t.Fatal("converged rounds dropped the local copy")
	}
	if got, ok := ft.Stores["b"].GetVersioned("k"); !ok || string(got.Value) != "v" {
		t.Fatalf("replica missing: %+v", got)
	}
}
