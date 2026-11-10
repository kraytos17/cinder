package shard

import (
	"testing"

	"github.com/kraytos17/cinder/internal/metrics"
)

// Copies bump on stay-and-push; migrations + failures + underreplicated
// on the leave paths.
func TestShardMetricsCopies(t *testing.T) {
	st, ring, table, ft := testSetup(t, []string{"a", "b"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	c := metrics.NewCollector()
	m.SetMetrics(c)
	if deferred := m.Rebalance(); deferred {
		t.Fatal("nothing quarantined")
	}
	if got := c.RebalanceCopies.Load(); got != 1 {
		t.Fatalf("copies = %d, want 1", got)
	}
}

func TestShardMetricsMigrationOutcomes(t *testing.T) {
	// Primary down: failure, no migration.
	st, ring, table, ft := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	putVer(t, st, "k", "v", 5, 1)
	ft.Fail["b"] = true
	m := NewManager(st, ring, ft, table, "a", 2, 0, nil)
	c := metrics.NewCollector()
	m.SetMetrics(c)
	m.Rebalance()
	if got := c.RebalanceMigrations.Load(); got != 1 {
		t.Fatalf("migrations = %d, want 1", got)
	}
	if got := c.RebalanceMigFailures.Load(); got != 1 {
		t.Fatalf("failures = %d, want 1", got)
	}

	// Primary up, replica down: retained underreplicated.
	st2, ring2, table2, ft2 := testSetup(t, []string{"b", "c"}, "a", 2, 0)
	putVer(t, st2, "k", "v", 5, 1)
	ft2.Fail["c"] = true
	m2 := NewManager(st2, ring2, ft2, table2, "a", 2, 0, nil)
	c2 := metrics.NewCollector()
	m2.SetMetrics(c2)
	m2.Rebalance()
	if got := c2.RebalanceDroppedUnder.Load(); got != 1 {
		t.Fatalf("underreplicated = %d, want 1", got)
	}
	if got := c2.RebalanceMigFailures.Load(); got != 0 {
		t.Fatalf("failures = %d, want 0", got)
	}
}
