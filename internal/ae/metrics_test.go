package ae

import (
	"testing"

	"github.com/kraytos17/cinder/internal/metrics"
)

// Counter repair rounds bump their dedicated counters; a converged LWW
// round bumps rounds/rehashes without mismatches or repairs.
func TestAEMetricsCounterRound(t *testing.T) {
	m1, _, cs1, cs2 := counterTestPair(t, 1<<20)
	c := metrics.NewCollector()
	m1.SetMetrics(c)
	_ = cs1.Increment("k", "n1", 5)
	_ = cs2.Increment("k", "n2", 7)
	m1.RunCounterRound(2)
	if got := c.CounterAERounds.Load(); got != 1 {
		t.Fatalf("counter rounds = %d, want 1", got)
	}
	if got := c.CounterAERepaired.Load(); got == 0 {
		t.Fatal("no counter repairs recorded")
	}
}

// Empty counter store skips the round entirely (no partner traffic).
func TestAEMetricsCounterEmptySkips(t *testing.T) {
	m1, _, _, _ := counterTestPair(t, 1<<20) //nolint:dogsled // helper returns a fixed 4-tuple
	c := metrics.NewCollector()
	m1.SetMetrics(c)
	m1.RunCounterRound(2)
	if got := c.CounterAERounds.Load(); got != 0 {
		t.Fatalf("rounds = %d, want 0", got)
	}
}
