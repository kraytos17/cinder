package counter

import "testing"

// The steady-state telemetry path (repeat increments on one key from one
// actor) must stay allocation-free.
func TestAllocsHotActorZero(t *testing.T) {
	s := NewStore(1 << 26)
	if err := s.Increment("hot", "n1", 1); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		_ = s.Increment("hot", "n1", 1)
	}); n != 0 {
		t.Fatalf("hot actor allocs = %v, want 0", n)
	}
}
