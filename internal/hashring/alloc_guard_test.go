package hashring

import "testing"

// Ring lookup is per-request: the owner fast path must stay
// allocation-free, the replica-set path bounded to one alloc.
func TestAllocsGetNodeZero(t *testing.T) {
	r, err := New(DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodes([]string{"n1", "n2", "n3"}); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(200, func() {
		_ = r.GetNode("some-key")
	}); n != 0 {
		t.Fatalf("GetNode allocs = %v, want 0", n)
	}
}

func TestAllocsGetNodesBounded(t *testing.T) {
	r, err := New(DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.SetNodes([]string{"n1", "n2", "n3"}); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		_, _ = r.GetNodes("some-key", 3)
	}); n > 1 {
		t.Fatalf("GetNodes allocs = %v, want <= 1", n)
	}
}
