package store

import "testing"

// Allocation guards for the store hot path: the overwrite and miss paths
// must stay allocation-free; the hit path returns a detached copy (one
// alloc). These are deterministic (unlike ns/op) so they run as unit
// tests and fail on regression.
func TestAllocsOverwriteZero(t *testing.T) {
	s := NewLRUStore(1<<20, nil)
	v := make([]byte, 64)
	if err := s.Put("hot", v); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(100, func() {
		_ = s.PutVersioned("hot", Entry{Value: v, Version: 2})
	}); n != 0 {
		t.Fatalf("overwrite allocs = %v, want 0", n)
	}
}

func TestAllocsGetMissZero(t *testing.T) {
	s := NewLRUStore(1<<20, nil)
	if n := testing.AllocsPerRun(200, func() {
		_, _ = s.Get("missing")
	}); n != 0 {
		t.Fatalf("get miss allocs = %v, want 0", n)
	}
}

func TestAllocsGetHitBounded(t *testing.T) {
	s := NewLRUStore(1<<20, nil)
	v := make([]byte, 64)
	if err := s.Put("hot", v); err != nil {
		t.Fatal(err)
	}
	if n := testing.AllocsPerRun(200, func() {
		_, _ = s.Get("hot")
	}); n > 1 {
		t.Fatalf("get hit allocs = %v, want <= 1", n)
	}
}
