package counter

import (
	"errors"
	"testing"
)

func TestStoreAdjustMergeGet(t *testing.T) {
	s := NewStore(1 << 20)
	if err := s.Increment("k", "n1", 5); err != nil {
		t.Fatal(err)
	}
	if err := s.Decrement("k", "n2", 2); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Value("k"); !ok || v != 3 {
		t.Fatalf("value = %d %v, want 3", v, ok)
	}
	if _, ok := s.Value("missing"); ok {
		t.Fatal("missing key reads present")
	}
	// Merge from a peer converges.
	var remote PNCounter
	_ = remote.Increment("n3", 7)
	if err := s.Merge("k", remote); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Value("k"); v != 10 {
		t.Fatalf("value = %d, want 10", v)
	}
	// Idempotent remerge.
	if err := s.Merge("k", remote); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Value("k"); v != 10 {
		t.Fatalf("remerge changed value: %d", v)
	}
}

func TestStoreZeroDeltaNoCreate(t *testing.T) {
	s := NewStore(1 << 20)
	if err := s.Increment("k", "n", 0); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 0 {
		t.Fatal("zero delta created a key")
	}
	if err := s.Merge("k", PNCounter{}); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 0 {
		t.Fatal("empty merge created a key")
	}
}

func TestStoreRemoveLocalOnly(t *testing.T) {
	s := NewStore(1 << 20)
	_ = s.Increment("k", "n1", 5)
	if !s.Remove("k") {
		t.Fatal("remove failed")
	}
	if s.Remove("k") {
		t.Fatal("second remove reported removal")
	}
	if _, ok := s.Value("k"); ok {
		t.Fatal("removed key still present")
	}
	// A merge from a peer that still holds the key resurrects it; counter
	// state has no replicated tombstone.
	var remote PNCounter
	_ = remote.Increment("n1", 5)
	if err := s.Merge("k", remote); err != nil {
		t.Fatal(err)
	}
	if v, ok := s.Value("k"); !ok || v != 5 {
		t.Fatalf("resurrect = %d %v, want 5", v, ok)
	}
}

func TestStoreCapacityNoEvict(t *testing.T) {
	// Tiny budget: one small counter fits, growth past the shard slice
	// fails with ErrCapacityExceeded instead of evicting a neighbor.
	// ("a"/"n" bills 1+1+16 = 18 bytes; the 64-byte store holds it but
	// not the 51-byte neighbor below.)
	s := NewStore(64)
	_ = s.Increment("a", "n", 1)
	before := s.SizeBytes()
	if err := s.Increment("b", "other-actor-with-long-id-0123456789", 1); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("err = %v, want ErrCapacityExceeded", err)
	}
	// Existing state untouched; the neighbor was not evicted.
	if _, ok := s.Value("a"); !ok {
		t.Fatal("neighbor evicted")
	}
	if got := s.SizeBytes(); got != before {
		t.Fatal("failed write mutated accounting")
	}
	// Merge growth also fails closed without partial apply.
	var big PNCounter
	_ = big.Increment("another-long-actor-id-0123456789", 9)
	if err := s.Merge("a", big); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("merge err = %v, want ErrCapacityExceeded", err)
	}
	if v, _ := s.Value("a"); v != 1 {
		t.Fatalf("failed merge partially applied: %d", v)
	}
}

func TestStoreSetCapacityNeverEvicts(t *testing.T) {
	s := NewStore(1 << 20)
	_ = s.Increment("a", "n", 5)
	s.SetCapacity(1) // shrink below use: state stays, growth stops.
	if v, ok := s.Value("a"); !ok || v != 5 {
		t.Fatalf("shrink evicted: %d %v", v, ok)
	}
	if err := s.Increment("b", "n", 1); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("err = %v, want ErrCapacityExceeded", err)
	}
}

func TestStoreLiveCountersSnapshot(t *testing.T) {
	s := NewStore(1 << 20)
	_ = s.Increment("b", "n", 2)
	_ = s.Increment("a", "n", 1)
	sorted := s.SortedLiveCounters()
	if len(sorted) != 2 || sorted[0].Key != "a" || sorted[1].Key != "b" {
		t.Fatalf("sorted = %v", sorted)
	}
}

func TestStoreMergeFailureLeavesResidentUntouched(t *testing.T) {
	// Regression: struct copies share maps, so Merge must clone before
	// merging — otherwise a failed (over-budget) merge partially applies
	// and growth computes as zero.
	s := NewStore(64)
	_ = s.Increment("a", "n", 1)
	var big PNCounter
	_ = big.Increment("another-long-actor-id-0123456789", 9)
	if err := s.Merge("a", big); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("err = %v, want ErrCapacityExceeded", err)
	}
	got, _ := s.Get("a")
	var want PNCounter
	_ = want.Increment("n", 1)
	if !got.Equal(want) {
		t.Fatalf("resident mutated by failed merge: %+v", got)
	}
	// Callers cannot mutate store state through a Get result either.
	got.Pos.Increment("evil", 100)
	if v, _ := s.Value("a"); v != 1 {
		t.Fatalf("Get aliasing mutated store: %d", v)
	}
}
