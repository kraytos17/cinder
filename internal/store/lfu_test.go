package store

import (
	"errors"
	"testing"
	"time"
)

// Each entry bills 1-byte key + 63-byte value + 136-byte LFU node = 200.
const lfuEntryBytes = 200

func TestParsePolicy(t *testing.T) {
	for input, want := range map[string]Policy{"lru": LRUPolicy, "lfu": LFUPolicy} {
		got, err := ParsePolicy(input)
		if err != nil || got != want {
			t.Errorf("ParsePolicy(%q) = %v, %v; want %v", input, got, err, want)
		}
	}
	if _, err := ParsePolicy("LRU"); err == nil {
		t.Fatal("ParsePolicy accepted non-canonical name")
	}
}

func TestLFUEvictionUsesFrequency(t *testing.T) {
	s := NewLFUStore(3*lfuEntryBytes, nil) // one shard; three entries fit
	for _, key := range []string{"a", "b", "c"} {
		if err := s.Put(key, make([]byte, 63)); err != nil {
			t.Fatal(err)
		}
	}
	// a gets two accesses (freq 3); b gets one (freq 2); c stays at 1.
	for range 2 {
		if _, ok := s.Get("a"); !ok {
			t.Fatal("a missing")
		}
	}
	if _, ok := s.Get("b"); !ok {
		t.Fatal("b missing")
	}
	if err := s.Put("d", make([]byte, 63)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "d"} {
		if _, ok := s.GetVersioned(key); !ok {
			t.Fatalf("hot/resident key %q evicted", key)
		}
	}
	if _, ok := s.GetVersioned("c"); ok {
		t.Fatal("lowest-frequency key c survived")
	}
}

func TestLFUTiesUseBucketOrder(t *testing.T) {
	s := NewLFUStore(3*lfuEntryBytes, nil)
	for _, key := range []string{"a", "b", "c"} {
		if err := s.Put(key, make([]byte, 63)); err != nil {
			t.Fatal(err)
		}
	}
	// All have freq 1; bucket insertion order makes a the first victim.
	if err := s.Put("d", make([]byte, 63)); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetVersioned("a"); ok {
		t.Fatal("oldest frequency-1 entry a survived tie")
	}
	for _, key := range []string{"b", "c", "d"} {
		if _, ok := s.GetVersioned(key); !ok {
			t.Fatalf("expected %q to survive tie", key)
		}
	}
}

func TestLFUOverwriteAndVersionedReadFrequency(t *testing.T) {
	s := NewLFUStore(1<<20, nil)
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if got := s.EntryFreq("k"); got != 1 {
		t.Fatalf("new freq=%d, want 1", got)
	}
	if _, ok := s.GetVersioned("k"); !ok {
		t.Fatal("versioned read miss")
	}
	if got := s.EntryFreq("k"); got != 1 {
		t.Fatalf("GetVersioned changed frequency: %d", got)
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatal("Get miss")
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if got := s.EntryFreq("k"); got != 3 {
		t.Fatalf("after Get+overwrite freq=%d, want 3", got)
	}
}

func TestLFUTombstonesAreExemptFromCapacityEviction(t *testing.T) {
	s := NewLFUStore(3*lfuEntryBytes, nil)
	for _, key := range []string{"a", "b"} {
		if err := s.Put(key, make([]byte, 63)); err != nil {
			t.Fatal(err)
		}
	}
	// Tombstone frequency is 1. It must be skipped while searching bucket 1
	// for a live victim; a higher-frequency live key is not needed here.
	if err := s.RemoveVersioned("t", 99, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("c", make([]byte, 63)); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("d", make([]byte, 63)); err != nil {
		t.Fatal(err)
	}
	if e, ok := s.GetVersioned("t"); !ok || !e.Tombstone {
		t.Fatalf("tombstone evicted: %+v %v", e, ok)
	}
}

func TestLFUCapacityUsesLargerNodeOverhead(t *testing.T) {
	// One shard has exactly enough for a 1-byte key and 63-byte value only
	// under the LRU 120-byte node; LFU's 136-byte node must reject it.
	s := NewLFUStore(1+63+NodeOverhead, nil)
	if err := s.Put("k", make([]byte, 63)); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("LFU put err=%v, want capacity exceeded", err)
	}
	if s.Size() != 0 {
		t.Fatalf("rejected store size=%d", s.Size())
	}
}

// Every LFU node must appear exactly once in the bucket matching freq;
// freqIdx must point back to its node. Run after mixed mutations to guard
// swap-remove, expiry, removal, and eviction bookkeeping.
func assertLFUInvariants(t *testing.T, s *Store) {
	t.Helper()
	if s.policy != LFUPolicy {
		t.Fatal("expected LFU store")
	}
	for si := range s.shards {
		sh := &s.shards[si]
		sh.mu.Lock()
		seen := make(map[int]bool, len(sh.index))
		for freq, bucket := range sh.freqBuckets {
			for pos, idx := range bucket {
				if idx < 0 || idx >= len(sh.nodes) {
					sh.mu.Unlock()
					t.Fatalf("shard %d bucket %d has invalid idx %d", si, freq, idx)
				}
				n := &sh.nodes[idx]
				if n.freq != freq || n.freqIdx != pos {
					sh.mu.Unlock()
					t.Fatalf("shard %d idx %d: node=(freq %d idx %d), bucket=(freq %d pos %d)", si, idx, n.freq, n.freqIdx, freq, pos)
				}
				if seen[idx] {
					sh.mu.Unlock()
					t.Fatalf("shard %d idx %d appears in multiple buckets", si, idx)
				}
				seen[idx] = true
			}
		}
		if len(seen) != len(sh.index) {
			sh.mu.Unlock()
			t.Fatalf("shard %d: bucket nodes=%d index=%d", si, len(seen), len(sh.index))
		}
		sh.mu.Unlock()
	}
}

func TestLFUBucketBookkeepingMixedMutations(t *testing.T) {
	clock := NewManualClock(time.Unix(1_700_000_000, 0))
	s := NewLFUStore(4096, clock)
	for i := range 40 {
		key := string(rune('a' + i%10))
		if err := s.Put(key, []byte("v")); err != nil && !errors.Is(err, ErrCapacityExceeded) {
			t.Fatalf("Put(%q) err=%v", key, err)
		}
		if i%3 == 0 {
			_, _ = s.Get(key)
		}
		if i%7 == 0 {
			_ = s.RemoveVersioned(key, s.MintVersion(), 1)
		}
		if i%11 == 0 {
			clock.Advance(time.Minute)
			s.EvictExpired()
		}
		assertLFUInvariants(t, s)
	}
	for range 30 {
		_ = s.Put("capacity", make([]byte, 256))
		assertLFUInvariants(t, s)
	}
}

func TestLFURestoreFrequencyNeverDemotes(t *testing.T) {
	s := NewLFUStore(1<<20, nil)
	_ = s.Put("k", []byte("v"))
	s.RestoreFreq("k", 8)
	if got := s.EntryFreq("k"); got != 8 {
		t.Fatalf("restored freq=%d, want 8", got)
	}
	_, _ = s.Get("k") // now 9
	s.RestoreFreq("k", 4)
	if got := s.EntryFreq("k"); got != 9 {
		t.Fatalf("restore demoted freq to %d", got)
	}
	if s.EntryFreq("missing") != 0 {
		t.Fatal("absent key has frequency")
	}
}
