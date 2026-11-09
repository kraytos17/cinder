package sharding

import (
	"hash/maphash"
	"testing"
)

func TestCountForClamps(t *testing.T) {
	if got := CountFor(0); got != 1 {
		t.Fatalf("zero capacity must stay single-shard, got %d", got)
	}
	if got := CountFor(MinShardBytes); got != 1 {
		t.Fatalf("got %d", got)
	}
	if got := CountFor(uint64(MaxShards) * MinShardBytes); got != MaxShards {
		t.Fatalf("got %d", got)
	}
	if got := CountFor(1 << 40); got != MaxShards {
		t.Fatalf("huge capacity must clamp, got %d", got)
	}
	if got := CountFor(4 * MinShardBytes); got != 4 {
		t.Fatalf("got %d", got)
	}
}

func TestForKeyStaysInRangeAndStable(t *testing.T) {
	seed := maphash.MakeSeed()
	seen := map[int]bool{}
	keys := []string{"", "a", "k", "user-key-123", "counter:c", "\x00\xff"}
	for _, k := range keys {
		for _, n := range []int{1, 2, 16} {
			a, b := ForKey(k, n, seed), ForKey(k, n, seed)
			if a != b || a < 0 || a >= n {
				t.Fatalf("ForKey(%q, %d) = %d, %d", k, n, a, b)
			}
			seen[a] = true
		}
	}
	if len(seen) < 2 {
		t.Fatal("expected some spread across shards")
	}
}
