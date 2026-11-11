package counter

import (
	"fmt"
	"testing"
)

// BenchmarkCounterIncrementHotActor measures repeat increments on one key
// from one actor (the steady-state telemetry path).
func BenchmarkCounterIncrementHotActor(b *testing.B) {
	s := NewStore(1 << 26)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		_ = i
		if err := s.Increment("hot", "n1", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCounterIncrementNewKeys measures churn: every increment names
// a fresh key (insert path, canonicalization cost included). Oversized
// store: 5s profile runs insert millions of keys.
func BenchmarkCounterIncrementNewKeys(b *testing.B) {
	s := NewStore(1 << 28)
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("ctr-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := s.Increment(keys[i], "n1", 1); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCounterMergeNew measures merge-insert of replicated state.
func BenchmarkCounterMergeNew(b *testing.B) {
	s := NewStore(1 << 28)
	var base PNCounter
	if err := base.Increment("n9", 1); err != nil {
		b.Fatal(err)
	}
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("mrg-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := s.Merge(keys[i], base); err != nil {
			b.Fatal(err)
		}
	}
}
