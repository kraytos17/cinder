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

// BenchmarkStoreMergeActorCardinality measures merging into an existing
// resident state as actor count and overlap vary. Remote-only actors exercise
// map growth and capacity accounting; full overlap exercises max/compare and
// the idempotent no-change path. Positive and negative components are both
// populated to reflect PN state.
func BenchmarkStoreMergeActorCardinality(b *testing.B) {
	for _, actors := range []int{1, 10, 100, 1000} {
		for _, overlapPercent := range []int{0, 50, 100} {
			modes := []string{"changed"}
			if overlapPercent == 100 {
				modes = append(modes, "noop")
			}
			for _, mode := range modes {
				b.Run(fmt.Sprintf("actors=%d/overlap=%d%%/%s", actors, overlapPercent, mode), func(b *testing.B) {
					resident := makePNState(b, actors, "resident")
					remote := makeMergeRemote(b, actors, overlapPercent)
					if mode == "noop" {
						remote = resident.Clone()
					}
					// Keep the benchmark target's shard comfortably under its
					// per-shard budget at the largest actor cardinality.
					st := NewStore(1 << 30)
					if err := st.Merge("key", resident); err != nil {
						b.Fatal(err)
					}
					changedActor := "remote-0000"
					if overlapPercent > 0 {
						changedActor = "resident-0000"
					}
					if mode == "noop" {
						if err := st.Merge("key", remote); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if mode == "changed" {
							// Advance one remote component so every iteration
							// exercises a changed-state merge without timing an
							// O(actor-count) input rebuild.
							remote.Pos.components[changedActor]++
							remote.Neg.components[changedActor]++
						}
						if err := st.Merge("key", remote); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func makePNState(tb testing.TB, actors int, prefix string) PNCounter {
	tb.Helper()
	var state PNCounter
	for i := range actors {
		actor := fmt.Sprintf("%s-%04d", prefix, i)
		if err := state.Increment(actor, uint64(i+1)); err != nil {
			tb.Fatal(err)
		}
		if err := state.Decrement(actor, uint64(i+2)); err != nil {
			tb.Fatal(err)
		}
	}
	return state
}

func makeMergeRemote(tb testing.TB, actors, overlapPercent int) PNCounter {
	tb.Helper()
	var state PNCounter
	overlapCount := actors * overlapPercent / 100
	for i := range actors {
		prefix := "remote"
		if i < overlapCount {
			prefix = "resident"
		}
		actor := fmt.Sprintf("%s-%04d", prefix, i)
		if err := state.Increment(actor, uint64(actors+i+1)); err != nil {
			tb.Fatal(err)
		}
		if err := state.Decrement(actor, uint64(actors+i+2)); err != nil {
			tb.Fatal(err)
		}
	}
	return state
}
