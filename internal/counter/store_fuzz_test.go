package counter

import (
	"testing"
)

// actors pools short and adversarial actor ids: a shared actor exercises
// same-actor accumulation while distinct actors exercise merge max logic.
var fuzzStoreActors = []string{"n1", "n2", "n3", string([]byte{0xff, 0x00, 'x'})}

// FuzzCounterStore fuzzes CRDT algebraic properties over a small pool of
// named counters: concurrent-style increments/decrements, pairwise merges,
// and merge commutativity/idempotence spot checks. Merge is
// component-wise max, so both directions must converge to equal states.
func FuzzCounterStore(f *testing.F) {
	f.Add([]byte{0x01, 0x02, 0x03})
	f.Add([]byte{})
	for _, seed := range [][]byte{
		{0, 0, 1, 10},
		{2, 0, 1, 5},
		{1, 2, 0, 3, 7},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var pool [4]PNCounter
		actor := func(b byte) string {
			if b < 4 {
				return fuzzStoreActors[b%byte(len(fuzzStoreActors))]
			}
			// Raw fuzz byte as actor id (kept short): exercises actor
			// strings the decoder never validated, including empty.
			return string([]byte{b})
		}
		pos := 0
		next := func() byte {
			if pos >= len(data) {
				return 0
			}
			b := data[pos]
			pos++
			return b
		}
		amount := func() uint64 { return 1 + uint64(next())%100 }
		for range data {
			op := next()
			a, b := int(next())%len(pool), int(next())%len(pool)
			switch op % 5 {
			case 0, 1:
				// Increment/decrement with valid deltas must not fail.
				if err := pool[a].Increment(actor(op), amount()); err != nil {
					t.Fatalf("increment err = %v", err)
				}
			case 2:
				if err := pool[a].Decrement(actor(op), amount()); err != nil {
					t.Fatalf("decrement err = %v", err)
				}
			case 3:
				pool[a].Merge(pool[b])
			default:
				// Commutativity spot check: both merge directions must
				// converge to equal states.
				x, y := pool[a].Clone(), pool[b].Clone()
				x.Merge(pool[b])
				y.Merge(pool[a])
				if !x.Equal(y) {
					t.Fatal("merge not commutative")
				}
				// Idempotence: merging a state into itself changes nothing.
				z := pool[a].Clone()
				z.Merge(pool[a])
				if !z.Equal(pool[a]) {
					t.Fatal("merge not idempotent")
				}
			}
		}
	})
}
