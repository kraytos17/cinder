package counter

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
)

func TestGCounterIncrementAndValue(t *testing.T) {
	var g GCounter
	if err := g.Increment("a", 3); err != nil {
		t.Fatal(err)
	}
	if err := g.Increment("b", 4); err != nil {
		t.Fatal(err)
	}
	if got := g.Value(); got != 7 {
		t.Fatalf("value = %d, want 7", got)
	}
	// Zero delta is a no-op that does not allocate.
	if err := g.Increment("c", 0); err != nil {
		t.Fatal(err)
	}
	if got := g.Component("c"); got != 0 {
		t.Fatalf("zero delta created a component: %d", got)
	}
	if len(g.Actors()) != 2 {
		t.Fatalf("actors = %v, want [a b]", g.Actors())
	}
}

func TestGCounterRejectsEmptyActor(t *testing.T) {
	var g GCounter
	if err := g.Increment("", 1); !errors.Is(err, ErrInvalidActor) {
		t.Fatalf("err = %v, want ErrInvalidActor", err)
	}
}

func TestGCounterOverflowFailsClean(t *testing.T) {
	var g GCounter
	if err := g.Increment("a", math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if err := g.Increment("a", 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("err = %v, want ErrOverflow", err)
	}
	if got := g.Component("a"); got != math.MaxUint64 {
		t.Fatalf("component mutated on overflow: %d", got)
	}
}

func TestGCounterValueSaturates(t *testing.T) {
	var g GCounter
	if err := g.Increment("a", math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if err := g.Increment("b", math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if got := g.Value(); got != math.MaxUint64 {
		t.Fatalf("value = %d, want saturation", got)
	}
}

func TestGCounterMergeTakesMax(t *testing.T) {
	var a, b GCounter
	_ = a.Increment("x", 5)
	_ = a.Increment("y", 1)
	_ = b.Increment("x", 3)
	_ = b.Increment("z", 9)
	a.Merge(b)
	if got := a.Component("x"); got != 5 {
		t.Fatalf("x = %d, want max 5", got)
	}
	if got := a.Component("z"); got != 9 {
		t.Fatalf("z = %d, want 9", got)
	}
	// Idempotent: merging again changes nothing.
	before := a
	a.Merge(b)
	if !a.Equal(before) {
		t.Fatal("remerge changed state")
	}
}

func TestPNCounterValueAndMerge(t *testing.T) {
	var a, b PNCounter
	_ = a.Increment("n1", 10)
	_ = a.Decrement("n1", 3)
	_ = b.Increment("n2", 5)
	_ = b.Decrement("n2", 8)
	a.Merge(b)
	// P: 10+5=15, N: 3+8=11, value 4.
	if got := a.Value(); got != 4 {
		t.Fatalf("value = %d, want 4", got)
	}
	// Negative value.
	var c PNCounter
	_ = c.Decrement("n", 7)
	if got := c.Value(); got != -7 {
		t.Fatalf("value = %d, want -7", got)
	}
}

func TestPNCounterConcurrentIncrementsSurvive(t *testing.T) {
	// The raison d'etre: two replicas incrementing independently merge
	// without loss, where LWW would drop one.
	var a, b PNCounter
	_ = a.Increment("n1", 5)
	_ = b.Increment("n2", 5)
	a.Merge(b)
	b.Merge(a)
	if !a.Equal(b) {
		t.Fatal("replicas diverged after mutual merge")
	}
	if got := a.Value(); got != 10 {
		t.Fatalf("value = %d, want 10", got)
	}
}

func TestPNCounterClamps(t *testing.T) {
	var p PNCounter
	_ = p.Pos.Increment("a", math.MaxUint64)
	if got := p.Value(); got != math.MaxInt64 {
		t.Fatalf("value = %d, want MaxInt64", got)
	}
	var n PNCounter
	_ = n.Neg.Increment("a", math.MaxUint64)
	if got := n.Value(); got != math.MinInt64 {
		t.Fatalf("value = %d, want MinInt64", got)
	}
}

func TestPNCounterValueSubtractsBeforeClamping(t *testing.T) {
	tests := []struct {
		name string
		pos  []uint64
		neg  []uint64
		want int64
	}{
		{
			name: "positive totals exceed uint64 but difference fits",
			pos:  []uint64{math.MaxUint64, math.MaxUint64},
			neg:  []uint64{math.MaxUint64},
			want: math.MaxInt64,
		},
		{
			name: "negative totals exceed uint64 but difference fits",
			pos:  []uint64{math.MaxUint64},
			neg:  []uint64{math.MaxUint64, math.MaxUint64},
			want: math.MinInt64,
		},
		{
			name: "positive cancellation",
			pos:  []uint64{math.MaxUint64, math.MaxUint64, 17},
			neg:  []uint64{math.MaxUint64, math.MaxUint64, 10},
			want: 7,
		},
		{
			name: "negative cancellation",
			pos:  []uint64{math.MaxUint64, 10},
			neg:  []uint64{math.MaxUint64, 17},
			want: -7,
		},
		{
			name: "positive clamp after cancellation",
			pos:  []uint64{math.MaxUint64, math.MaxUint64},
			neg:  []uint64{math.MaxUint64, 1},
			want: math.MaxInt64,
		},
		{
			name: "negative clamp after cancellation",
			pos:  []uint64{math.MaxUint64, 1},
			neg:  []uint64{math.MaxUint64, math.MaxUint64},
			want: math.MinInt64,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p PNCounter
			p.Pos.components = make(map[string]uint64, len(tt.pos))
			p.Neg.components = make(map[string]uint64, len(tt.neg))
			for i, value := range tt.pos {
				p.Pos.components[string(rune('a'+i))] = value
			}
			for i, value := range tt.neg {
				p.Neg.components[string(rune('a'+i))] = value
			}
			if got := p.Value(); got != tt.want {
				t.Fatalf("Value() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPNCounterValueMatchesBigIntOracle(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x43524454, 0x504e434e))
	for trial := range 1000 {
		p := PNCounter{
			Pos: GCounter{components: make(map[string]uint64)},
			Neg: GCounter{components: make(map[string]uint64)},
		}
		for actor := range 1 + rng.IntN(16) {
			p.Pos.components[string(rune('a'+actor))] = rng.Uint64()
			p.Neg.components[string(rune('a'+actor))] = rng.Uint64()
		}
		if got, want := p.Value(), pnValueOracle(p); got != want {
			t.Fatalf("trial %d: Value() = %d, oracle = %d; pos=%v neg=%v", trial, got, want, p.Pos.components, p.Neg.components)
		}
	}
}

func TestPNCounterMergeLaws(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x4d455247, 0x43524454))
	randomCounter := func() PNCounter {
		p := PNCounter{
			Pos: GCounter{components: make(map[string]uint64)},
			Neg: GCounter{components: make(map[string]uint64)},
		}
		for actor := range 1 + rng.IntN(12) {
			id := fmt.Sprintf("node-%02d", actor)
			if rng.IntN(2) == 0 {
				if err := p.Increment(id, uint64(rng.IntN(1000)+1)); err != nil {
					t.Fatal(err)
				}
			}
			if rng.IntN(2) == 0 {
				if err := p.Decrement(id, uint64(rng.IntN(1000)+1)); err != nil {
					t.Fatal(err)
				}
			}
		}
		return p
	}

	for trial := range 1000 {
		a, b, c := randomCounter(), randomCounter(), randomCounter()

		ab := a.Clone()
		ab.Merge(b)
		ba := b.Clone()
		ba.Merge(a)
		if !ab.Equal(ba) {
			t.Fatalf("trial %d: merge is not commutative", trial)
		}

		idempotent := a.Clone()
		idempotent.Merge(a)
		if !idempotent.Equal(a) {
			t.Fatalf("trial %d: merge is not idempotent", trial)
		}

		left := a.Clone()
		left.Merge(b)
		left.Merge(c)
		bc := b.Clone()
		bc.Merge(c)
		right := a.Clone()
		right.Merge(bc)
		if !left.Equal(right) {
			t.Fatalf("trial %d: merge is not associative", trial)
		}

		assertCounterDominates(t, trial, "positive", a.Pos, left.Pos)
		assertCounterDominates(t, trial, "negative", a.Neg, left.Neg)
		assertCounterDominates(t, trial, "positive", b.Pos, left.Pos)
		assertCounterDominates(t, trial, "negative", b.Neg, left.Neg)
		assertCounterDominates(t, trial, "positive", c.Pos, left.Pos)
		assertCounterDominates(t, trial, "negative", c.Neg, left.Neg)
	}
}

func assertCounterDominates(t *testing.T, trial int, side string, before, after GCounter) {
	t.Helper()
	for actor, count := range before.components {
		if got := after.Component(actor); got < count {
			t.Fatalf("trial %d: %s component %q decreased from %d to %d", trial, side, actor, count, got)
		}
	}
}

func pnValueOracle(p PNCounter) int64 {
	pos, neg := new(big.Int), new(big.Int)
	for _, component := range p.Pos.components {
		pos.Add(pos, new(big.Int).SetUint64(component))
	}
	for _, component := range p.Neg.components {
		neg.Add(neg, new(big.Int).SetUint64(component))
	}
	diff := pos.Sub(pos, neg)
	maxValue, minValue := big.NewInt(math.MaxInt64), big.NewInt(math.MinInt64)
	if diff.Cmp(maxValue) > 0 {
		return math.MaxInt64
	}
	if diff.Cmp(minValue) < 0 {
		return math.MinInt64
	}
	return diff.Int64()
}
