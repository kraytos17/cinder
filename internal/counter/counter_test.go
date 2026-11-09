package counter

import (
	"math"
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
	if err := g.Increment("", 1); err != ErrInvalidActor {
		t.Fatalf("err = %v, want ErrInvalidActor", err)
	}
}

func TestGCounterOverflowFailsClean(t *testing.T) {
	var g GCounter
	if err := g.Increment("a", math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if err := g.Increment("a", 1); err != ErrOverflow {
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
