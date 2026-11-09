package store

import (
	"testing"
	"time"
)

func tickCollect(w *Wheel) []int {
	var expired []int
	w.Tick(func(id int) { expired = append(expired, id) })
	return expired
}

func TestWheelInsertAndTick(t *testing.T) {
	w := NewWheel()
	w.Insert(1, 1)
	w.Insert(2, 2)
	if got := tickCollect(w); len(got) != 1 || got[0] != 1 {
		t.Fatalf("tick 1: %v", got)
	}
	if got := tickCollect(w); len(got) != 1 || got[0] != 2 {
		t.Fatalf("tick 2: %v", got)
	}
}

func TestWheelNoExpiryBeforeSlot(t *testing.T) {
	w := NewWheel()
	w.Insert(7, 5)
	for i := range 4 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("tick %d: %v", i, got)
		}
	}
}

func TestWheelMultipleExpiries(t *testing.T) {
	w := NewWheel()
	w.Insert(1, 1)
	w.Insert(2, 1)
	if got := tickCollect(w); len(got) != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestWheelRemoveBeforeExpiry(t *testing.T) {
	w := NewWheel()
	w.Insert(1, 1)
	w.Remove(1)
	if got := tickCollect(w); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestWheelInsertOverwritesRemovesOldSlot(t *testing.T) {
	w := NewWheel()
	w.Insert(1, 1)
	w.Insert(1, 10)
	for i := range 9 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("stale fire at tick %d: %v", i, got)
		}
	}
	if got := tickCollect(w); len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestWheelWrapAround(t *testing.T) {
	w := NewWheel()
	w.Insert(1, wheelSlots)
	for range wheelSlots {
		if got := tickCollect(w); len(got) != 0 {
			if len(got) != 1 || got[0] != 1 {
				t.Fatalf("got %v", got)
			}
			return
		}
	}
	t.Fatal("id never expired after a full rotation")
}

func TestWheelLongTtlFiresWithoutReinsert(t *testing.T) {
	w := NewWheel()
	w.Insert(1, wheelSlots+100)
	for i := range 355 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("fired early at tick %d", i)
		}
	}
	if got := tickCollect(w); len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestWheelLongTtlRemovePreventsFire(t *testing.T) {
	w := NewWheel()
	w.Insert(1, wheelSlots+200)
	w.Remove(1)
	for i := range wheelSlots + 201 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("removed id fired at tick %d", i)
		}
	}
}

func TestWheelReusedIDCannotFireStaleHeap(t *testing.T) {
	w := NewWheel()
	w.Insert(7, wheelSlots+144)
	w.Remove(7) // store erasure cancels the timer before recycling storage
	w.Insert(7, 1)
	if got := tickCollect(w); len(got) != 1 || got[0] != 7 {
		t.Fatalf("got %v", got)
	}
	for range wheelSlots + 144 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("stale fire: %v", got)
		}
	}
}

func TestWheelMixedShortAndLongTtl(t *testing.T) {
	w := NewWheel()
	w.Insert(1, 1)
	w.Insert(2, wheelSlots+50)
	if got := tickCollect(w); len(got) != 1 || got[0] != 1 {
		t.Fatalf("tick 1: %v", got)
	}
	for i := range 304 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("unexpected fire at tick %d", i+1)
		}
	}
	if got := tickCollect(w); len(got) != 1 || got[0] != 2 {
		t.Fatalf("tick 306: %v", got)
	}
}

func TestWheelLongTtlOverwriteRoutesToWheel(t *testing.T) {
	w := NewWheel()
	w.Insert(1, wheelSlots+100)
	w.Insert(1, 3)
	for range 2 {
		if got := tickCollect(w); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	}
	if got := tickCollect(w); len(got) != 1 || got[0] != 1 {
		t.Fatalf("got %v", got)
	}
}

func TestWheelTickCountMonotonic(t *testing.T) {
	w := NewWheel()
	if w.TickCount() != 0 {
		t.Fatal("expected 0")
	}
	tickCollect(w)
	tickCollect(w)
	if w.TickCount() != 2 {
		t.Fatalf("count = %d", w.TickCount())
	}
}

func TestExpiryTicksMinimumOne(t *testing.T) {
	now := time.Now()
	if expiryTicks(now, now) != 1 {
		t.Fatal("zero remaining must yield 1 tick")
	}
	if expiryTicks(now.Add(-time.Second), now) != 1 {
		t.Fatal("past expiry must yield 1 tick")
	}
	if expiryTicks(now.Add(1500*time.Millisecond), now) != 2 {
		t.Fatal("1.5s must ceil to 2 ticks")
	}
}
