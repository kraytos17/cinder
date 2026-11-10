package expiry

import (
	"testing"
	"time"
)

func TestZeroIsNone(t *testing.T) {
	var e Expiry
	if !e.IsNone() {
		t.Fatal("zero must be none")
	}
	if e.Expired(time.Now()) {
		t.Fatal("none must never expire")
	}
	if _, ok := e.Time(); ok {
		t.Fatal("none must not yield a time")
	}
	if e.UnixMilli() != 0 {
		t.Fatal("none must encode as 0")
	}
	if got := After(0, time.Now()); !got.IsNone() {
		t.Fatal("non-positive After must be none")
	}
	if got := FromUnixMilli(0); !got.IsNone() {
		t.Fatal("zero ms must be none")
	}
}

func TestRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	e := At(now)
	if e.IsNone() {
		t.Fatal("At must set")
	}
	if !e.Expired(now) || !e.Expired(now.Add(time.Second)) {
		t.Fatal("must be expired at/past deadline")
	}
	if e.Expired(now.Add(-time.Second)) {
		t.Fatal("must be live before deadline")
	}
	got, ok := e.Time()
	if !ok || !got.Equal(now) {
		t.Fatalf("got %v %v", got, ok)
	}
	if ms := e.UnixMilli(); ms != now.UnixMilli() {
		t.Fatalf("ms = %d", ms)
	}
	if back := FromUnixMilli(e.UnixMilli()); back.UnixMilli() != e.UnixMilli() || back.IsNone() {
		t.Fatalf("ms round trip: %+v vs %+v", back, e)
	}
	if a := After(time.Minute, now); a.IsNone() {
		t.Fatal("After must set a future deadline")
	} else if at, _ := a.Time(); !at.After(now) {
		t.Fatal("After must be in the future")
	}
}

func TestAfterFuture(t *testing.T) {
	now := time.Now()
	if got := After(time.Minute, now); got.IsNone() || !got.Expired(now.Add(2*time.Minute)) {
		t.Fatal("After must expire after its duration")
	}
}
