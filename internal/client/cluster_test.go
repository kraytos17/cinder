package client

import (
	"math"
	"testing"
	"time"
)

func TestRetryBackoffUsesDurationAndSaturates(t *testing.T) {
	base := 5 * time.Millisecond
	wants := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for attempt, want := range wants {
		if got := retryBackoff(attempt, base); got != want {
			t.Errorf("retryBackoff(%d, %s) = %s, want %s", attempt, base, got, want)
		}
	}
	if got := retryBackoff(1, time.Duration(math.MaxInt64)); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflowing backoff = %s, want saturated duration", got)
	}
	if got := retryBackoff(3, -time.Second); got != 0 {
		t.Fatalf("negative base backoff = %s, want 0", got)
	}
}

func TestJitterBackoffKeepsDurationUnits(t *testing.T) {
	base := 10 * time.Millisecond
	for range 100 {
		got := jitterBackoff(base)
		if got < 8*time.Millisecond || got > 12*time.Millisecond {
			t.Fatalf("jittered backoff = %s, outside [8ms, 12ms]", got)
		}
	}
	if got := jitterBackoff(0); got != 0 {
		t.Fatalf("zero backoff = %s, want 0", got)
	}
}
