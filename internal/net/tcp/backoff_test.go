package tcp

import (
	"testing"
	"time"
)

// replayBackoff is a pure delay function: table-test it directly. Loop
// wiring (reset on progress/empty, increment on all-failed) is covered by
// the hint-replay integration tests.
func TestReplayBackoff(t *testing.T) {
	const baseDelay, maxDelay = time.Second, 30 * time.Second
	for fails, want := range map[int]time.Duration{
		-1: baseDelay,
		0:  baseDelay,
		1:  2 * time.Second,
		2:  4 * time.Second,
		3:  8 * time.Second,
		4:  16 * time.Second,
		5:  maxDelay, // 32s capped
		10: maxDelay,
	} {
		if got := replayBackoff(baseDelay, maxDelay, fails); got != want {
			t.Errorf("replayBackoff(%v, %v, %d) = %v, want %v", baseDelay, maxDelay, fails, got, want)
		}
	}
	if got := replayBackoff(baseDelay, 0, 3); got != baseDelay {
		t.Errorf("non-positive max = %v, want base %v", got, baseDelay)
	}
	if got := replayBackoff(30*time.Second, time.Second, 0); got != time.Second {
		t.Errorf("base above max = %v, want max", got)
	}
	// Doubling past int64 range must cap, not wrap negative.
	if got := replayBackoff(time.Duration(1<<62), maxDelay, 2); got != maxDelay {
		t.Errorf("overflow = %v, want max %v", got, maxDelay)
	}
}
