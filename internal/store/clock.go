package store

import (
	"sync"
	"time"
)

// Clock supplies wall-clock time to stores. Tests can provide a ManualClock
// to advance time deterministically.
type Clock interface {
	Now() time.Time
}

// SystemClock is the production clock.
type SystemClock struct{}

// Now returns the current wall-clock time.
func (SystemClock) Now() time.Time { return time.Now() }

// ManualClock is a test clock advanced explicitly by the test.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a clock starting at t.
func NewManualClock(t time.Time) *ManualClock { return &ManualClock{t: t} }

// Now returns the manual time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the manual time forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// Set jumps the manual time to t.
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}
