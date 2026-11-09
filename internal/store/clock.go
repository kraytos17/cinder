// Package store implements Cinder's in-memory eviction stores (Go port).
//
// Behavior mirrors include/cinder/store (LRU policy, TTL wheel, LWW
// versioned writes, tombstones) case-for-case. The C++ tree is the
// authority until it is retired.
//
// GC discipline: the hot path keeps no pointer graphs. Nodes live in
// index-addressed slices linked by integer handles; keys are map strings
// and bulk bytes stay in flat []byte slabs, so the collector has almost
// nothing to scan.
package store

import (
	"sync"
	"time"
)

// Clock abstracts time so tests can run deterministically on a manual
// clock instead of racing real sleeps.
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
