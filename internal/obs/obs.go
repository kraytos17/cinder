// Package obs holds the two tiny generics behind the process-wide
// observability wiring every subsystem repeats: an atomically-swapped
// optional pointer (metrics collector, logger) with nil-checked
// application, and the compare-and-swap retry for monotonic maxima
// (Lamport clocks, byte peaks).
package obs

import (
	"cmp"
	"sync/atomic"
)

// IfPresent applies fn to the currently installed value, and does nothing
// when none is installed. It replaces the
//
//	if m := x.met.Load(); m != nil { ... }
//
// guard repeated across store/cluster/repl/shard/ae: one pointer load on
// the hot path either way, and non-capturing closures allocate nothing.
func IfPresent[T any](p *atomic.Pointer[T], fn func(*T)) {
	if v := p.Load(); v != nil {
		fn(v)
	}
}

// RetryMax stores v when it exceeds the loaded value, retrying races lost
// to concurrent writers. Replaces the hand-rolled peak/Lamport-max loops:
// an atomic's CompareAndSwap method value fits cas directly
// (e.g. s.nextVer.CompareAndSwap for T = uint64).
func RetryMax[T cmp.Ordered](load func() T, cas func(cur, want T) bool, v T) {
	for {
		cur := load()
		if v <= cur || cas(cur, v) {
			return
		}
	}
}
