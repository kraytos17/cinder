// Package expiry is the single time-to-live representation shared by
// the store entry, the wire request/response, the WAL/snapshot records,
// and hinted handoff. It replaces the four HasTTL/HasExpiresAt +
// time/duration/ms quadruples, whose bool and instant could desync
// (a copied instant without its flag never expires, or expires at the
// zero time).
//
// The zero value means no expiry. Absolute instants ride in UTC;
// conversions to wire milliseconds saturate at zero for none.
package expiry

import (
	"time"
)

// Expiry is an optional absolute deadline. Compare and copy by value;
// the zero value carries no deadline.
type Expiry struct {
	at time.Time
}

// None returns the no-expiry value.
func None() Expiry { return Expiry{} }

// At returns an expiry at t.
func At(t time.Time) Expiry { return Expiry{at: t} }

// After returns an expiry d after now, or None for non-positive d.
func After(d time.Duration, now time.Time) Expiry {
	if d <= 0 {
		return Expiry{}
	}
	return Expiry{at: now.Add(d)}
}

// FromUnixMilli decodes a wall-clock millisecond instant, with 0
// meaning none (the WAL/snapshot convention).
func FromUnixMilli(ms int64) Expiry {
	if ms <= 0 {
		return Expiry{}
	}
	return Expiry{at: time.UnixMilli(ms).UTC()}
}

// IsNone reports whether no deadline is set.
func (e Expiry) IsNone() bool { return e.at.IsZero() }

// Time returns the deadline and whether one is set.
func (e Expiry) Time() (time.Time, bool) {
	if e.at.IsZero() {
		return time.Time{}, false
	}
	return e.at, true
}

// Expired reports whether now is at or past the deadline. None never
// expires.
func (e Expiry) Expired(now time.Time) bool {
	return !e.at.IsZero() && !now.Before(e.at)
}

// UnixMilli encodes the deadline as wall-clock milliseconds, or 0 for
// none (the WAL/snapshot convention).
func (e Expiry) UnixMilli() int64 {
	if e.at.IsZero() {
		return 0
	}
	return e.at.UnixMilli()
}
