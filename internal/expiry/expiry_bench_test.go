package expiry

import (
	"testing"
	"time"
)

var (
	expiryBenchSink  bool
	expiryBenchValue int64
)

// BenchmarkExpiryCore measures the small value operations independently
// from store locking and wheel scheduling.
func BenchmarkExpiryCore(b *testing.B) {
	now := time.Unix(1_700_000_000, 123_456_000)
	exp := At(now.Add(time.Hour))
	deadline, _ := exp.Time()
	for _, tc := range []struct {
		name string
		fn   func()
	}{
		{"none-check", func() { expiryBenchSink = None().IsNone() }},
		{"is-none", func() { expiryBenchSink = exp.IsNone() }},
		{"expired-live", func() { expiryBenchSink = exp.Expired(now) }},
		{"expired-deadline", func() { expiryBenchSink = exp.Expired(deadline) }},
		{"unix-milli", func() { expiryBenchValue = exp.UnixMilli() }},
		{"from-unix-milli", func() { expiryBenchSink = FromUnixMilli(exp.UnixMilli()).Expired(now) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				tc.fn()
			}
		})
	}
}
