package metrics

import (
	"math/bits"
	"sync/atomic"
)

// Histogram is a lock-free log2-bucketed latency histogram. Bucket i counts
// samples in [2^i, 2^(i+1)) nanoseconds; the last bucket also absorbs larger
// values. Percentiles are estimates from cumulative bucket counts and report
// the selected bucket's upper bound.
type Histogram struct {
	buckets [numBuckets]atomic.Uint64
	count   atomic.Uint64
	totalNs atomic.Uint64
}

const numBuckets = 40 // 1 ns .. ~2^40 ns (~18 min)

// Record adds one sample.
func (h *Histogram) Record(ns uint64) {
	idx := 0
	if ns > 0 {
		idx = 63 - bits.LeadingZeros64(ns)
		if idx >= numBuckets {
			idx = numBuckets - 1
		}
	}

	h.buckets[idx].Add(1)
	h.count.Add(1)
	h.totalNs.Add(ns)
}

// Count returns the sample count.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// SumNs returns the total nanoseconds recorded.
func (h *Histogram) SumNs() uint64 { return h.totalNs.Load() }

// Percentile estimates the q latency in nanoseconds (the selected bucket's
// upper bound), or 0 when empty. It selects the first bucket whose cumulative
// count reaches uint64(q*count)+1; the top bucket reports MaxUint64.
func (h *Histogram) Percentile(q float64) uint64 {
	c := h.count.Load()
	if c == 0 {
		return 0
	}

	target := uint64(q*float64(c)) + 1
	var cum uint64
	for i := range numBuckets {
		cum += h.buckets[i].Load()
		if cum >= target {
			if i == numBuckets-1 {
				return ^uint64(0)
			}
			return uint64(1) << (i + 1)
		}
	}
	return ^uint64(0)
}

// UpperBoundNs is the finite upper bound of bucket i in ns.
func UpperBoundNs(i int) uint64 {
	if i == numBuckets-1 {
		return ^uint64(0)
	}
	return uint64(1) << (i + 1)
}
