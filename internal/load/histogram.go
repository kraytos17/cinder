// Package load drives a live cluster with a synthetic mixed workload
// and reports client-observed latency histograms.
package load

import "math/bits"

// Histogram is a log2-bucketed latency distribution in nanoseconds.
// Bucket i counts samples in [2^i, 2^i+1). Zero and negative durations
// land in bucket 0. It is not safe for concurrent use; merge per-worker
// histograms at the end of a run.
type Histogram struct {
	buckets [64]uint64
	n       uint64
	max     int64
}

// bucketOf returns the log2 bucket for v (0 for v < 2).
func bucketOf(v uint64) uint {
	if v < 2 {
		return 0
	}
	return uint(bits.Len64(v) - 1)
}

// Record adds one sample.
func (h *Histogram) Record(d int64) {
	if d < 0 {
		d = 0
	}

	h.buckets[bucketOf(uint64(d))]++
	h.n++
	if d > h.max {
		h.max = d
	}
}

// Count returns the number of recorded samples.
func (h *Histogram) Count() uint64 { return h.n }

// Max returns the largest recorded sample, or 0 when empty.
func (h *Histogram) Max() int64 { return h.max }

// Quantile returns the smallest bucket upper bound holding at least q of
// samples (q in [0,1]). Empty histograms return 0; q <= 0 returns the
// smallest non-empty bucket bound, q >= 1 returns Max.
func (h *Histogram) Quantile(q float64) int64 {
	if h.n == 0 {
		return 0
	}
	if q >= 1 {
		return h.max
	}

	target := uint64(q * float64(h.n))
	if target == 0 {
		target = 1
	}

	var cm uint64
	for i, c := range h.buckets {
		cm += c
		if cm >= target {
			if i == 63 {
				return h.max
			}
			return int64(uint64(1) << uint(i+1))
		}
	}
	return h.max
}

// Merge folds other into h.
func (h *Histogram) Merge(other *Histogram) {
	for i, c := range other.buckets {
		h.buckets[i] += c
	}

	h.n += other.n
	if other.max > h.max {
		h.max = other.max
	}
}
