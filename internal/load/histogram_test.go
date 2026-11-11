package load

import (
	"testing"
)

// Bucket bounds: bucket i holds [2^i, 2^i+1). 100 samples at exactly
// 1000ns land in bucket 9 ([512,1024)); p50/p99 report the bound 1024.
func TestHistogramQuantile(t *testing.T) {
	var h Histogram
	for range 100 {
		h.Record(1000)
	}
	if h.Count() != 100 {
		t.Fatalf("count = %d", h.Count())
	}
	if h.Max() != 1000 {
		t.Fatalf("max = %d", h.Max())
	}
	for _, q := range []float64{0, 0.5, 0.99, 1} {
		if got := h.Quantile(q); got != 1024 && !(q == 1 && got == 1000) {
			t.Fatalf("q(%v) = %d", q, got)
		}
	}
}

func TestHistogramEmpty(t *testing.T) {
	var h Histogram
	if h.Count() != 0 || h.Max() != 0 || h.Quantile(0.99) != 0 {
		t.Fatal("empty histogram must report zeros")
	}
}

func TestHistogramMerge(t *testing.T) {
	var a, b Histogram
	for range 3 {
		a.Record(100)
	}
	for range 7 {
		b.Record(100000)
	}
	a.Merge(&b)
	if a.Count() != 10 || a.Max() != 100000 {
		t.Fatalf("merged = %+v", a)
	}
	// 70% of samples are slow: p50 lands in the slow bucket.
	if got := a.Quantile(0.5); got < 100000 {
		t.Fatalf("p50 = %d, want >= 100000", got)
	}
}
