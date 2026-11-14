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

// bucketOf edge table: 0/1 land in bucket 0, powers of two open their
// bucket, values just below a power stay in the previous one. This pins
// the branchless v|1 fold against the old guarded behavior.
func TestBucketOfEdges(t *testing.T) {
	cases := []struct {
		v    uint64
		want uint
	}{
		{0, 0},
		{1, 0},
		{2, 1},
		{3, 1},
		{4, 2},
		{5, 2},
		{7, 2},
		{8, 3},
		{1023, 9},
		{1024, 10},
		{1 << 63, 63},
		{1<<64 - 1, 63},
	}
	for _, tc := range cases {
		if got := bucketOf(tc.v); got != tc.want {
			t.Fatalf("bucketOf(%d) = %d, want %d", tc.v, got, tc.want)
		}
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
