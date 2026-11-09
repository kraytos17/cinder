package ae

import (
	"fmt"
	"testing"

	"github.com/kraytos17/cinder/internal/store"
)

// BenchmarkComputeDigest scans N live entries (the per-round rehash).
func BenchmarkComputeDigest(b *testing.B) {
	st := store.NewLRUStore(1<<26, nil)
	val := make([]byte, 64)
	const n = 10_000
	for i := range n {
		if err := st.PutVersioned(fmt.Sprintf("ae-key-%d", i), store.Entry{Value: val, Version: uint64(i + 1), WriterNodeHash: 1}); err != nil {
			b.Fatal(err)
		}
	}
	m := &Manager{store: st, numBuckets: 256}
	b.ReportAllocs()

	for b.Loop() {
		_ = m.ComputeDigest()
	}
}

// BenchmarkIBLTBuildPeel builds two sketches over 300 rows (20 differing
// each side) and peels the symmetric difference (the sketch refinement
// path). Cells comfortably exceed the difference, as in production where
// sketches cover single divergent buckets.
func BenchmarkIBLTBuildPeel(b *testing.B) {
	mk := func(shared []IBLTItem, base uint64) []IBLTItem {
		rows := append([]IBLTItem(nil), shared...)
		for i := range 20 {
			rows = append(rows, IBLTItem{KeyHash: base + uint64(i), Version: 1, Writer: 1})
		}
		return rows
	}
	shared := make([]IBLTItem, 280)
	for i := range shared {
		shared[i] = IBLTItem{KeyHash: uint64(i), Version: 1, Writer: 1}
	}
	a, c := mk(shared, 1000), mk(shared, 2000)
	b.ReportAllocs()

	for b.Loop() {
		sa, sb := BuildIBLT(a, 256), BuildIBLT(c, 256)
		if _, ok := DifferenceIBLT(sa, sb); !ok {
			b.Fatal("peel failed")
		}
	}
}
