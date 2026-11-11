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

// BenchmarkDigestForRound measures the incremental steady state: one
// cold rehash upfront, then per-round 2 KiB accumulator copies.
func BenchmarkDigestForRound(b *testing.B) {
	st := store.NewLRUStore(1<<26, nil)
	m := NewManager(Config{Store: st, Buckets: 256, RehashInterval: -1})
	defer m.DetachSink()
	val := make([]byte, 64)
	const n = 10_000
	for i := range n {
		if err := st.PutVersioned(fmt.Sprintf("ae-key-%d", i), store.Entry{Value: val, Version: uint64(i + 1), WriterNodeHash: 1}); err != nil {
			b.Fatal(err)
		}
	}
	if _, rehashed := m.digestForRound(); !rehashed {
		b.Fatal("cold start must rehash")
	}
	b.ReportAllocs()
	for b.Loop() {
		_, rehashed := m.digestForRound()
		if rehashed {
			b.Fatal("steady state must not rehash")
		}
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
