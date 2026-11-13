package ae

import (
	"fmt"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
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

func benchmarkAEStore(b *testing.B, count int) *store.Store {
	b.Helper()
	st := store.NewLRUStore(1<<30, nil)
	value := make([]byte, 64)
	for i := range count {
		if err := st.PutVersioned(fmt.Sprintf("ae-key-%08d", i), store.Entry{
			Value: value, Version: uint64(i + 1), WriterNodeHash: 1,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return st
}

func BenchmarkAECollectRows10K(b *testing.B) {
	st := benchmarkAEStore(b, 10_000)
	m := &Manager{store: st, numBuckets: 256}
	buckets := allBuckets(256)
	b.ReportAllocs()
	for b.Loop() {
		benchAERows = m.CollectRows(buckets)
	}
}

func BenchmarkAECollectMetadata10K(b *testing.B) {
	st := benchmarkAEStore(b, 10_000)
	m := &Manager{store: st, numBuckets: 256}
	buckets := allBuckets(256)
	b.ReportAllocs()
	for b.Loop() {
		benchAEMetadata, benchAECovered = m.CollectMetadata(buckets, MaxMetaBytes)
	}
}

// BenchmarkAECollectMetadataScale measures full-snapshot metadata collection
// across dataset size and requested-bucket selectivity.
func BenchmarkAECollectMetadataScale(b *testing.B) {
	for _, keyCount := range []int{10_000, 100_000} {
		st := benchmarkAEStore(b, keyCount)
		m := &Manager{store: st, numBuckets: 256}
		for _, bucketCount := range []int{1, 16, 256} {
			b.Run(fmt.Sprintf("keys=%d/buckets=%d", keyCount, bucketCount), func(b *testing.B) {
				buckets := make([]uint32, bucketCount)
				for i := range buckets {
					buckets[i] = uint32(i * 256 / bucketCount)
				}
				b.ReportAllocs()
				for b.Loop() {
					benchAEMetadata, benchAECovered = m.CollectMetadata(buckets, MaxMetaBytes)
				}
				b.ReportMetric(float64(len(benchAEMetadata)), "wire-B/op")
			})
		}
	}
}

func BenchmarkAEDiffRows10K(b *testing.B) {
	st := benchmarkAEStore(b, 10_000)
	m := &Manager{store: st, numBuckets: 256}
	perBucket := m.CollectRows(allBuckets(256))
	var rows []MetaRow
	for _, bucketRows := range perBucket {
		for _, row := range bucketRows {
			row.Version++ // every remote row is newer, exercising the need path
			rows = append(rows, row)
		}
	}
	buckets := allBuckets(256)
	b.ReportAllocs()
	for b.Loop() {
		benchAEDiff = m.DiffRows(rows, buckets, buckets)
	}
}

func BenchmarkAECollectEntries10K(b *testing.B) {
	st := benchmarkAEStore(b, 10_000)
	m := &Manager{store: st, numBuckets: 256, fetchBytes: 1 << 20}
	keys := make([]string, 0, 10_000)
	st.ForEach(func(key string, _ store.Entry) { keys = append(keys, key) })
	b.ReportAllocs()
	for b.Loop() {
		benchAEItems = m.CollectEntriesForKeys(keys, 1<<20)
	}
}

func BenchmarkAEEncodeCounterBatch10K(b *testing.B) {
	cs := counter.NewStore(1 << 30)
	for i := range 10_000 {
		if err := cs.Increment(fmt.Sprintf("counter-%08d", i), "node-a", uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
	m := &Manager{fetchBytes: 1 << 20}
	m.SetCounterStore(cs)
	b.ReportAllocs()
	for b.Loop() {
		benchAEBytes = m.EncodeCounterBatch(1 << 20)
	}
}

func BenchmarkAECloneAndEncodeCounterBatch10K(b *testing.B) {
	cs := counter.NewStore(1 << 30)
	for i := range 10_000 {
		if err := cs.Increment(fmt.Sprintf("counter-%08d", i), "node-a", uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for b.Loop() {
		all := cs.SortedLiveCounters()
		selected := make([]counter.EncodedBundleEntry, len(all))
		for i, entry := range all {
			selected[i] = counter.EncodedBundleEntry{Key: entry.Key, State: entry.Counter.Encode()}
		}
		benchAEBytes = counter.EncodeEncodedCounterBundle(selected)
	}
}

var (
	benchAERows     [][]MetaRow
	benchAEDiff     Diff
	benchAEItems    []SyncItem
	benchAEBytes    []byte
	benchAEMetadata []byte
	benchAECovered  []uint32
)
