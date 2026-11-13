package ae

import (
	"cmp"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

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

type benchIndexedRow struct {
	key string
	row MetaRow
}

// bucketMapIndex prototypes one string-keyed map per AE bucket.
type bucketMapIndex struct {
	buckets [256]map[string]MetaRow
	loc     map[string]uint32
}

func buildBucketMapIndex(b *testing.B, st *store.Store) *bucketMapIndex {
	b.Helper()
	idx := &bucketMapIndex{loc: make(map[string]uint32)}
	for _, item := range st.MetadataSnapshot() {
		e := item.Entry
		bucket := BucketFor(item.Key, 256)
		if idx.buckets[bucket] == nil {
			idx.buckets[bucket] = make(map[string]MetaRow)
		}
		idx.buckets[bucket][item.Key] = MetaRow{Bucket: bucket, KeyHash: KeyHash(item.Key), Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone}
		idx.loc[item.Key] = bucket
	}
	return idx
}

func (idx *bucketMapIndex) collect(buckets []uint32, maxBytes int) ([]byte, []uint32) {
	var out []MetaRow
	covered := make([]uint32, 0, len(buckets))
	used := 8
	for _, bucket := range buckets {
		rows := make([]MetaRow, 0, len(idx.buckets[bucket]))
		for _, row := range idx.buckets[bucket] {
			rows = append(rows, row)
		}
		slices.SortFunc(rows, func(a, b MetaRow) int { return cmp.Compare(a.KeyHash, b.KeyHash) })
		need := 4 + len(rows)*metaRowBytes
		if used+need > maxBytes && len(covered) > 0 {
			break
		}
		covered = append(covered, bucket)
		out = append(out, rows...)
		used += need
		if used > maxBytes {
			break
		}
	}
	return EncodeMetaRows(out), covered
}

func (idx *bucketMapIndex) upsert(key string, row MetaRow) {
	bucket := row.Bucket
	if oldBucket, ok := idx.loc[key]; ok {
		if oldBucket == bucket {
			idx.buckets[bucket][key] = row
			return
		}
		delete(idx.buckets[oldBucket], key)
	}
	if idx.buckets[bucket] == nil {
		idx.buckets[bucket] = make(map[string]MetaRow)
	}
	idx.buckets[bucket][key] = row
	idx.loc[key] = bucket
}

func (idx *bucketMapIndex) remove(key string) {
	if bucket, ok := idx.loc[key]; ok {
		delete(idx.buckets[bucket], key)
		delete(idx.loc, key)
	}
}

// sortedSliceIndex prototypes sorted bucket vectors with a key->(bucket,idx)
// location map for overwrite and removal updates.
type sortedSliceIndex struct {
	buckets [256][]benchIndexedRow
	loc     map[string]benchRowLocation
}

type benchRowLocation struct {
	bucket uint32
	index  int
}

func buildSortedSliceIndex(b *testing.B, st *store.Store) *sortedSliceIndex {
	b.Helper()
	idx := &sortedSliceIndex{loc: make(map[string]benchRowLocation)}
	for _, item := range st.MetadataSnapshot() {
		e := item.Entry
		bucket := BucketFor(item.Key, 256)
		idx.buckets[bucket] = append(idx.buckets[bucket], benchIndexedRow{key: item.Key, row: MetaRow{
			Bucket: bucket, KeyHash: KeyHash(item.Key), Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone,
		}})
	}
	for bucket := range idx.buckets {
		slices.SortFunc(idx.buckets[bucket], compareBenchIndexedRow)
		for i, item := range idx.buckets[bucket] {
			idx.loc[item.key] = benchRowLocation{bucket: uint32(bucket), index: i}
		}
	}
	return idx
}

func compareBenchIndexedRow(a, b benchIndexedRow) int {
	return cmp.Or(cmp.Compare(a.row.KeyHash, b.row.KeyHash), cmp.Compare(a.key, b.key))
}

func (idx *sortedSliceIndex) collect(buckets []uint32, maxBytes int) ([]byte, []uint32) {
	var out []MetaRow
	covered := make([]uint32, 0, len(buckets))
	used := 8
	for _, bucket := range buckets {
		rows := idx.buckets[bucket]
		need := 4 + len(rows)*metaRowBytes
		if used+need > maxBytes && len(covered) > 0 {
			break
		}
		covered = append(covered, bucket)
		for _, item := range rows {
			out = append(out, item.row)
		}
		used += need
		if used > maxBytes {
			break
		}
	}
	return EncodeMetaRows(out), covered
}

func (idx *sortedSliceIndex) upsert(key string, row MetaRow) {
	bucket := row.Bucket
	rows := idx.buckets[bucket]
	if old, ok := idx.loc[key]; ok {
		if old.bucket == bucket {
			rows[old.index].row = row
			return
		}
		idx.remove(key)
		rows = idx.buckets[bucket]
	}
	item := benchIndexedRow{key: key, row: row}
	pos, _ := slices.BinarySearchFunc(rows, item, compareBenchIndexedRow)
	rows = append(rows, benchIndexedRow{})
	copy(rows[pos+1:], rows[pos:])
	rows[pos] = item
	idx.buckets[bucket] = rows
	for i := pos; i < len(rows); i++ {
		idx.loc[rows[i].key] = benchRowLocation{bucket: bucket, index: i}
	}
}

func (idx *sortedSliceIndex) remove(key string) {
	location, ok := idx.loc[key]
	if !ok {
		return
	}
	rows := idx.buckets[location.bucket]
	copy(rows[location.index:], rows[location.index+1:])
	rows = rows[:len(rows)-1]
	idx.buckets[location.bucket] = rows
	delete(idx.loc, key)
	for i := location.index; i < len(rows); i++ {
		idx.loc[rows[i].key] = benchRowLocation{bucket: location.bucket, index: i}
	}
}

func retainedHeapDelta(b *testing.B, build func() any) (any, int64) {
	b.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	index := build()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(index)
	return index, int64(after.HeapAlloc) - int64(before.HeapAlloc)
}

func BenchmarkAEIndexedMetadataLayouts(b *testing.B) {
	for _, keyCount := range []int{10_000, 100_000} {
		_, mapBytes := retainedHeapDelta(b, func() any {
			return buildBucketMapIndex(b, benchmarkAEStore(b, keyCount))
		})
		runtime.GC()
		_, sliceBytes := retainedHeapDelta(b, func() any {
			return buildSortedSliceIndex(b, benchmarkAEStore(b, keyCount))
		})
		runtime.GC()
		st := benchmarkAEStore(b, keyCount)
		mapIndex := buildBucketMapIndex(b, st)
		sliceIndex := buildSortedSliceIndex(b, st)
		for _, bucketCount := range []int{1, 16, 256} {
			b.Run(fmt.Sprintf("map/keys=%d/buckets=%d", keyCount, bucketCount), func(b *testing.B) {
				buckets := evenlySpacedBuckets(bucketCount)
				b.ReportAllocs()
				for b.Loop() {
					benchAEMetadata, benchAECovered = mapIndex.collect(buckets, MaxMetaBytes)
				}
				b.ReportMetric(float64(mapBytes)/float64(keyCount), "retained-B/key")
			})
			b.Run(fmt.Sprintf("sorted-slices/keys=%d/buckets=%d", keyCount, bucketCount), func(b *testing.B) {
				buckets := evenlySpacedBuckets(bucketCount)
				b.ReportAllocs()
				for b.Loop() {
					benchAEMetadata, benchAECovered = sliceIndex.collect(buckets, MaxMetaBytes)
				}
				b.ReportMetric(float64(sliceBytes)/float64(keyCount), "retained-B/key")
			})
		}
	}
}

func evenlySpacedBuckets(count int) []uint32 {
	buckets := make([]uint32, count)
	for i := range buckets {
		buckets[i] = uint32(i * 256 / count)
	}
	return buckets
}

func BenchmarkAEIndexedMetadataMutationLayouts(b *testing.B) {
	const keyCount = 10_000
	insertKeys := make([]string, 1024)
	for i := range insertKeys {
		insertKeys[i] = fmt.Sprintf("ae-new-%08d", i)
	}
	const updateKey = "ae-key-00000001"
	for _, op := range []string{"overwrite", "insert-evict", "delete-reinsert"} {
		b.Run("map/"+op, func(b *testing.B) {
			st := benchmarkAEStore(b, keyCount)
			idx := buildBucketMapIndex(b, st)
			if op == "delete-reinsert" {
				idx.upsert(updateKey, benchmarkMeta(updateKey, 1))
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				switch op {
				case "overwrite":
					idx.upsert(updateKey, benchmarkMeta(updateKey, uint64(i+1)))
				case "insert-evict":
					poolIdx := i % len(insertKeys)
					idx.remove(insertKeys[poolIdx])
					idx.upsert(insertKeys[poolIdx], benchmarkMeta(insertKeys[poolIdx], uint64(i+1)))
				case "delete-reinsert":
					idx.remove(updateKey)
					idx.upsert(updateKey, benchmarkMeta(updateKey, uint64(i+1)))
				}
			}
		})
		b.Run("sorted-slices/"+op, func(b *testing.B) {
			st := benchmarkAEStore(b, keyCount)
			idx := buildSortedSliceIndex(b, st)
			if op == "delete-reinsert" {
				idx.upsert(updateKey, benchmarkMeta(updateKey, 1))
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				switch op {
				case "overwrite":
					idx.upsert(updateKey, benchmarkMeta(updateKey, uint64(i+1)))
				case "insert-evict":
					poolIdx := i % len(insertKeys)
					idx.remove(insertKeys[poolIdx])
					idx.upsert(insertKeys[poolIdx], benchmarkMeta(insertKeys[poolIdx], uint64(i+1)))
				case "delete-reinsert":
					idx.remove(updateKey)
					idx.upsert(updateKey, benchmarkMeta(updateKey, uint64(i+1)))
				}
			}
		})
	}
}

func BenchmarkAEIndexedMetadataMixedMutationLayouts(b *testing.B) {
	const keyCount = 10_000
	insertKeys := make([]string, 1024)
	for i := range insertKeys {
		insertKeys[i] = fmt.Sprintf("ae-new-%08d", i)
	}
	const updateKey = "ae-key-00000001"
	for _, layout := range []string{"map", "sorted-slices"} {
		b.Run(layout, func(b *testing.B) {
			st := benchmarkAEStore(b, keyCount)
			var mapIndex *bucketMapIndex
			var sliceIndex *sortedSliceIndex
			if layout == "map" {
				mapIndex = buildBucketMapIndex(b, st)
			} else {
				sliceIndex = buildSortedSliceIndex(b, st)
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				key := updateKey
				row := benchmarkMeta(key, uint64(i+1))
				switch i % 20 {
				case 0:
					key = insertKeys[(i/20)%len(insertKeys)]
					row = benchmarkMeta(key, uint64(i+1))
					if layout == "map" {
						mapIndex.upsert(key, row)
					} else {
						sliceIndex.upsert(key, row)
					}
				case 1:
					if layout == "map" {
						mapIndex.remove(updateKey)
						mapIndex.upsert(updateKey, row)
					} else {
						sliceIndex.remove(updateKey)
						sliceIndex.upsert(updateKey, row)
					}
				default:
					if layout == "map" {
						mapIndex.upsert(key, row)
					} else {
						sliceIndex.upsert(key, row)
					}
				}
			}
		})
	}
}

func BenchmarkAEStoreMutationBaseline(b *testing.B) {
	const keyCount = 10_000
	insertKeys := make([]string, keyCount)
	for i := range insertKeys {
		insertKeys[i] = fmt.Sprintf("ae-new-%08d", i)
	}
	const updateKey = "ae-key-00000001"
	for _, op := range []string{"overwrite", "insert", "delete", "expiry"} {
		b.Run(op, func(b *testing.B) {
			st := benchmarkAEStore(b, keyCount)
			if op == "delete" {
				if err := st.PutVersioned(updateKey, benchmarkEntry(1)); err != nil {
					b.Fatal(err)
				}
			}
			if op == "expiry" {
				if err := st.PutWithTTL(updateKey, []byte("expired-value"), time.Nanosecond); err != nil {
					b.Fatal(err)
				}
				time.Sleep(time.Microsecond)
			}
			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				switch op {
				case "overwrite":
					if err := st.PutVersioned(updateKey, benchmarkEntry(uint64(i+2))); err != nil {
						b.Fatal(err)
					}
				case "insert":
					if err := st.PutVersioned(insertKeys[i%len(insertKeys)], benchmarkEntry(uint64(i+1))); err != nil {
						b.Fatal(err)
					}
				case "delete":
					_ = st.Remove(updateKey)
					if err := st.PutVersioned(updateKey, benchmarkEntry(uint64(i+2))); err != nil {
						b.Fatal(err)
					}
				case "expiry":
					if err := st.PutWithTTL(updateKey, []byte("expired-value"), time.Nanosecond); err != nil {
						b.Fatal(err)
					}
					_, _ = st.Get(updateKey)
				}
			}
		})
	}
}

func benchmarkEntry(version uint64) store.Entry {
	return store.Entry{Value: []byte("updated-value"), Version: version, WriterNodeHash: 1}
}

func benchmarkMeta(key string, version uint64) MetaRow {
	return MetaRow{Bucket: BucketFor(key, 256), KeyHash: KeyHash(key), Version: version, Writer: 1}
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
