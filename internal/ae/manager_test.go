package ae

import (
	"testing"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

func testStore(n uint64) *store.LRUStore {
	return store.NewLRUStore(1<<20, nil)
}

func putVer(t *testing.T, st *store.LRUStore, key, val string, ver, writer uint64) {
	t.Helper()
	e := store.Entry{Value: []byte(val), Version: ver, WriterNodeHash: writer}
	if err := st.PutVersioned(key, e); err != nil {
		t.Fatal(err)
	}
}

func TestDigestDeterministic(t *testing.T) {
	a := testStore(10)
	b := testStore(10)
	m1 := &Manager{store: a, numBuckets: 16}
	m2 := &Manager{store: b, numBuckets: 16}
	putVer(t, a, "k1", "v1", 3, 1)
	putVer(t, b, "k1", "v1", 3, 1)
	putVer(t, a, "k2", "v2", 4, 1)
	putVer(t, b, "k2", "v2", 4, 1)
	d1, d2 := m1.ComputeDigest(), m2.ComputeDigest()
	for i := range d1 {
		if d1[i] != d2[i] {
			t.Fatalf("bucket %d differs: %x vs %x", i, d1[i], d2[i])
		}
	}
	putVer(t, b, "k3", "v3", 5, 1)
	d3 := m2.ComputeDigest()
	same := true
	for i := range d1 {
		if d1[i] != d3[i] {
			same = false
		}
	}
	if same {
		t.Fatal("divergent stores must differ")
	}
}

func TestGroupFoldMatchesBucketCount(t *testing.T) {
	m := &Manager{store: testStore(10), numBuckets: 256}
	if got := len(GroupFold(m.ComputeDigest())); got != 16 {
		t.Fatalf("groups = %d", got)
	}
}

func TestDiffAndApply(t *testing.T) {
	local := testStore(10)
	remote := testStore(10)
	putVer(t, local, "same", "v", 5, 1)
	putVer(t, remote, "same", "v", 5, 1)
	putVer(t, local, "mine", "m", 3, 1)    // local wins by absence
	putVer(t, remote, "theirs", "t", 4, 1) // remote wins by absence
	putVer(t, local, "old", "o", 1, 1)
	putVer(t, remote, "old", "n", 9, 1) // remote wins LWW
	putVer(t, local, "new", "n", 9, 1)
	putVer(t, remote, "new", "o", 1, 1) // local wins LWW
	putVer(t, local, "dead", "x", 2, 1)
	if err := remote.RemoveVersioned("dead", 3, 1); err != nil {
		t.Fatal(err)
	} // remote tombstone wins

	m := &Manager{store: local, numBuckets: 256}
	rows := remoteRows(t, remote, nil)
	diff := m.DiffRows(rows, allBuckets(256), nil)
	need := map[uint64]bool{}
	for _, h := range diff.Need {
		need[h] = true
	}
	for _, k := range []string{"theirs", "old", "dead"} {
		if !need[KeyHash(k)] {
			t.Fatalf("expected need for %q: %v", k, diff.Need)
		}
	}
	if need[KeyHash("mine")] || need[KeyHash("same")] || need[KeyHash("new")] {
		t.Fatalf("unexpected need: %v", diff.Need)
	}
	have := map[string]bool{}
	for _, h := range diff.Have {
		have[h.Key] = true
	}
	if !have["mine"] || !have["new"] {
		t.Fatalf("have = %+v", diff.Have)
	}
	// Apply a sync carrying the remote winners.
	msg := SyncMessage{}
	for _, h := range diff.Need {
		e, ok := remote.GetVersioned(keyByHash(t, remote, h))
		if !ok {
			t.Fatalf("remote missing %x", h)
		}
		msg.Entries = append(msg.Entries, SyncItem{Key: keyByHash(t, remote, h), Entry: e})
	}
	if n := m.ApplySync(msg); n != 3 {
		t.Fatalf("repaired = %d", n)
	}
	// Idempotent re-apply repairs nothing.
	if n := m.ApplySync(msg); n != 0 {
		t.Fatalf("re-apply repaired %d", n)
	}
	for _, k := range []string{"theirs", "old"} {
		if e, ok := local.GetVersioned(k); !ok {
			t.Fatalf("%q missing after sync", k)
		} else if string(e.Value) == "" && k != "dead" {
			t.Fatalf("%q empty", k)
		}
	}
	if e, ok := local.GetVersioned("dead"); !ok || !e.Tombstone {
		t.Fatalf("tombstone not applied: %+v %v", e, ok)
	}
	if _, ok := local.Get("dead"); ok {
		t.Fatal("tombstone reads live")
	}
}

func allBuckets(n uint32) []uint32 {
	out := make([]uint32, 0, n)
	for i := range n {
		out = append(out, i)
	}
	return out
}

func remoteRows(t *testing.T, st *store.LRUStore, _ []uint32) []MetaRow {
	t.Helper()
	var rows []MetaRow
	st.ForEach(func(key string, e store.Entry) {
		rows = append(rows, MetaRow{Bucket: BucketFor(key, 256), KeyHash: KeyHash(key), Version: e.Version, Writer: e.WriterNodeHash, Tombstone: e.Tombstone})
	})
	return rows
}

func keyByHash(t *testing.T, st *store.LRUStore, h uint64) string {
	t.Helper()
	found := ""
	st.ForEach(func(key string, _ store.Entry) {
		if KeyHash(key) == h {
			found = key
		}
	})
	if found == "" {
		t.Fatalf("no key for hash %x", h)
	}
	return found
}

func TestCollectMetadataBudget(t *testing.T) {
	st := testStore(10)
	for i := range 50 {
		putVer(t, st, string(rune('a'+i/26))+string(rune('0'+i%26)), "v", uint64(i+1), 1)
	}
	m := &Manager{store: st, numBuckets: 256}
	// Tiny budget: bucket-atomic fill. Every emitted row must sit in a
	// covered bucket, and at least the first bucket is always covered.
	blob, covered := m.CollectMetadata(allBuckets(256), 100)
	if len(covered) == 0 {
		t.Fatal("first bucket must be covered even over budget")
	}
	rows, ok := DecodeMetaRows(blob)
	if !ok {
		t.Fatal("own encoding must decode")
	}
	coveredSet := map[uint32]bool{}
	for _, b := range covered {
		coveredSet[b] = true
	}
	for _, r := range rows {
		if !coveredSet[r.Bucket] {
			t.Fatalf("row outside covered set: %+v in %v", r, covered)
		}
	}
	// Generous budget covers everything present.
	blob, covered = m.CollectMetadata(allBuckets(256), MaxMetaBytes)
	rows, ok = DecodeMetaRows(blob)
	if !ok {
		t.Fatal("decode failed")
	}
	if len(rows) != 50 {
		t.Fatalf("rows = %d", len(rows))
	}
	_ = covered
}

func TestHandlerRoundTrip(t *testing.T) {
	a := testStore(10)
	b := testStore(10)
	ma := &Manager{store: a, numBuckets: 64}
	mb := &Manager{store: b, numBuckets: 64}
	putVer(t, a, "k", "va", 5, 1)

	// Initiator digest -> responder answers its own group hashes.
	st, val := mb.OnDigestRequest(EncodeGroupDigest(GroupFold(mb.ComputeDigest())))
	if st != protocol.StatusOK {
		t.Fatalf("digest status %v", st)
	}
	groups := GroupFold(ma.ComputeDigest())
	if _, ok := DecodeResponse(val, len(groups)); !ok {
		t.Fatal("bad digest response")
	}
	// Refine the actually-divergent group (the one holding k).
	local := ma.ComputeDigest()
	kb := BucketFor("k", 64)
	fb := Refinement{}
	for b := (kb / BucketsPerGroup) * BucketsPerGroup; b < (kb/BucketsPerGroup+1)*BucketsPerGroup && b < 64; b++ {
		fb.BucketHashes = append(fb.BucketHashes, BucketHash{Bucket: uint32(b), Hash: local[uint32(b)]})
	}
	st, val2 := mb.OnBucketRefinement(EncodeRefinement(fb))
	if st != protocol.StatusOK {
		t.Fatalf("refinement status %v", st)
	}
	dec, ok := DecodeResponse(val2, len(groups))
	if !ok || len(dec.Covered) == 0 {
		t.Fatalf("refinement response: %+v %v", dec, ok)
	}
	// Diff remote rows (empty: b has nothing) against local: everything is
	// "have" (local wins by absence), nothing needed.
	diff := ma.DiffRows(dec.Rows, []uint32{uint32(BucketFor("k", 64))}, dec.Covered)
	if len(diff.Need) != 0 || len(diff.Have) != 1 {
		t.Fatalf("diff = %+v", diff)
	}
	// Full sync of the local winner into b converges it.
	items := ma.CollectEntriesForKeys([]string{diff.Have[0].Key}, 1<<20)
	if len(items) != 1 {
		t.Fatalf("items = %+v", items)
	}
	syncBytes, err := EncodeSync(SyncMessage{Entries: items})
	if err != nil {
		t.Fatal(err)
	}
	msg, ok := DecodeSync(syncBytes)
	if !ok {
		t.Fatal("own sync undecodable")
	}
	st, val3 := mb.OnSyncRequest(syncBytes)
	if st != protocol.StatusOK {
		t.Fatalf("sync status %v", st)
	}
	resp, ok := DecodeSync(val3)
	if !ok || len(resp.Entries) != 0 || len(resp.Needs) != 0 {
		t.Fatalf("sync response: %+v %v", resp, ok)
	}
	_ = msg
	if e, ok := b.GetVersioned("k"); !ok || string(e.Value) != "va" {
		t.Fatalf("b missing k: %+v %v", e, ok)
	}
}
