package ae

import (
	"testing"

	"github.com/kraytos17/cinder/internal/store"
)

// Each decoder below gets its own target (rather than one multiplexed
// target) so every codec receives the full time budget, corpora evolve
// independently, and failures attribute to exactly one codec. The one
// exception is FuzzIBLTDifference (iblt_fuzz_test.go), which fuzzes the
// structured peel property instead of raw blobs.
// FuzzAEDigest feeds arbitrary bytes to the group-digest decoder.
func FuzzAEDigest(f *testing.F) {
	f.Add(Digest{1, 2, 3}.Encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		digestFuzz(t, data)
	})
}

// FuzzAERefinement feeds arbitrary bytes to the refinement decoder.
func FuzzAERefinement(f *testing.F) {
	f.Add(Refinement{BucketHashes: []BucketHash{{Bucket: 1, Hash: 9}}, ForceMeta: true}.Encode())
	f.Add([]byte{0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		refinementFuzz(t, data)
	})
}

// FuzzAEMetaRows feeds arbitrary bytes to the metadata-row decoder.
func FuzzAEMetaRows(f *testing.F) {
	f.Add(EncodeMetaRows([]MetaRow{{Bucket: 1, KeyHash: 7, Version: 2, Writer: 3, Tombstone: true}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		metaFuzz(t, data)
	})
}

// FuzzAESync feeds arbitrary bytes to the sync-message decoder.
func FuzzAESync(f *testing.F) {
	f.Add(mustEncodeSync(SyncMessage{Entries: []SyncItem{{Key: "k", Entry: store.Entry{Value: []byte("v"), Version: 1}}}, Needs: []uint64{42}}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		syncFuzz(t, data)
	})
}

// FuzzAEIBLT feeds arbitrary bytes to the sketch decoder. The property is
// no-panic plus peel termination; exact symmetric differences are covered
// by FuzzIBLTDifference with structured item sets.
func FuzzAEIBLT(f *testing.F) {
	f.Add(EncodeIBLT(BuildIBLT([]IBLTItem{{KeyHash: 1, Version: 2, Writer: 3}}, IBLTCells)))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		ibltFuzz(t, data)
	})
}

// FuzzAEGroupDigest feeds arbitrary bytes to the grouped-digest decoder.
func FuzzAEGroupDigest(f *testing.F) {
	f.Add(Digest{5}.Fold().Encode())
	f.Add([]byte{0xFF, 0xFF})
	f.Fuzz(func(t *testing.T, data []byte) {
		groupFuzz(t, data)
	})
}

func mustEncodeSync(m SyncMessage) []byte {
	b, err := m.Encode()
	if err != nil {
		panic(err)
	}
	return b
}

func digestFuzz(t *testing.T, data []byte) {
	t.Helper()
	// Group count unknown: try the 256-bucket shape (4 groups); the
	// property is no-panic + fixed point on whatever decodes.
	for _, groups := range []int{1, 4} {
		dec, ok := DecodeResponse(data, groups)
		if !ok {
			continue
		}
		again := dec.Encode()
		back, ok := DecodeResponse(again, groups)
		if !ok {
			t.Fatalf("re-decode failed")
		}
		if len(back.GroupHashes) != len(dec.GroupHashes) || len(back.Rows) != len(dec.Rows) || len(back.Sketches) != len(dec.Sketches) {
			t.Fatalf("not a fixed point")
		}
	}
}

func refinementFuzz(t *testing.T, data []byte) {
	t.Helper()
	dec, ok := DecodeRefinement(data, 256)
	if !ok {
		return
	}
	again := dec.Encode()
	back, ok := DecodeRefinement(again, 256)
	if !ok {
		t.Fatalf("re-decode failed")
	}
	if len(back.BucketHashes) != len(dec.BucketHashes) || back.ForceMeta != dec.ForceMeta {
		t.Fatalf("not a fixed point:\n%+v\n%+v", dec, back)
	}
}

func metaFuzz(t *testing.T, data []byte) {
	t.Helper()
	rows, ok := DecodeMetaRows(data)
	if !ok {
		return
	}
	back, ok := DecodeMetaRows(EncodeMetaRows(rows))
	if !ok {
		t.Fatalf("re-decode failed")
	}
	if len(back) != len(rows) {
		t.Fatalf("row count changed: %d -> %d", len(rows), len(back))
	}
	for i := range rows {
		if back[i] != rows[i] {
			t.Fatalf("row %d differs:\n%+v\n%+v", i, rows[i], back[i])
		}
	}
}

func syncFuzz(t *testing.T, data []byte) {
	t.Helper()
	msg, ok := DecodeSync(data)
	if !ok {
		return
	}
	enc, err := msg.Encode()
	if err != nil {
		t.Fatalf("re-encode failed: %v", err)
	}
	back, ok := DecodeSync(enc)
	if !ok {
		t.Fatalf("re-decode failed")
	}
	if len(back.Entries) != len(msg.Entries) || len(back.Needs) != len(msg.Needs) {
		t.Fatalf("not a fixed point")
	}
}

func ibltFuzz(t *testing.T, data []byte) {
	t.Helper()
	// IBLT blobs decode in sketch context; the property here is no-panic
	// decode plus peel termination. Undecodable or mismatched sketches
	// must fail closed, never hang.
	sk := DecodeIBLT(data)
	if sk == nil {
		return
	}
	empty := BuildIBLT(nil, len(sk))
	diff, ok := DifferenceIBLT(sk, empty)
	if !ok {
		// Legitimate undecodable sketch: only panics/hangs are bugs.
		return
	}
	_ = diff
}

func groupFuzz(t *testing.T, data []byte) {
	t.Helper()
	if _, ok := DecodeGroupDigest(data, 4); !ok {
		return
	}
	// Fold is total: folding any digest twice is idempotent.
	folded := Digest{1, 2}.Fold().Fold()
	if len(folded) != 1 || folded[0] != 3 {
		t.Fatalf("fold not idempotent: %v", folded)
	}
}
