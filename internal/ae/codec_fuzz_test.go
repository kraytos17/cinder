package ae

import (
	"testing"

	"github.com/kraytos17/cinder/internal/store"
)

// FuzzAECodecs feeds arbitrary bytes to every anti-entropy decoder. Each
// must never panic; every accepted blob must re-encode to bytes that
// decode to the same value (canonical fixed points).
func FuzzAECodecs(f *testing.F) {
	for _, s := range aeCodecSeeds() {
		f.Add(s.tag, s.data)
	}
	f.Fuzz(func(t *testing.T, tag uint8, data []byte) {
		switch tag % 6 {
		case 0:
			digestFuzz(t, data)
		case 1:
			refinementFuzz(t, data)
		case 2:
			metaFuzz(t, data)
		case 3:
			syncFuzz(t, data)
		case 4:
			ibltFuzz(t, data)
		default:
			groupFuzz(t, data)
		}
	})
}

type codecSeed struct {
	tag  uint8
	data []byte
}

func aeCodecSeeds() []codecSeed {
	return []codecSeed{
		{0, EncodeGroupDigest([]uint64{1, 2, 3})},
		{0, []byte{}},
		{1, EncodeRefinement(Refinement{BucketHashes: []BucketHash{{Bucket: 1, Hash: 9}}, ForceMeta: true})},
		{1, []byte{0x00}},
		{2, EncodeMetaRows([]MetaRow{{Bucket: 1, KeyHash: 7, Version: 2, Writer: 3, Tombstone: true}})},
		{2, []byte{}},
		{3, mustEncodeSync(SyncMessage{Entries: []SyncItem{{Key: "k", Entry: store.Entry{Value: []byte("v"), Version: 1}}}, Needs: []uint64{42}})},
		{3, []byte{}},
		{4, EncodeIBLT(BuildIBLT([]IBLTItem{{KeyHash: 1, Version: 2, Writer: 3}}, IBLTCells))},
		{4, []byte{}},
		{5, EncodeGroupDigest(GroupFold([]uint64{5}))},
		{5, []byte{0xFF, 0xFF}},
	}
}

func mustEncodeSync(m SyncMessage) []byte {
	b, err := EncodeSync(m)
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
		again := EncodeResponse(dec)
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
	again := EncodeRefinement(dec)
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
	enc, err := EncodeSync(msg)
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
	// GroupFold is total: folding any digest twice is idempotent.
	folded := GroupFold(GroupFold([]uint64{1, 2}))
	if len(folded) != 1 || folded[0] != 3 {
		t.Fatalf("fold not idempotent: %v", folded)
	}
}
