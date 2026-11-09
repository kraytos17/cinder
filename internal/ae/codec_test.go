package ae

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/kraytos17/cinder/internal/store"
)

func entryForTest(value string, version, writer uint64) store.Entry {
	return store.Entry{Value: []byte(value), Version: version, WriterNodeHash: writer}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Every vector below was produced by the C++ codec (oracle). Any byte of
// divergence fails here first.
func TestOracleGroupDigest(t *testing.T) {
	buckets := make([]uint64, 17)
	buckets[0] = 0x123456789ABCDEF0
	groups := GroupFold(buckets)
	if len(groups) != 2 {
		t.Fatalf("groups = %d", len(groups))
	}
	got := EncodeGroupDigest(groups)
	want := mustHex(t, "334D4541020000006097734C8796ADC8299368DD226403CA")
	if !bytes.Equal(got, want) {
		t.Fatalf("group digest:\n got %X\nwant %X", got, want)
	}
	if _, ok := DecodeGroupDigest(want, 2); !ok {
		t.Fatal("reject own encoding")
	}
	if _, ok := DecodeGroupDigest(want, 1); ok {
		t.Fatal("wrong group count accepted")
	}
}

func TestOracleRefinement(t *testing.T) {
	req := Refinement{BucketHashes: []BucketHash{{Bucket: 3, Hash: 0xAAAAAAAAAAAAAAAA}, {Bucket: 7, Hash: 0xBBBBBBBBBBBBBBBB}}}
	got := EncodeRefinement(req)
	want := mustHex(t, "35424541000000000200000003000000AAAAAAAAAAAAAAAA07000000BBBBBBBBBBBBBBBB")
	if !bytes.Equal(got, want) {
		t.Fatalf("refinement:\n got %X\nwant %X", got, want)
	}
	req.ForceMeta = true
	got = EncodeRefinement(req)
	want = mustHex(t, "35424541010000000200000003000000AAAAAAAAAAAAAAAA07000000BBBBBBBBBBBBBBBB")
	if !bytes.Equal(got, want) {
		t.Fatalf("refinement+force:\n got %X\nwant %X", got, want)
	}
	dec, ok := DecodeRefinement(want, 256)
	if !ok || !dec.ForceMeta || len(dec.BucketHashes) != 2 {
		t.Fatalf("decode: %+v %v", dec, ok)
	}
	if _, ok := DecodeRefinement(want, 3); ok {
		t.Fatal("out-of-range bucket accepted")
	}
	bad := append([]byte{}, want...)
	bad[4] = 2 // force_metadata bit 1 set
	if _, ok := DecodeRefinement(bad, 256); ok {
		t.Fatal("bad flags accepted")
	}
}

func TestOracleMetaRows(t *testing.T) {
	rows := []MetaRow{
		{Bucket: 3, KeyHash: 0x1111111111111111, Version: 5, Writer: 42},
		{Bucket: 3, Tombstone: true, KeyHash: 0x2222222222222222, Version: 6, Writer: 43},
	}
	got := EncodeMetaRows(rows)
	want := mustHex(t, "0200000003000000111111111111111105000000000000002A000000000000000003000000222222222222222206000000000000002B0000000000000001")
	if !bytes.Equal(got, want) {
		t.Fatalf("metarows:\n got %X\nwant %X", got, want)
	}
	dec, ok := DecodeMetaRows(want)
	if !ok || len(dec) != 2 || dec[1].Tombstone != true {
		t.Fatalf("decode: %+v %v", dec, ok)
	}
	trunc := want[:len(want)-1]
	if _, ok := DecodeMetaRows(trunc); ok {
		t.Fatal("truncated rows accepted")
	}
}

func TestOracleIBLTHeader(t *testing.T) {
	// First cell of a 64-cell sketch over the two oracle rows: count=1,
	// key/version/writer XORs. Verifies cell layout + XXH3 item checksum.
	rows := []IBLTItem{
		{KeyHash: 0x1111111111111111, Version: 5, Writer: 42},
		{KeyHash: 0x2222222222222222, Version: 6, Writer: 43, Tombstone: true},
	}
	sk := BuildIBLT(rows, 64)
	if sk == nil {
		t.Fatal("build failed")
	}
	enc := EncodeIBLT(sk)
	if len(enc) != 8+64*48 {
		t.Fatalf("len = %d", len(enc))
	}
	want := mustHex(t, "3449454140000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
	if !bytes.Equal(enc[:8+48], want) {
		t.Fatalf("iblt header+cell0:\n got %X\nwant %X", enc[:8+48], want)
	}
	dec := DecodeIBLT(enc)
	if dec == nil || len(dec) != 64 {
		t.Fatal("reject own encoding")
	}
	for i := range dec {
		a, b := dec[i], sk[i]
		if a != b {
			t.Fatalf("cell %d differs", i)
		}
	}
	if DecodeIBLT(enc[:len(enc)-1]) != nil {
		t.Fatal("truncated sketch accepted")
	}
	if DecodeIBLT(append([]byte("AEI4"), make([]byte, 4)...)[:7]) != nil {
		t.Fatal("short header accepted")
	}
	// Absurd count bits are rejected like the C++ decoder.
	bad := append([]byte(nil), enc...)
	for i := range 8 {
		bad[8+i] = 0xFF
	}
	if DecodeIBLT(bad) != nil {
		t.Fatal("absurd count accepted")
	}
}

func TestOracleDigestResponse(t *testing.T) {
	groups := GroupFold(append([]uint64{0x123456789ABCDEF0}, make([]uint64, 16)...))
	rows := []MetaRow{
		{Bucket: 3, KeyHash: 0x1111111111111111, Version: 5, Writer: 42},
		{Bucket: 3, Tombstone: true, KeyHash: 0x2222222222222222, Version: 6, Writer: 43},
	}
	resp := DigestResponse{
		GroupHashes:  groups,
		BucketHashes: []BucketHash{{Bucket: 3, Hash: 0xAAAAAAAAAAAAAAAA}},
		Covered:      []uint32{3},
		Rows:         rows,
	}
	got := EncodeResponse(resp)
	want := mustHex(t, "36524541020000006097734C8796ADC8299368DD226403CA0100000003000000AAAAAAAAAAAAAAAA01000000030000003E0000000200000003000000111111111111111105000000000000002A000000000000000003000000222222222222222206000000000000002B000000000000000100000000")
	if !bytes.Equal(got, want) {
		t.Fatalf("response:\n got %X\nwant %X", got, want)
	}
	dec, ok := DecodeResponse(want, 2)
	if !ok {
		t.Fatal("reject own encoding")
	}
	if len(dec.Rows) != 2 || len(dec.Covered) != 1 || dec.Covered[0] != 3 {
		t.Fatalf("decoded = %+v", dec)
	}
	if _, ok := DecodeResponse(want, 1); ok {
		t.Fatal("wrong group count accepted")
	}
	// Row outside the covered set is rejected.
	bad := DigestResponse{GroupHashes: groups, BucketHashes: []BucketHash{{Bucket: 3, Hash: 1}}, Covered: []uint32{9}, Rows: rows}
	if _, ok := DecodeResponse(EncodeResponse(bad), 2); ok {
		t.Fatal("uncovered row accepted")
	}
	// Duplicate sketch buckets are rejected.
	sk := EncodeIBLT(BuildIBLT([]IBLTItem{{KeyHash: 1, Version: 1}}, 64))
	dup := DigestResponse{
		GroupHashes: groups, Covered: []uint32{3},
		Sketches: []SketchRef{{Bucket: 3, Blob: sk}, {Bucket: 3, Blob: sk}},
	}
	if _, ok := DecodeResponse(EncodeResponse(dup), 2); ok {
		t.Fatal("duplicate sketch accepted")
	}
}

func TestOracleSync(t *testing.T) {
	msg := SyncMessage{
		Entries: []SyncItem{{Key: "key-9", Entry: entryForTest("hello", 9, 7)}},
		Needs:   []uint64{0xDEADBEEFDEADBEEF},
	}
	got, err := EncodeSync(msg)
	if err != nil {
		t.Fatal(err)
	}
	want := mustHex(t, "534D454101000000050000006B65792D3909000000000000000700000000000000000500000068656C6C6F01000000EFBEADDEEFBEADDE")
	if !bytes.Equal(got, want) {
		t.Fatalf("sync:\n got %X\nwant %X", got, want)
	}
	dec, ok := DecodeSync(want)
	if !ok || len(dec.Entries) != 1 || dec.Entries[0].Key != "key-9" || string(dec.Entries[0].Entry.Value) != "hello" {
		t.Fatalf("decoded = %+v %v", dec, ok)
	}
	if len(dec.Needs) != 1 || dec.Needs[0] != 0xDEADBEEFDEADBEEF {
		t.Fatalf("needs = %v", dec.Needs)
	}
	// Unknown entry flags rejected; trailing bytes rejected.
	bad := append([]byte(nil), want...)
	bad[33] = 0x04 // flags byte: magic(4)+count(4)+klen(4)+key(5)+ver(8)+writer(8)
	if _, ok := DecodeSync(bad); ok {
		t.Fatal("bad flags accepted")
	}
	if _, ok := DecodeSync(append(want, 0x00)); ok {
		t.Fatal("trailing bytes accepted")
	}
	if _, ok := DecodeSync(want[:len(want)-1]); ok {
		t.Fatal("truncated sync accepted")
	}
}

func TestIBLTDifference(t *testing.T) {
	mk := func(h uint64, v uint64) IBLTItem { return IBLTItem{KeyHash: h, Version: v, Writer: 1} }
	// Symmetric difference of 2 items peels cleanly out of 64 cells.
	a := []IBLTItem{mk(100, 1), mk(200, 2), mk(300, 3)}
	b := []IBLTItem{mk(200, 2), mk(300, 3), mk(400, 4)}
	diff, ok := DifferenceIBLT(BuildIBLT(a, 64), BuildIBLT(b, 64))
	if !ok {
		t.Fatal("peel failed")
	}
	if len(diff.RemoteOnly) != 1 || diff.RemoteOnly[0].KeyHash != 100 {
		t.Fatalf("remote = %+v", diff.RemoteOnly)
	}
	if len(diff.LocalOnly) != 1 || diff.LocalOnly[0].KeyHash != 400 {
		t.Fatalf("local = %+v", diff.LocalOnly)
	}
	// Identical sketches peel to empty.
	diff, ok = DifferenceIBLT(BuildIBLT(a, 64), BuildIBLT(a, 64))
	if !ok || len(diff.RemoteOnly) != 0 || len(diff.LocalOnly) != 0 {
		t.Fatalf("identical: %+v %v", diff, ok)
	}
	// Size mismatch fails cleanly (P2 fallback path).
	if _, ok := DifferenceIBLT(BuildIBLT(a, 64), BuildIBLT(a, 32)); ok {
		t.Fatal("size mismatch peeled")
	}
	// Tombstone bit is part of item identity: live write and delete never
	// cancel each other during peeling.
	live := []IBLTItem{{KeyHash: 7, Version: 1, Writer: 1}}
	del := []IBLTItem{{KeyHash: 7, Version: 1, Writer: 1, Tombstone: true}}
	diff, ok = DifferenceIBLT(BuildIBLT(live, 64), BuildIBLT(del, 64))
	if !ok || len(diff.RemoteOnly) != 1 || len(diff.LocalOnly) != 1 {
		t.Fatalf("tombstone identity: %+v %v", diff, ok)
	}
}
