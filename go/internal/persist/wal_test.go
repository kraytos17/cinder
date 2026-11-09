package persist

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWALRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	entries := []Entry{
		{Op: OpSet, Key: []byte("k1"), Value: []byte("v1"), Version: 2, Writer: 9},
		{Op: OpSet, Key: []byte("k2"), Value: []byte("v2"), Version: 3, Writer: 9, HasTTL: true, ExpiresMs: 1700000000000},
		{Op: OpSet, Key: []byte("t"), Tombstone: true, Version: 4, Writer: 9, HasTTL: true, ExpiresMs: 1700000000500},
		{Op: OpDel, Key: []byte("gone"), Version: 5, Writer: 9},
		{Op: OpSet, Key: []byte(""), Value: []byte("")},
	}
	if _, err := w.AppendBatch(entries[:2]); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries[2:] {
		if err := w.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	var got []Entry
	for r.HasMore() {
		e, ok := r.Next()
		if !ok {
			break
		}
		got = append(got, e)
	}
	if r.Corrupted() {
		t.Fatal("unexpected corruption")
	}
	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	for i := range entries {
		a, b := entries[i], got[i]
		if a.Op != b.Op || string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) ||
			a.Version != b.Version || a.Writer != b.Writer || a.ExpiresMs != b.ExpiresMs ||
			a.HasTTL != b.HasTTL || a.Tombstone != b.Tombstone {
			t.Fatalf("entry %d mismatch:\n got %+v\nwant %+v", i, b, a)
		}
	}
}

func TestWALCppInterop(t *testing.T) {
	r, err := OpenReader("testdata/cpp_wal_v2.log")
	if err != nil {
		t.Fatal(err)
	}
	var got []Entry
	for r.HasMore() {
		e, ok := r.Next()
		if !ok {
			break
		}
		got = append(got, e)
	}
	if r.Corrupted() {
		t.Fatal("C++ file flagged corrupt")
	}
	want := []Entry{
		{Op: OpSet, Key: []byte("key-1"), Value: []byte("value-1"), Version: 7, Writer: 42},
		{Op: OpSet, Key: []byte("ttl-key"), Value: []byte("tv"), Version: 8, Writer: 43, HasTTL: true, ExpiresMs: 1700000000000},
		{Op: OpSet, Key: []byte("tomb"), Version: 9, Writer: 43, HasTTL: true, ExpiresMs: 1700000000500, Tombstone: true},
		{Op: OpDel, Key: []byte("gone"), Version: 10, Writer: 44},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries: %+v", len(got), got)
	}
	for i := range want {
		a, b := want[i], got[i]
		if a.Op != b.Op || string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) ||
			a.Version != b.Version || a.Writer != b.Writer || a.ExpiresMs != b.ExpiresMs ||
			a.HasTTL != b.HasTTL || a.Tombstone != b.Tombstone {
			t.Fatalf("entry %d mismatch:\n got %+v\nwant %+v", i, b, a)
		}
	}
}

func TestWALTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v"), Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k-tail"), Value: []byte("longer-value"), Version: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Truncate mid-second-record: clean stop, no corruption, validSize at
	// the end of record 1.
	raw, _ := os.ReadFile(path)
	cut := raw[:len(raw)-3]
	if err := os.WriteFile(path, cut, 0644); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Next(); !ok {
		t.Fatal("first record must decode")
	}
	if _, ok := r.Next(); ok {
		t.Fatal("torn tail must stop iteration")
	}
	if r.Corrupted() {
		t.Fatal("torn tail is not corruption")
	}
	// Recovery truncates to the watermark and appends cleanly after.
	if err := os.Truncate(path, int64(r.ValidSize())); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Append(Entry{Op: OpSet, Key: []byte("k2"), Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}
	w2.Close()
	r2, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for r2.HasMore() {
		if _, ok := r2.Next(); !ok {
			break
		}
		n++
	}
	if n != 2 || r2.Corrupted() {
		t.Fatalf("replay after truncate: n=%d corrupt=%v", n, r2.Corrupted())
	}
}

func TestWALCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xFF // flip a checksum bit
	os.WriteFile(path, raw, 0644)
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Next(); ok {
		t.Fatal("corrupt record decoded")
	}
	if !r.Corrupted() {
		t.Fatal("checksum mismatch must flag corruption")
	}
	// Bad opcode is structural corruption, not a torn tail.
	raw2 := append([]byte{}, raw...)
	raw2[8] = 0x42
	os.WriteFile(path, raw2, 0644)
	r2, _ := OpenReader(path)
	if _, ok := r2.Next(); ok {
		t.Fatal("bad opcode decoded")
	}
	if !r2.Corrupted() {
		t.Fatal("bad opcode must flag corruption")
	}
}

func TestWALAppendMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Entry{Op: OpSet, Key: []byte("a"), Value: []byte("1")})
	w.Close()
	w2, err := OpenWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	w2.Append(Entry{Op: OpSet, Key: []byte("b"), Value: []byte("2")})
	w2.Close()
	r, _ := OpenReader(path)
	n := 0
	for r.HasMore() {
		if _, ok := r.Next(); !ok {
			break
		}
		n++
	}
	if n != 2 || r.Corrupted() {
		t.Fatalf("n=%d corrupt=%v", n, r.Corrupted())
	}
	// Garbage file in append mode restarts fresh.
	os.WriteFile(path, []byte("garbage-bytes-here!!"), 0644)
	w3, err := OpenWriter(path, false)
	if err != nil {
		t.Fatal(err)
	}
	w3.Append(Entry{Op: OpSet, Key: []byte("c"), Value: []byte("3")})
	w3.Close()
	r3, _ := OpenReader(path)
	n = 0
	for r3.HasMore() {
		if _, ok := r3.Next(); !ok {
			break
		}
		n++
	}
	if n != 1 || r3.Corrupted() {
		t.Fatalf("fresh-start replay: n=%d corrupt=%v", n, r3.Corrupted())
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snap.csp"
	entries := []SnapEntry{
		{Key: []byte("a"), Value: []byte("1"), Version: 2, Writer: 7},
		{Key: []byte("b"), Value: []byte("22"), Version: 3, Writer: 7, HasTTL: true, ExpiresMs: 2700000000000, Freq: 3},
		{Key: []byte("t"), Version: 4, Writer: 7, HasTTL: true, ExpiresMs: 2700000000500, Tombstone: true},
	}
	if err := NewSnapWriter(path).Write(9, entries); err != nil {
		t.Fatal(err)
	}
	data, err := NewSnapReader(path).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if data.NextVersion != 9 || len(data.Entries) != len(entries) {
		t.Fatalf("got %+v", data)
	}
	for i := range entries {
		a, b := entries[i], data.Entries[i]
		if string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) ||
			a.Version != b.Version || a.Writer != b.Writer || a.ExpiresMs != b.ExpiresMs ||
			a.HasTTL != b.HasTTL || a.Tombstone != b.Tombstone || a.Freq != b.Freq {
			t.Fatalf("entry %d mismatch:\n got %+v\nwant %+v", i, b, a)
		}
	}
}

func TestSnapshotCppInterop(t *testing.T) {
	data, err := NewSnapReader("testdata/cpp_snap_v2.csp").ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if data.NextVersion != 11 {
		t.Fatalf("next_version = %d", data.NextVersion)
	}
	want := []SnapEntry{
		{Key: []byte("key-1"), Value: []byte("value-1"), Version: 7, Writer: 42},
		{Key: []byte("ttl-key"), Value: []byte("tv"), Version: 8, Writer: 43, HasTTL: true, ExpiresMs: 2700000000000, Freq: 3},
		{Key: []byte("tomb"), Version: 9, Writer: 43, HasTTL: true, ExpiresMs: 2700000000500, Tombstone: true},
	}
	if len(data.Entries) != len(want) {
		t.Fatalf("entries = %+v", data.Entries)
	}
	for i := range want {
		a, b := want[i], data.Entries[i]
		if string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) ||
			a.Version != b.Version || a.Writer != b.Writer || a.ExpiresMs != b.ExpiresMs ||
			a.HasTTL != b.HasTTL || a.Tombstone != b.Tombstone || a.Freq != b.Freq {
			t.Fatalf("entry %d mismatch:\n got %+v\nwant %+v", i, b, a)
		}
	}
}

func TestSnapshotV3Rejected(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snap.csp"
	var hdr []byte
	hdr = putU32(hdr, snapMagic)
	hdr = putU32(hdr, 3)
	hdr = putU64(hdr, 0)
	hdr = putU32(hdr, 0)
	os.WriteFile(path, hdr, 0644)
	if _, err := NewSnapReader(path).ReadAll(); err == nil {
		t.Fatal("v3 must fail cleanly (zstd blocks deferred)")
	}
}

func TestSnapshotStreamEarlyStop(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/snap.csp"
	var entries []SnapEntry
	for i := range 10 {
		entries = append(entries, SnapEntry{Key: []byte{byte('a' + i)}, Value: []byte("v")})
	}
	if err := NewSnapWriter(path).Write(1, entries); err != nil {
		t.Fatal(err)
	}
	var seen int
	next, err := NewSnapReader(path).ReadStream(func(SnapEntry) bool {
		seen++
		return seen < 3
	})
	if err != nil || next != 1 || seen != 3 {
		t.Fatalf("next=%d seen=%d err=%v", next, seen, err)
	}
	// Streaming output must equal bulk output.
	bulk, err := NewSnapReader(path).ReadAll()
	if err != nil || len(bulk.Entries) != 10 {
		t.Fatalf("bulk: %+v %v", bulk, err)
	}
	var streamed []SnapEntry
	NewSnapReader(path).ReadStream(func(e SnapEntry) bool {
		e.Key = append([]byte(nil), e.Key...)
		e.Value = append([]byte(nil), e.Value...)
		streamed = append(streamed, e)
		return true
	})
	for i := range bulk.Entries {
		if !bytes.Equal(bulk.Entries[i].Key, streamed[i].Key) {
			t.Fatal("stream/bulk diverge")
		}
	}
}
