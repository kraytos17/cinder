package persist

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/kraytos17/cinder/internal/codec"
	"github.com/kraytos17/cinder/internal/expiry"
)

// FuzzSnapshot feeds arbitrary bytes to the snapshot decoder. It must
// never panic; a cleanly decoded image must re-encode (v2) to bytes that
// decode to the same entries (modulo format: v1 has no tombstone byte,
// and decoded v1 markers are already Tombstone=false, so equality is
// exact).
func FuzzSnapshot(f *testing.F) {
	for _, s := range buildSnapSeeds() {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		next, entries, ok := decodeAll(data)
		_ = next
		if !ok {
			return
		}
		if uint64(len(entries)) > 0xFFFFFFFF {
			t.Fatal("impossible entry count")
		}
		// Re-encode as v2 and re-decode: fixed point.
		var body []byte
		for _, e := range entries {
			enc, err := encodeSnapEntry(e)
			if err != nil {
				t.Fatalf("re-encode failed: %v", err)
			}
			body = append(body, enc...)
		}
		hdr := make([]byte, 0, 20)
		hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapMagic))
		hdr = codec.Append(hdr, binary.LittleEndian, uint32(2))
		hdr = codec.Append(hdr, binary.LittleEndian, next)
		hdr = codec.AppendLen(hdr, binary.LittleEndian, len(entries))
		back, err := decodeEntries(append(hdr, body...))
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(back) != len(entries) {
			t.Fatalf("entry count changed: %d -> %d", len(entries), len(back))
		}
		for i := range entries {
			if !snapEqual(entries[i], back[i]) {
				t.Fatalf("entry %d differs:\n%+v\n%+v", i, entries[i], back[i])
			}
		}
	})
}

func decodeAll(data []byte) (uint64, []SnapEntry, bool) {
	var entries []SnapEntry
	next, err := decodeSnapshot(data, func(e SnapEntry) bool {
		entries = append(entries, e)
		// Bound accumulation: a lying count with tiny records could
		// otherwise grow the slice on garbage.
		return len(entries) < 10000
	})
	if err != nil {
		return 0, nil, false
	}
	return next, entries, true
}

func decodeEntries(raw []byte) ([]SnapEntry, error) {
	var entries []SnapEntry
	_, err := decodeSnapshot(raw, func(e SnapEntry) bool {
		entries = append(entries, e)
		return true
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func buildSnapSeeds() [][]byte {
	var out [][]byte
	out = append(out, []byte{})
	out = append(out, []byte{0x00, 0x01, 0x02})
	mk := func(next uint64, entries []SnapEntry) []byte {
		var body []byte
		for _, e := range entries {
			enc, err := encodeSnapEntry(e)
			if err != nil {
				panic(err)
			}
			body = append(body, enc...)
		}
		hdr := make([]byte, 0, 20)
		hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapMagic))
		hdr = codec.Append(hdr, binary.LittleEndian, uint32(2))
		hdr = codec.Append(hdr, binary.LittleEndian, next)
		hdr = codec.AppendLen(hdr, binary.LittleEndian, len(entries))
		return append(hdr, body...)
	}
	out = append(out, mk(7, []SnapEntry{{Key: []byte("k"), Value: []byte("v"), Version: 1, Writer: 2, Freq: 3}}))
	out = append(out, mk(9, []SnapEntry{{Key: []byte("t"), Version: 4, Writer: 5, Expiry: expiry.FromUnixMilli(100), Tombstone: true}}))
	full := mk(1, []SnapEntry{{Key: []byte("a"), Value: []byte("1")}, {Key: []byte("b"), Value: []byte("2")}})
	out = append(out, full)
	out = append(out, full[:len(full)-5]) // torn tail
	// v3 (compressed) must fail cleanly.
	v3 := append([]byte{}, full...)
	v3[7] = 3
	out = append(out, v3)
	return out
}

func snapEqual(a, b SnapEntry) bool {
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value) &&
		a.Version == b.Version && a.Writer == b.Writer &&
		a.Expiry.UnixMilli() == b.Expiry.UnixMilli() &&
		a.Tombstone == b.Tombstone && a.Freq == b.Freq
}
