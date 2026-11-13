package persist

import (
	"bytes"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/expiry"
)

// FuzzWALReader feeds arbitrary bytes to the WAL reader. It must never
// panic; a clean drain must consume a prefix (ValidSize) and any decoded
// record must re-encode to bytes that decode to the same record
// (encode->decode fixed point on the accepted domain).
func FuzzWALReader(f *testing.F) {
	seeds := buildWALSeeds()
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, format := range []Format{FormatLegacy, FormatV1, FormatV2, FormatV3} {
			r := &Reader{data: append([]byte(nil), data...), format: format}
			for {
				e, ok := r.Next()
				if !ok {
					break
				}
				enc, err := e.encode()
				if err != nil {
					t.Fatalf("re-encode of accepted record failed: %v", err)
				}
				r2 := &Reader{data: enc, format: FormatV3}
				back, ok := r2.Next()
				if !ok {
					t.Fatalf("re-decode of re-encoded record failed: %+v", e)
				}
				// V1 has no tombstone byte: tombstones do not survive a
				// V1-framed round trip, so compare modulo tombstone when
				// the harness format drops it.
				if format == FormatLegacy || format == FormatV1 {
					back.Tombstone = e.Tombstone
				}
				if !entriesEqual(e, back) || !bytes.Equal(e.Key, back.Key) || !bytes.Equal(e.Value, back.Value) {
					t.Fatalf("not a fixed point:\n%+v\n%+v", e, back)
				}
			}
			if vs := r.ValidSize(); uint64(len(r.data)) < vs {
				t.Fatalf("ValidSize %d exceeds input %d", vs, len(r.data))
			}
		}
	})
}

// FuzzWALCounterStateRecord validates arbitrary CRDC payloads after they pass
// through the v3 WAL framing/checksum reader. Malformed counter bytes must be
// rejected without panicking; accepted states must encode/decode to a fixed
// point.
func FuzzWALCounterStateRecord(f *testing.F) {
	var seed counter.PNCounter
	_ = seed.Increment("node-a", 1)
	for _, payload := range [][]byte{
		seed.Encode(),
		{},
		[]byte("CRDC"),
		{0x43, 0x52, 0x44, 0x43, 2, 0, 0, 0, 0, 0, 0, 0},
	} {
		f.Add(payload)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		record := Entry{Op: OpCounterState, Key: []byte("fuzz"), Counter: payload}
		encoded, err := record.encode()
		if err != nil {
			t.Fatalf("encode outer WAL record: %v", err)
		}
		r := &Reader{data: encoded, format: FormatV3}
		got, ok := r.Next()
		if !ok || r.Corrupted() {
			t.Fatalf("outer v3 record rejected: ok=%v corrupt=%v", ok, r.Corrupted())
		}
		state, err := counter.DecodePNCounter(got.Counter)
		if err != nil {
			return
		}
		if !bytes.Equal(state.Encode(), payload) {
			t.Fatalf("accepted counter payload not canonical fixed point")
		}
	})
}

func buildWALSeeds() [][]byte {
	out := make([][]byte, 0, 8)
	mk := func(e Entry) []byte {
		enc, err := e.encode()
		if err != nil {
			panic(err)
		}
		return enc
	}
	out = append(out, []byte{})
	out = append(out, []byte{0x01})
	out = append(out, mk(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v"), Version: 1, Writer: 2}))
	out = append(out, mk(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v"), Version: 1, Writer: 2, Expiry: expiry.FromUnixMilli(999), Tombstone: true}))
	out = append(out, mk(Entry{Op: OpDel, Key: []byte("gone"), Version: 7, Writer: 11}))
	var p counter.PNCounter
	_ = p.Increment("node-a", 3)
	counterRecord, err := CounterEntry("counter", p, false)
	if err != nil {
		panic(err)
	}
	out = append(out, mk(counterRecord))
	removeRecord, err := CounterEntry("counter", p, true)
	if err != nil {
		panic(err)
	}
	out = append(out, mk(removeRecord))
	two := append(append([]byte{}, mk(Entry{Op: OpSet, Key: []byte("a"), Value: []byte("1")})...), mk(Entry{Op: OpSet, Key: []byte("b"), Value: []byte("2")})...)
	out = append(out, two)
	// Torn tail: valid record cut mid-checksum.
	out = append(out, two[:len(two)-3])
	// Bad op + bad tombstone byte.
	out = append(out, []byte{0x7F, 0, 0, 0, 0})
	return out
}

func entriesEqual(a, b Entry) bool {
	return a.Op == b.Op && a.Version == b.Version && a.Writer == b.Writer &&
		a.Expiry.UnixMilli() == b.Expiry.UnixMilli() && a.Tombstone == b.Tombstone && bytes.Equal(a.Counter, b.Counter)
}
