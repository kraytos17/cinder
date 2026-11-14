package counter

import (
	"encoding/binary"
	"testing"

	"github.com/kraytos17/cinder/internal/codec"
)

// sideEntry is one verbatim (actor, count) record in a framed PNCounter
// body: unlike Encode it permits the non-canonical shapes the decoder
// must reject (zero counts, duplicate actors).
type sideEntry struct {
	actor string
	val   uint64
}

// rawSide frames a PNCounter body with the given Pos components verbatim
// (empty Neg), bypassing Encode so seeds can carry non-canonical records.
func rawSide(pos []sideEntry) []byte {
	buf := make([]byte, 0, 32)
	buf = codec.Append(buf, binary.BigEndian, MagicCRDC)
	buf = append(buf, KindPNCounter)
	buf = codec.AppendLen(buf, binary.BigEndian, len(pos))
	for _, e := range pos {
		buf = codec.AppendLen(buf, binary.BigEndian, len(e.actor))
		buf = append(buf, e.actor...)
		buf = codec.Append(buf, binary.BigEndian, e.val)
	}
	buf = codec.AppendLen(buf, binary.BigEndian, 0) // empty Neg
	return buf
}

// FuzzCounterCodec feeds arbitrary bytes to the CRDC/CAE1 decoders. Each
// must never panic; every accepted body must re-encode to bytes that
// decode to an equal counter (canonical fixed points).
func FuzzCounterCodec(f *testing.F) {
	var g GCounter
	_ = g.Increment("a", 1)
	var p PNCounter
	_ = p.Increment("n1", 10)
	_ = p.Decrement("n2", 3)
	seeds := [][]byte{
		{},
		{0x00},
		g.Encode(),
		p.Encode(),
		EncodeCounterBundle([]BundleEntry{{Key: "k", Counter: p}}),
		EncodeCounterBundle(nil),
		// Non-canonical records must decode as corrupt: a zero-count
		// component and a duplicated actor can never come from Encode,
		// so accepting either would break the fixed point.
		rawSide([]sideEntry{{"a", 0}}),
		rawSide([]sideEntry{{"a", 1}, {"a", 1}}),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if g, err := DecodeGCounter(data); err == nil {
			back, err := DecodeGCounter(g.Encode())
			if err != nil {
				t.Fatalf("gcounter re-decode failed")
			}
			if !back.Equal(g) {
				t.Fatal("gcounter not a fixed point")
			}
		}
		if p, err := DecodePNCounter(data); err == nil {
			enc := p.Encode()
			if len(enc) == 0 {
				t.Fatal("pncounter re-encode empty")
			}
			back, err := DecodePNCounter(enc)
			if err != nil {
				t.Fatalf("pncounter re-decode failed")
			}
			if !back.Equal(p) {
				t.Fatal("pncounter not a fixed point")
			}
		}
		if b, err := DecodeCounterBundle(data); err == nil {
			entries := make([]BundleEntry, len(b))
			copy(entries, b)
			back, err := DecodeCounterBundle(EncodeCounterBundle(entries))
			if err != nil {
				t.Fatalf("bundle re-decode failed")
			}
			if len(back) != len(b) {
				t.Fatal("bundle not a fixed point")
			}
		}
	})
}
