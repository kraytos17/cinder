package counter

import (
	"testing"
)

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
		EncodeGCounter(g),
		EncodePNCounter(p),
		EncodeCounterBundle([]BundleEntry{{Key: "k", Counter: p}}),
		EncodeCounterBundle(nil),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if g, err := DecodeGCounter(data); err == nil {
			back, err := DecodeGCounter(EncodeGCounter(g))
			if err != nil {
				t.Fatalf("gcounter re-decode failed")
			}
			if !back.Equal(g) {
				t.Fatal("gcounter not a fixed point")
			}
		}
		if p, err := DecodePNCounter(data); err == nil {
			enc := EncodePNCounter(p)
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
