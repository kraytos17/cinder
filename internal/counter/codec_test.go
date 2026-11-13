package counter

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// Hand-computed oracle: GCounter{a:1} must encode to exactly these bytes
// (big-endian, byte-identical with the C++ BufferWriter output).
func TestEncodeGCounterOracle(t *testing.T) {
	var g GCounter
	if err := g.Increment("a", 1); err != nil {
		t.Fatal(err)
	}
	got := g.Encode()
	var want []byte
	want = binary.BigEndian.AppendUint32(want, MagicCRDC)
	want = append(want, KindGCounter)
	want = binary.BigEndian.AppendUint32(want, 1) // one component
	want = binary.BigEndian.AppendUint32(want, 1) // actor len
	want = append(want, 'a')
	want = binary.BigEndian.AppendUint64(want, 1)
	if !bytes.Equal(got, want) {
		t.Fatalf("encode = %x, want %x", got, want)
	}
	back, err := DecodeGCounter(got)
	if err != nil {
		t.Fatal(err)
	}
	if !back.Equal(g) {
		t.Fatal("round trip mismatch")
	}
}

func TestCodecRoundTrip(t *testing.T) {
	var p PNCounter
	_ = p.Increment("node1", 10)
	_ = p.Increment("node2", 5)
	_ = p.Decrement("node1", 3)
	enc := p.Encode()
	if len(enc) == 0 {
		t.Fatal("empty encoding")
	}
	back, err := DecodePNCounter(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !back.Equal(p) {
		t.Fatal("PNCounter round trip mismatch")
	}
	// GCounter body must not decode as PNCounter and vice versa.
	var g GCounter
	_ = g.Increment("a", 1)
	if _, err := DecodePNCounter(g.Encode()); !errors.Is(err, ErrDecode) {
		t.Fatalf("cross-kind decode err = %v, want ErrDecode", err)
	}
	if _, err := DecodeGCounter(enc); !errors.Is(err, ErrDecode) {
		t.Fatalf("cross-kind decode err = %v, want ErrDecode", err)
	}
}

func TestEncodedSizeMatchesEncoding(t *testing.T) {
	var p PNCounter
	if err := p.Increment("node-a", 3); err != nil {
		t.Fatal(err)
	}
	if err := p.Decrement("node-with-long-name", 5); err != nil {
		t.Fatal(err)
	}
	if got, want := p.EncodedSize(), len(p.Encode()); got != want {
		t.Fatalf("PNCounter EncodedSize=%d, len(Encode)=%d", got, want)
	}
	if got, want := p.Pos.EncodedSize(), len(p.Pos.Encode()); got != want {
		t.Fatalf("GCounter EncodedSize=%d, len(Encode)=%d", got, want)
	}
}

func TestEncodeEncodedCounterBundleMatchesCounterBundle(t *testing.T) {
	entries := make([]BundleEntry, 0, 3)
	encoded := make([]EncodedBundleEntry, 0, 3)
	for _, key := range []string{"a", "b", "c"} {
		var state PNCounter
		if err := state.Increment("node-a", uint64(len(key))); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, BundleEntry{Key: key, Counter: state})
		encoded = append(encoded, EncodedBundleEntry{Key: key, State: state.Encode()})
	}
	got := EncodeEncodedCounterBundle(encoded)
	want := EncodeCounterBundle(entries)
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded bundle mismatch:\n got %x\nwant %x", got, want)
	}
	if _, err := DecodeCounterBundle(got); err != nil {
		t.Fatalf("decode encoded bundle: %v", err)
	}
}

func TestCodecRejects(t *testing.T) {
	var g GCounter
	_ = g.Increment("a", 1)
	good := g.Encode()
	cases := map[string][]byte{
		"empty":       {},
		"truncated":   good[:len(good)-1],
		"trailing":    append(append([]byte(nil), good...), 0x00),
		"bad magic":   append([]byte{0, 0, 0, 0}, good[4:]...),
		"lying count": append(append(append([]byte(nil), good[:5]...), 0xFF, 0xFF, 0xFF, 0xFF), good[9:]...),
		"bad kind":    append(append([]byte(nil), good[:4]...), 0x7F),
	}
	for name, data := range cases {
		if _, err := DecodeGCounter(data); !errors.Is(err, ErrDecode) {
			t.Fatalf("%s: err = %v, want ErrDecode", name, err)
		}
		if _, err := DecodePNCounter(data); !errors.Is(err, ErrDecode) {
			t.Fatalf("%s (pn): err = %v, want ErrDecode", name, err)
		}
	}
}

func TestBundleRoundTripAndOrder(t *testing.T) {
	var a, b PNCounter
	_ = a.Increment("n1", 4)
	_ = b.Decrement("n2", 2)
	entries := []BundleEntry{{Key: "k1", Counter: a}, {Key: "k2", Counter: b}}
	enc := EncodeCounterBundle(entries)
	if len(enc) == 0 {
		t.Fatal("empty bundle")
	}
	back, err := DecodeCounterBundle(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 2 || back[0].Key != "k1" || back[1].Key != "k2" {
		t.Fatalf("keys = %v", back)
	}
	if !back[0].Counter.Equal(a) || !back[1].Counter.Equal(b) {
		t.Fatal("bundle round trip mismatch")
	}
	// Empty selection still encodes the header.
	empty := EncodeCounterBundle(nil)
	back, err = DecodeCounterBundle(empty)
	if err != nil || len(back) != 0 {
		t.Fatalf("empty bundle = %v %v", back, err)
	}
}

func TestBundleRejectsUnsorted(t *testing.T) {
	var a PNCounter
	_ = a.Increment("n", 1)
	// Hand-assemble an unsorted bundle: keys k2 then k1.
	var buf []byte
	buf = binary.BigEndian.AppendUint32(buf, MagicCAE1)
	buf = binary.BigEndian.AppendUint32(buf, 2)
	for _, k := range []string{"k2", "k1"} {
		st := a.Encode()
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(st)))
		buf = append(buf, st...)
	}
	if _, err := DecodeCounterBundle(buf); !errors.Is(err, ErrDecode) {
		t.Fatalf("unsorted err = %v, want ErrDecode", err)
	}
	// Duplicate keys also fail the strictly-increasing rule.
	dup := EncodeCounterBundle([]BundleEntry{{Key: "k", Counter: a}, {Key: "k", Counter: a}})
	if _, err := DecodeCounterBundle(dup); !errors.Is(err, ErrDecode) {
		t.Fatalf("duplicate err = %v, want ErrDecode", err)
	}
}
