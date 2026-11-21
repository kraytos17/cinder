package protocol

import "testing"

// Wire codec budgets: small SET encode/decode stay at their current
// allocation counts. Allocs are deterministic; ns/op is compared
// separately with benchstat on the same host.
func TestAllocsCodecBounded(t *testing.T) {
	req := Request{Opcode: OpSet, Key: []byte("k"), Value: []byte("v")}
	if n := testing.AllocsPerRun(100, func() {
		_, _ = EncodeRequest(req, nil)
	}); n > 1 {
		t.Fatalf("encode allocs = %v, want <= 1", n)
	}
	frame, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	hdr, pay := frame[:FrameHeaderSize], frame[FrameHeaderSize:]
	if n := testing.AllocsPerRun(100, func() {
		_, _ = DecodeRequest(hdr, pay)
	}); n > 2 {
		t.Fatalf("decode allocs = %v, want <= 2", n)
	}
}
