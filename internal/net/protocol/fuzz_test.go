package protocol

import (
	"bytes"
	"testing"
)

// Seed corpus frames: valid representatives plus truncation edges. `go test`
// runs these as unit tests; `go test -fuzz=.` explores from them.
var fuzzSeeds = [][]byte{
	{Magic, Version, uint8(OpPing), 0, 0, 0, 1, 0},
	mustEncodeSeed(Request{Opcode: OpSet, Key: []byte("foo"), Value: []byte("bar")}),
	mustEncodeSeed(Request{Opcode: OpGet, Key: []byte("k")}),
	mustEncodeSeedResp(Response{Status: StatusOK, Value: []byte("v"), HasValue: true}),
	{Magic, Version, uint8(OpGetVersioned), 0, 0, 0, 0}, // empty payload
	{Magic}, // truncated header
	{0x00, Version, uint8(OpGet), 0, 0, 0, 1, 0}, // bad magic
}

func mustEncodeSeed(req Request) []byte {
	f, err := EncodeRequest(req, nil)
	if err != nil {
		panic(err)
	}
	return f
}

func mustEncodeSeedResp(res Response) []byte {
	f, err := EncodeResponse(res)
	if err != nil {
		panic(err)
	}
	return f
}

// FuzzDecodeRequest feeds arbitrary bytes to the request decoder. It must
// never panic; every accepted frame must re-encode to an identical frame
// (decode->encode->decode fixed point, mirroring the C++ invariant).
func FuzzDecodeRequest(f *testing.F) {
	for _, seed := range fuzzSeeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		dec, err := DecodeRequest(splitTestFrame(frame))
		if err != nil {
			return
		}
		again, err := EncodeRequest(dec.Req, dec.AuthToken)
		if err != nil {
			t.Fatalf("re-encode of accepted frame failed: %v", err)
		}
		// The re-encoded frame must decode to the same request: the codec
		// is canonical on its accepted domain.
		dec2, err := DecodeRequest(splitTestFrame(again))
		if err != nil {
			t.Fatalf("re-decode of re-encoded frame failed: %v", err)
		}
		if !requestsEqual(dec.Req, dec2.Req) || !bytes.Equal(dec.AuthToken, dec2.AuthToken) {
			t.Fatalf("not a fixed point:\n%+v\n%+v", dec, dec2)
		}
	})
}

// FuzzDecodeResponse applies the same fixed-point property to responses.
func FuzzDecodeResponse(f *testing.F) {
	for _, seed := range append(fuzzSeeds, mustEncodeSeedResp(Response{Status: StatusNotFound})) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		dec, err := DecodeResponse(testPayload(frame))
		if err != nil {
			return
		}
		again, err := EncodeResponse(dec)
		if err != nil {
			t.Fatalf("re-encode of accepted response failed: %v", err)
		}
		dec2, err := DecodeResponse(testPayload(again))
		if err != nil {
			t.Fatalf("re-decode of re-encoded response failed: %v", err)
		}
		if dec.Status != dec2.Status || dec.HasValue != dec2.HasValue ||
			!bytes.Equal(dec.Value, dec2.Value) || dec.Version != dec2.Version ||
			dec.WriterNodeHash != dec2.WriterNodeHash ||
			dec.ExpiresAt.UnixMilli() != dec2.ExpiresAt.UnixMilli() ||
			dec.Tombstone != dec2.Tombstone || dec.TraceID != dec2.TraceID || dec.SpanID != dec2.SpanID {
			t.Fatalf("not a fixed point:\n%+v\n%+v", dec, dec2)
		}
	})
}
