package protocol

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
)

// The docs/protocol.md worked examples predate v6 (they show version byte
// 0x05); the payload bytes are unchanged, so the vectors below reuse them
// with the v6 header (0xC1 0x06).

// splitTestFrame separates a complete test frame into header and payload
// for the split-form decoders (production callers hold the two staging
// buffers separately, never a joined frame). Short inputs pass through
// as a short header so truncation cases keep failing the same way.
func splitTestFrame(frame []byte) (header, payload []byte) {
	if len(frame) < FrameHeaderSize {
		return frame, nil
	}
	return frame[:FrameHeaderSize], frame[FrameHeaderSize:]
}

// testPayload strips the header of a complete test frame for the
// payload-only response decoder; short inputs pass through so
// truncation cases keep failing the same way.
func testPayload(frame []byte) []byte {
	if len(frame) < FrameHeaderSize {
		return frame
	}
	return frame[FrameHeaderSize:]
}

func TestEncodeSetWorkedExample(t *testing.T) {
	req := Request{
		Opcode:         OpSet,
		Key:            []byte("foo"),
		Value:          []byte("bar"),
		Version:        2,
		WriterNodeHash: 66,
		TTL:            30 * time.Second,
	}
	got, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		0xC1, 0x06, 0x02, 0x00, 0x00, 0x00, 0x23,
		0x01,
		0x00, 0x00, 0x75, 0x30,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42,
		0x00, 0x00, 0x00, 0x03,
		'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x03,
		'b', 'a', 'r',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeRequest mismatch:\n got % X\nwant % X", got, want)
	}
}

func TestEncodeSetWithTraceWorkedExample(t *testing.T) {
	req := Request{
		Opcode:         OpSet,
		Key:            []byte("foo"),
		Value:          []byte("bar"),
		Version:        2,
		WriterNodeHash: 66,
		TTL:            30 * time.Second,
		TraceID:        7,
		SpanID:         10,
	}
	got, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	want := []byte{
		0xC1, 0x06, 0x02, 0x00, 0x00, 0x00, 0x33,
		0x05,
		0x00, 0x00, 0x75, 0x30,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42,
		0x00, 0x00, 0x00, 0x03,
		'f', 'o', 'o',
		0x00, 0x00, 0x00, 0x03,
		'b', 'a', 'r',
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeRequest mismatch:\n got % X\nwant % X", got, want)
	}
}

func TestEncodeResponseWorkedExample(t *testing.T) {
	res := Response{Status: StatusOK, Value: []byte("bar"), HasValue: true}
	got, err := EncodeResponse(res)
	if err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	want := []byte{
		0xC1, 0x06, 0x00, 0x00, 0x00, 0x00, 0x09,
		0x00,
		0x00,
		0x00, 0x00, 0x00, 0x01,
		'b', 'a', 'r',
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("EncodeResponse mismatch:\n got % X\nwant % X", got, want)
	}
}

func TestRoundTripRequests(t *testing.T) {
	auth := bytes.Repeat([]byte{0xAB}, AuthTokenLen)
	cases := []Request{
		{Opcode: OpPing},
		{Opcode: OpGet, Key: []byte("k")},
		{Opcode: OpGet, Key: []byte(""), Value: []byte("")},
		{Opcode: OpSet, Key: []byte("foo"), Value: []byte("bar"), Version: 2, WriterNodeHash: 66, TTL: 30 * time.Second},
		{Opcode: OpSet, Key: []byte("foo"), Value: []byte("bar"), TraceID: 7, SpanID: 10},
		{Opcode: OpDel, Key: []byte("gone"), Version: 9, WriterNodeHash: 1},
		{Opcode: OpGossip, Value: []byte("node1@127.0.0.1:7000:alive:3")},
		{
			Opcode: OpReplicate, Key: []byte("k"), Value: []byte("v"),
			Version: 3, WriterNodeHash: 5, ExpiresAt: expiry.FromUnixMilli(1_700_000_000_000), Tombstone: false,
			TraceID: 1, SpanID: 2,
		},
		{Opcode: OpReplicate, Key: []byte("k"), Tombstone: true, Version: 4, WriterNodeHash: 6, ExpiresAt: expiry.FromUnixMilli(1_700_000_000_000)},
		{Opcode: OpHint, Key: []byte("k"), Value: []byte("v"), Version: 1, WriterNodeHash: 1},
		{Opcode: OpGetVersioned, Key: []byte("k")},
		{Opcode: OpAntiEntropyDigest, Key: []byte("bucket-refinement-v3"), Value: []byte{0x41, 0x45, 0x42, 0x35}},
		{Opcode: OpAdminInfo},
		{Opcode: OpAdminShutdown},
		{Opcode: OpCounterAdjust, Key: []byte("c"), CounterDelta: 5},
		{Opcode: OpCounterAdjust, Key: []byte("c"), CounterDelta: 1 << 63, CounterDecrement: true},
		{Opcode: OpCounterMerge, Key: []byte("c"), Value: []byte("CRDC-state")},
		{Opcode: OpCounterGet, Key: []byte("c")},
		{Opcode: OpCounterAntiEntropy, Value: bytes.Repeat([]byte{0x01}, 64)},
	}
	for _, tc := range cases {
		for _, withAuth := range []bool{false, true} {
			var token []byte
			if withAuth {
				token = auth
			}
			frame, err := EncodeRequest(tc, token)
			if err != nil {
				t.Fatalf("%v auth=%v: encode: %v", tc.Opcode, withAuth, err)
			}
			// Frames must be self-delimiting: trailing garbage is ignored.
			padded := append(append([]byte(nil), frame...), 0xDE, 0xAD)
			dec, err := DecodeRequest(splitTestFrame(padded))
			if err != nil {
				t.Fatalf("%v auth=%v: decode: %v", tc.Opcode, withAuth, err)
			}
			if withAuth && !bytes.Equal(dec.AuthToken, auth) {
				t.Fatalf("%v: auth token lost", tc.Opcode)
			}
			if !withAuth && len(dec.AuthToken) != 0 {
				t.Fatalf("%v: unexpected auth token", tc.Opcode)
			}
			if !requestsEqual(dec.Req, tc) {
				t.Fatalf("%v auth=%v: round trip mismatch:\n got %+v\nwant %+v", tc.Opcode, withAuth, dec.Req, tc)
			}
			// decode->encode->decode is a fixed point.
			again, err := EncodeRequest(dec.Req, dec.AuthToken)
			if err != nil {
				t.Fatalf("%v: re-encode: %v", tc.Opcode, err)
			}
			if !bytes.Equal(again, frame) {
				t.Fatalf("%v: re-encode mismatch:\n got % X\nwant % X", tc.Opcode, again, frame)
			}
		}
	}
}

func requestsEqual(a, b Request) bool {
	if a.Opcode != b.Opcode || a.Version != b.Version || a.WriterNodeHash != b.WriterNodeHash ||
		a.TTL != b.TTL || a.ExpiresAt.UnixMilli() != b.ExpiresAt.UnixMilli() ||
		a.Tombstone != b.Tombstone || a.CounterDelta != b.CounterDelta ||
		a.CounterDecrement != b.CounterDecrement || a.TraceID != b.TraceID || a.SpanID != b.SpanID {
		return false
	}
	return bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

func TestRoundTripResponses(t *testing.T) {
	cases := []Response{
		{Status: StatusOK, Value: []byte("bar"), HasValue: true},
		{Status: StatusNotFound},
		{Status: StatusOK},                                    // no value, no flags
		{Status: StatusOK, Value: []byte(""), HasValue: true}, // present-but-empty
		{Status: StatusOK, Value: []byte("v"), HasValue: true, Version: 3, WriterNodeHash: 9, Tombstone: true, TraceID: 7, SpanID: 10},
		{Status: StatusOK, Value: []byte("v"), HasValue: true, ExpiresAt: expiry.FromUnixMilli(1_700_000_000_000)},
		{Status: StatusNotReady, Value: []byte("moved to node-2"), HasValue: true},
		{Status: StatusPermissionDenied},
		{Status: StatusCorruptData},
	}
	for _, tc := range cases {
		frame, err := EncodeResponse(tc)
		if err != nil {
			t.Fatalf("%v: encode: %v", tc.Status, err)
		}
		dec, err := DecodeResponse(testPayload(frame))
		if err != nil {
			t.Fatalf("%v: decode: %v", tc.Status, err)
		}
		if dec.Status != tc.Status || dec.HasValue != tc.HasValue ||
			!bytes.Equal(dec.Value, tc.Value) || dec.Version != tc.Version ||
			dec.WriterNodeHash != tc.WriterNodeHash ||
			dec.ExpiresAt.UnixMilli() != tc.ExpiresAt.UnixMilli() ||
			dec.Tombstone != tc.Tombstone || dec.TraceID != tc.TraceID || dec.SpanID != tc.SpanID {
			t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", dec, tc)
		}
		again, err := EncodeResponse(dec)
		if err != nil {
			t.Fatalf("%v: re-encode: %v", tc.Status, err)
		}
		if !bytes.Equal(again, frame) {
			t.Fatalf("%v: re-encode mismatch:\n got % X\nwant % X", tc.Status, again, frame)
		}
	}
}

func TestDecodeErrors(t *testing.T) {
	valid, err := EncodeRequest(Request{Opcode: OpSet, Key: []byte("k"), Value: []byte("v")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f []byte, fn func([]byte)) []byte {
		c := append([]byte(nil), f...)
		fn(c)
		return c
	}
	cases := []struct {
		name  string
		frame []byte
	}{
		{"empty", nil},
		{"short header", valid[:6]},
		{"bad magic", mutate(valid, func(b []byte) { b[0] = 0x00 })},
		{"bad version", mutate(valid, func(b []byte) { b[1] = 0x05 })},
		{"opcode zero", mutate(valid, func(b []byte) { b[2] = 0x00 })},
		{"opcode 21", mutate(valid, func(b []byte) { b[2] = 21 })},
		{"opcode 255", mutate(valid, func(b []byte) { b[2] = 0xFF })},
		{"truncated payload", valid[:len(valid)-2]},
		{"huge payload", []byte{Magic, Version, 0x01, 0xFF, 0xFF, 0xFF, 0xFF}},
		{"truncated key", valid[:20]},
	}
	for _, tc := range cases {
		if _, err := DecodeRequest(splitTestFrame(tc.frame)); err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
	}
	// Like the C++ decodeResponse, the response opcode byte is never
	// validated: any value decodes. Pin that parity explicitly.
	for _, op := range []byte{0x00, 0x02, 21, 0xFF} {
		resFrame, err := EncodeResponse(Response{Status: StatusOK, Value: []byte("v"), HasValue: true})
		if err != nil {
			t.Fatal(err)
		}
		resFrame[2] = op
		if _, err := DecodeResponse(testPayload(resFrame)); err != nil {
			t.Errorf("opcode byte %#02x: response decode should ignore opcode, got %v", op, err)
		}
	}
}

func TestDecodeExpiresAtOverflow(t *testing.T) {
	req := Request{
		Opcode: OpSet, Key: []byte("k"), ExpiresAt: expiry.FromUnixMilli(math.MaxInt64 / 1_000_000),
	}
	frame, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Max representable value decodes fine.
	if _, err := DecodeRequest(splitTestFrame(frame)); err != nil {
		t.Fatalf("max expires_at should decode: %v", err)
	}
	// Bump the u64 past the guard by editing the frame: flags(1) then 8 bytes.
	off := FrameHeaderSize + 1
	for i := range 8 {
		frame[off+i] = 0xFF
	}
	if _, err := DecodeRequest(splitTestFrame(frame)); err == nil {
		t.Fatal("expected expires_at overflow error")
	}
}

func TestDecodeCounterValidation(t *testing.T) {
	// decrement must be 0 or 1.
	req := Request{Opcode: OpCounterAdjust, Key: []byte("c"), CounterDelta: 3}
	frame, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] = 2
	if _, err := DecodeRequest(splitTestFrame(frame)); err == nil {
		t.Fatal("expected invalid counter adjust error")
	}
	// Counter opcodes reject LWW metadata.
	bad := Request{Opcode: OpCounterMerge, Key: []byte("c"), Value: []byte("s"), Version: 1}
	frame, err = EncodeRequest(bad, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRequest(splitTestFrame(frame)); err == nil {
		t.Fatal("expected counter LWW metadata error")
	}
	for _, op := range []Opcode{OpCounterAdjust, OpCounterMerge, OpCounterGet} {
		withTTL := Request{Opcode: op, Key: []byte("c"), TTL: time.Second}
		if op == OpCounterAdjust {
			withTTL.CounterDelta = 1
		}
		frame, err = EncodeRequest(withTTL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeRequest(splitTestFrame(frame)); err == nil {
			t.Fatalf("%v with TTL: expected error", op)
		}
	}
}

func TestEncodeAuthTokenLength(t *testing.T) {
	req := Request{Opcode: OpGet, Key: []byte("k")}
	if _, err := EncodeRequest(req, bytes.Repeat([]byte{1}, 31)); err == nil {
		t.Fatal("expected 31-byte token error")
	}
	if _, err := EncodeRequest(req, bytes.Repeat([]byte{1}, 33)); err == nil {
		t.Fatal("expected 33-byte token error")
	}
}

// The C++ decoder ignores trace/auth tails shorter than the full field.
// Pin that leniency so interop never diverges on truncated trailers.
func TestDecodeShortTrailerLenient(t *testing.T) {
	req := Request{Opcode: OpSet, Key: []byte("k"), Value: []byte("v")}
	frame, err := EncodeRequest(req, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame[FrameHeaderSize] |= byte(FlagHasTrace) // claim trace, append nothing
	dec, err := DecodeRequest(splitTestFrame(frame))
	if err != nil {
		t.Fatalf("short trace should be ignored: %v", err)
	}
	if dec.Req.TraceID != 0 {
		t.Fatal("short trace must not populate trace_id")
	}
	frame[FrameHeaderSize] |= byte(FlagHasAuth)
	dec, err = DecodeRequest(splitTestFrame(frame))
	if err != nil {
		t.Fatalf("short auth should be ignored: %v", err)
	}
	if len(dec.AuthToken) != 0 {
		t.Fatal("short auth must not populate token")
	}
}

func TestDecodeZeroTraceCanonicalizesSpan(t *testing.T) {
	// trace_id 0 with nonzero span_id decodes to span 0 (encoder fixed point).
	payload := []byte{
		byte(FlagHasTrace),
		0, 0, 0, 0, 0, 0, 0, 0, // version
		0, 0, 0, 0, 0, 0, 0, 0, // writer
		0, 0, 0, 1, 'k', // key
		0, 0, 0, 0, // empty value
		0, 0, 0, 0, 0, 0, 0, 0, // trace_id 0
		0, 0, 0, 0, 0, 0, 0, 9, // span_id 9
	}
	frame := append([]byte{Magic, Version, uint8(OpGet), 0, 0, 0, uint8(len(payload))}, payload...)
	dec, err := DecodeRequest(splitTestFrame(frame))
	if err != nil {
		t.Fatal(err)
	}
	if dec.Req.TraceID != 0 || dec.Req.SpanID != 0 {
		t.Fatalf("expected canonicalized trace, got %+v", dec.Req)
	}
}
