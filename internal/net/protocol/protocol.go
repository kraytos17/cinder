// Package protocol implements the Cinder wire protocol v6 framing and the
// request/response codecs.
//
// Every frame starts with a 7-byte header (magic 0xC1, version 0x06, opcode,
// big-endian payload length), followed by a payload whose multi-byte integers
// are also big-endian. The total frame is bounded by MaxMessageSize (64 MiB).
// The normative layout and compatibility requirements are in
// docs/protocol.md is the normative layout; golden-vector, interoperability,
// and fuzz tests cover this implementation.
package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/kraytos17/cinder/internal/codec"
	"github.com/kraytos17/cinder/internal/expiry"
)

// Wire constants for frame encoding and size limits. The opcode and field
// layout is exercised by protocol_test.go and the codec fuzz tests.
const (
	Magic           = 0xC1
	Version         = 0x06
	FrameHeaderSize = 7
	MaxMessageSize  = 67_108_864
	MaxPayloadSize  = MaxMessageSize - FrameHeaderSize

	// AuthTokenLen is the HMAC-SHA256 token size; anything else is rejected.
	AuthTokenLen = 32

	// MaxExpiresAtMs guards the milliseconds-to-nanoseconds conversion on
	// decode: values above it would overflow int64 nanoseconds.
	MaxExpiresAtMs = int64(^uint64(0)>>1) / 1_000_000
)

// RequestFlags is the bitmask stored in the first request payload byte.
type RequestFlags uint8

// Request flag bits (first payload byte).
const (
	FlagHasTTL       RequestFlags = 1 << 0
	FlagHasExpiresAt RequestFlags = 1 << 1
	FlagHasTrace     RequestFlags = 1 << 2
	FlagHasAuth      RequestFlags = 1 << 3
	FlagTombstone    RequestFlags = 1 << 4
)

// ResponseFlags is the bitmask stored in the response payload.
type ResponseFlags uint8

// Response flag bits. Bit 0 carries version metadata; bit 1 indicates an
// expiry, bit 2 trace context, and bit 4 a tombstone.
const (
	RespFlagVersionMeta ResponseFlags = 1 << 0
	RespFlagExpiresAt   ResponseFlags = 1 << 1
	RespFlagTrace       ResponseFlags = 1 << 2
	RespFlagTombstone   ResponseFlags = 1 << 4
)

// Opcode is a v6 request opcode (1-20). Response frames always carry 0x00.
type Opcode uint8

// Request opcodes for the v6 protocol. The wire numbers are stable; opcode
// handling is implemented by the TCP server and client packages.
const (
	OpGet                Opcode = 1
	OpSet                Opcode = 2
	OpDel                Opcode = 3
	OpPing               Opcode = 4
	OpGossip             Opcode = 5
	OpReplicate          Opcode = 6
	OpHint               Opcode = 7
	OpGetVersioned       Opcode = 8
	OpAntiEntropyDigest  Opcode = 9
	OpAntiEntropySync    Opcode = 10
	OpAdminInfo          Opcode = 11
	OpAdminCluster       Opcode = 12
	OpAdminRing          Opcode = 13
	OpAdminCompact       Opcode = 14
	OpAdminConfigReload  Opcode = 15
	OpAdminShutdown      Opcode = 16
	OpCounterAdjust      Opcode = 17
	OpCounterMerge       Opcode = 18
	OpCounterGet         Opcode = 19
	OpCounterAntiEntropy Opcode = 20
)

// String returns the stable uppercase name used in logs and diagnostics.
func (o Opcode) String() string {
	switch o {
	case OpGet:
		return "GET"
	case OpSet:
		return "SET"
	case OpDel:
		return "DEL"
	case OpPing:
		return "PING"
	case OpGossip:
		return "GOSSIP"
	case OpReplicate:
		return "REPLICATE"
	case OpHint:
		return "HINT"
	case OpGetVersioned:
		return "GET_VERSIONED"
	case OpAntiEntropyDigest:
		return "ANTI_ENTROPY_DIGEST"
	case OpAntiEntropySync:
		return "ANTI_ENTROPY_SYNC"
	case OpAdminInfo:
		return "ADMIN_INFO"
	case OpAdminCluster:
		return "ADMIN_CLUSTER"
	case OpAdminRing:
		return "ADMIN_RING"
	case OpAdminCompact:
		return "ADMIN_COMPACT"
	case OpAdminConfigReload:
		return "ADMIN_CONFIG_RELOAD"
	case OpAdminShutdown:
		return "ADMIN_SHUTDOWN"
	case OpCounterAdjust:
		return "COUNTER_ADJUST"
	case OpCounterMerge:
		return "COUNTER_MERGE"
	case OpCounterGet:
		return "COUNTER_GET"
	case OpCounterAntiEntropy:
		return "COUNTER_ANTI_ENTROPY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(o))
	}
}

// Status is a v6 response status code (0-10).
type Status uint8

// Response status codes for the v6 binary protocol. Members are
// self-describing numeric codes; semantics live in docs/protocol.md.
const (
	StatusOK               Status = 0
	StatusNotFound         Status = 1
	StatusCapacityExceeded Status = 2
	StatusInvalidArgument  Status = 3
	StatusTTLExpired       Status = 4
	StatusNotSupported     Status = 5
	StatusInternalError    Status = 6
	StatusTimeout          Status = 7
	StatusNotReady         Status = 8
	StatusPermissionDenied Status = 9
	StatusCorruptData      Status = 10
)

// String returns the stable status name used in logs and test failures.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "OK"
	case StatusNotFound:
		return "NotFound"
	case StatusCapacityExceeded:
		return "CapacityExceeded"
	case StatusInvalidArgument:
		return "InvalidArgument"
	case StatusTTLExpired:
		return "TtlExpired"
	case StatusNotSupported:
		return "NotSupported"
	case StatusInternalError:
		return "InternalError"
	case StatusTimeout:
		return "Timeout"
	case StatusNotReady:
		return "NotReady"
	case StatusPermissionDenied:
		return "PermissionDenied"
	case StatusCorruptData:
		return "CorruptData"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(s))
	}
}

// Error is a codec failure. Codec errors carry StatusInvalidArgument; Msg
// identifies the validation failure for logs and diagnostics.
type Error struct {
	Code Status
	Msg  string
}

func (e *Error) Error() string { return "protocol: " + e.Msg }

func errInvalid(msg string) error { return &Error{Code: StatusInvalidArgument, Msg: msg} }

// Request is a decoded v6 request. TTL is a relative client-side duration;
// ExpiresAt is an absolute deadline used when forwarding replicated data.
// They are normally used separately, but the decoder accepts both fields.
// A zero TTL means no relative expiry. The wire flags are derived from the
// values, so callers cannot desynchronize a presence bit from its payload.
type Request struct {
	Opcode         Opcode
	Key            []byte
	Value          []byte
	Version        uint64
	WriterNodeHash uint64
	TTL            time.Duration
	ExpiresAt      expiry.Expiry
	Tombstone      bool
	// CounterAdjust only. A zero delta is a no-op.
	CounterDelta     uint64
	CounterDecrement bool
	TraceID          uint64
	SpanID           uint64
	// AuthenticatedNode is populated by the TCP server after validating a
	// node token. It is local metadata and is never encoded on the wire.
	AuthenticatedNode string
}

// DecodedRequest pairs a request with its transport credential. The auth
// token is opaque here; verification happens at the connection layer.
type DecodedRequest struct {
	Req       Request
	AuthToken []byte
}

// Response is a decoded v6 response. HasValue distinguishes "present but
// empty" (value="") from absent, which the wire encodes as has_val 1 vs 0.
type Response struct {
	Status         Status
	Value          []byte
	HasValue       bool
	Version        uint64
	WriterNodeHash uint64
	ExpiresAt      expiry.Expiry
	Tombstone      bool
	TraceID        uint64
	SpanID         uint64
}

// payloadAcc accumulates payload size with overflow-safe saturation so
// both encoders enforce the same frame-size limit.
type payloadAcc struct {
	size uint64
	ok   bool
}

func (a *payloadAcc) add(n uint64) {
	if !a.ok || a.size > MaxPayloadSize || n > MaxPayloadSize-a.size {
		a.ok = false
		return
	}

	a.size += n
	a.ok = true
}

// splitFrame validates a header/payload pair without requiring the joined
// frame. Callers that already hold the two staging buffers (server,
// client) skip the join alloc plus two copies per RPC.
func splitFrame(header, payload []byte) (Opcode, []byte, error) {
	if len(header) < FrameHeaderSize {
		return 0, nil, errInvalid("frame too small")
	}
	if header[0] != Magic {
		return 0, nil, errInvalid("bad magic")
	}
	if header[1] != Version {
		return 0, nil, errInvalid("bad version")
	}

	payloadLen := binary.BigEndian.Uint32(header[3:7])
	if payloadLen > MaxPayloadSize {
		return 0, nil, errInvalid("payload too large")
	}
	if uint64(len(payload)) < uint64(payloadLen) {
		return 0, nil, errInvalid("truncated frame")
	}
	return Opcode(header[2]), payload[:payloadLen], nil
}

func EncodeRequest(req Request, authToken []byte) ([]byte, error) {
	return EncodeRequestInto(req, authToken, nil)
}

// EncodeRequestInto is EncodeRequest reusing buf's capacity across calls.
func EncodeRequestInto(req Request, authToken []byte, buf []byte) ([]byte, error) {
	var acc payloadAcc
	acc.ok = true
	acc.add(4)
	acc.add(uint64(len(req.Key)))
	acc.add(4)
	acc.add(uint64(len(req.Value)))
	acc.add(8 + 8)
	if req.TTL > 0 {
		acc.add(4)
	}

	acc.add(1)
	if !req.ExpiresAt.IsNone() {
		acc.add(8)
	}
	if req.Opcode == OpCounterAdjust {
		acc.add(8)
		acc.add(1)
	}

	hasTrace := req.TraceID != 0
	if hasTrace {
		acc.add(8 + 8)
	}

	hasAuth := len(authToken) != 0
	if hasAuth {
		if len(authToken) != AuthTokenLen {
			return nil, errInvalid("auth token must be 32 bytes")
		}
		acc.add(AuthTokenLen)
	}
	if !acc.ok {
		return nil, errInvalid("message too large")
	}
	if acc.size > MaxPayloadSize {
		return nil, errInvalid("message too large")
	}

	w := codec.NewWriter(binary.BigEndian, buf[:0:cap(buf)])
	// Reserve header + payload up front so the frame is one allocation.
	need := FrameHeaderSize + int(acc.size)
	if cap(w.Out()) < need {
		w.Reset(make([]byte, 0, need))
	}

	w.U8(Magic)
	w.U8(Version)
	w.U8(uint8(req.Opcode))
	w.U32(uint32(acc.size))

	var flags RequestFlags
	if req.TTL > 0 {
		flags |= FlagHasTTL
	}
	if !req.ExpiresAt.IsNone() {
		flags |= FlagHasExpiresAt
	}
	if req.Tombstone {
		flags |= FlagTombstone
	}
	if hasTrace {
		flags |= FlagHasTrace
	}
	if hasAuth {
		flags |= FlagHasAuth
	}

	w.U8(uint8(flags))
	if req.TTL > 0 {
		w.U32(uint32(req.TTL.Milliseconds()))
	}
	if !req.ExpiresAt.IsNone() {
		w.U64(uint64(req.ExpiresAt.UnixMilli()))
	}

	w.U64(req.Version)
	w.U64(req.WriterNodeHash)
	w.U32(uint32(len(req.Key)))
	w.Bytes(req.Key)
	w.U32(uint32(len(req.Value)))
	w.Bytes(req.Value)
	if req.Opcode == OpCounterAdjust {
		w.U64(req.CounterDelta)
		if req.CounterDecrement {
			w.U8(1)
		} else {
			w.U8(0)
		}
	}
	if hasTrace {
		w.U64(req.TraceID)
		w.U64(req.SpanID)
	}
	if hasAuth {
		w.Bytes(authToken)
	}
	return w.Out(), nil
}

// DecodeRequest parses a v6 request from separate header and payload
// buffers. Optional trace and authentication tails shorter than their full
// fields are ignored.
func DecodeRequest(header, payload []byte) (DecodedRequest, error) {
	var out DecodedRequest
	rawOpcode, payload, err := splitFrame(header, payload)
	if err != nil {
		return out, err
	}
	if rawOpcode < OpGet || rawOpcode > OpCounterAntiEntropy {
		return out, errInvalid("unknown opcode")
	}

	req := &out.Req
	req.Opcode = rawOpcode
	r := codec.NewReader(binary.BigEndian, payload)
	rawFlags, ok := r.U8()
	if !ok {
		return out, errInvalid("truncated frame")
	}

	flags := RequestFlags(rawFlags)
	if flags&FlagHasTTL != 0 {
		ttl, ok := r.U32()
		if !ok {
			return out, errInvalid("truncated frame")
		}
		req.TTL = time.Duration(ttl) * time.Millisecond
	}
	if flags&FlagHasExpiresAt != 0 {
		ms, ok := r.U64()
		if !ok {
			return out, errInvalid("truncated frame")
		}
		if ms > uint64(MaxExpiresAtMs) {
			return out, errInvalid("expires_at overflow")
		}
		req.ExpiresAt = expiry.FromUnixMilli(int64(ms))
	}

	req.Tombstone = flags&FlagTombstone != 0
	version, ok := r.U64()
	if !ok {
		return out, errInvalid("truncated frame")
	}

	req.Version = version
	writerHash, ok := r.U64()
	if !ok {
		return out, errInvalid("truncated frame")
	}

	req.WriterNodeHash = writerHash
	keyLen, ok := r.U32()
	if !ok {
		return out, errInvalid("truncated frame")
	}
	if keyLen > 0 {
		key, ok := r.Bytes(int(keyLen))
		if !ok {
			return out, errInvalid("truncated frame")
		}
		req.Key = append([]byte(nil), key...)
	}

	valLen, ok := r.U32()
	if !ok {
		return out, errInvalid("truncated frame")
	}
	if valLen > 0 {
		val, ok := r.Bytes(int(valLen))
		if !ok {
			return out, errInvalid("truncated frame")
		}
		req.Value = append([]byte(nil), val...)
	}
	if req.Opcode == OpCounterAdjust {
		delta, ok := r.U64()
		if !ok {
			return out, errInvalid("invalid counter adjust")
		}

		decr, ok := r.U8()
		if !ok || decr > 1 {
			return out, errInvalid("invalid counter adjust")
		}

		req.CounterDelta = delta
		req.CounterDecrement = decr != 0
	}
	if req.Opcode == OpCounterAdjust || req.Opcode == OpCounterMerge || req.Opcode == OpCounterGet {
		if req.TTL > 0 || !req.ExpiresAt.IsNone() || req.Tombstone || req.Version != 0 || req.WriterNodeHash != 0 {
			return out, errInvalid("counter opcode has LWW metadata")
		}
	}
	// Trace context: optional trailing data, ignored when truncated.
	if flags&FlagHasTrace != 0 && r.Remaining() >= 16 {
		tid, _ := r.U64()
		sid, _ := r.U64()
		req.TraceID = tid
		// The encoder gates span_id on trace_id != 0, keeping round trips
		// stable when a frame contains a zero trace identifier.
		if tid != 0 {
			req.SpanID = sid
		}
	}
	if flags&FlagHasAuth != 0 && r.Remaining() >= AuthTokenLen {
		token, _ := r.Bytes(AuthTokenLen)
		out.AuthToken = append([]byte(nil), token...)
	}
	return out, nil
}

// EncodeResponse serializes res into a complete v6 frame (opcode 0x00).
func EncodeResponse(res Response) ([]byte, error) {
	return EncodeResponseInto(res, nil)
}

// EncodeResponseInto is EncodeResponse reusing buf's capacity across calls.
func EncodeResponseInto(res Response, buf []byte) ([]byte, error) {
	var flags ResponseFlags
	hasVersionMeta := res.Version != 0 || res.WriterNodeHash != 0
	if hasVersionMeta {
		flags |= RespFlagVersionMeta
	}
	if !res.ExpiresAt.IsNone() {
		flags |= RespFlagExpiresAt
	}
	if res.Tombstone {
		flags |= RespFlagTombstone
	}

	hasTrace := res.TraceID != 0
	if hasTrace {
		flags |= RespFlagTrace
	}

	var acc payloadAcc
	acc.ok = true
	acc.add(1 + 1 + 4) // status, flags, has_val
	if hasVersionMeta {
		acc.add(8 + 8)
	}
	if !res.ExpiresAt.IsNone() {
		acc.add(8)
	}
	if res.HasValue {
		acc.add(uint64(len(res.Value)))
	}
	if hasTrace {
		acc.add(8 + 8)
	}
	if !acc.ok || acc.size > MaxPayloadSize {
		return nil, errInvalid("message too large")
	}

	w := codec.NewWriter(binary.BigEndian, buf[:0:cap(buf)])
	need := FrameHeaderSize + int(acc.size)
	if cap(w.Out()) < need {
		w.Reset(make([]byte, 0, need))
	}

	w.U8(Magic)
	w.U8(Version)
	w.U8(0) // response opcode unused
	w.U32(uint32(acc.size))

	w.U8(uint8(res.Status))
	w.U8(uint8(flags))
	if hasVersionMeta {
		w.U64(res.Version)
		w.U64(res.WriterNodeHash)
	}
	if !res.ExpiresAt.IsNone() {
		w.U64(uint64(res.ExpiresAt.UnixMilli()))
	}
	if res.HasValue {
		w.U32(1)
	} else {
		w.U32(0)
	}

	w.Bytes(res.Value)
	if hasTrace {
		w.U64(res.TraceID)
		w.U64(res.SpanID)
	}
	return w.Out(), nil
}

// DecodeResponse parses one v6 response payload. The frame header carries
// no response fields (opcode is reserved as zero); callers validate it from
// the staging buffer. The flags byte is optional for older frames. A
// present-but-empty value remains distinguishable from an absent value.
func DecodeResponse(payload []byte) (Response, error) {
	var res Response
	r := codec.NewReader(binary.BigEndian, payload)
	status, ok := r.U8()
	if !ok {
		return res, errInvalid("truncated frame")
	}

	res.Status = Status(status)
	var flags ResponseFlags
	if r.Remaining() > 0 {
		rawFlags, readOK := r.U8()
		ok = readOK
		if !ok {
			return res, errInvalid("truncated frame")
		}
		flags = ResponseFlags(rawFlags)
	}

	res.Tombstone = flags&RespFlagTombstone != 0
	if flags&RespFlagVersionMeta != 0 {
		version, ok := r.U64()
		if !ok {
			return res, errInvalid("truncated frame")
		}

		res.Version = version
		writerHash, ok := r.U64()
		if !ok {
			return res, errInvalid("truncated frame")
		}
		res.WriterNodeHash = writerHash
	}
	if flags&RespFlagExpiresAt != 0 {
		ms, ok := r.U64()
		if !ok {
			return res, errInvalid("truncated frame")
		}
		if ms > uint64(MaxExpiresAtMs) {
			return res, errInvalid("expires_at overflow")
		}
		res.ExpiresAt = expiry.FromUnixMilli(int64(ms))
	}

	hasVal, ok := r.U32()
	if !ok {
		return res, errInvalid("truncated frame")
	}
	if hasVal != 0 {
		// Reserve the trace bytes only when the complete 16-byte field is
		// present; otherwise the value length is 0 but still present.
		var traceSize int
		if flags&RespFlagTrace != 0 {
			traceSize = 16
		}

		valLen := 0
		if r.Remaining() > traceSize {
			valLen = r.Remaining() - traceSize
		}

		val, ok := r.Bytes(valLen)
		if !ok {
			return res, errInvalid("truncated frame")
		}

		res.HasValue = true
		res.Value = append([]byte(nil), val...)
	}
	if flags&RespFlagTrace != 0 && r.Remaining() >= 16 {
		tid, _ := r.U64()
		sid, _ := r.U64()
		res.TraceID = tid
		if tid != 0 {
			res.SpanID = sid
		}
	}
	return res, nil
}
