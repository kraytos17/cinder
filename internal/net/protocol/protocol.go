// Package protocol implements the Cinder wire protocol v6 framing and the
// request/response codecs.
//
// This is a byte-identical port of include/cinder/net/protocol.hpp and
// src/net/protocol.cpp. docs/protocol.md is the specification; when this
// package and the C++ implementation disagree, the C++ side wins until the
// C++ tree is retired, and docs/protocol.md is updated to match.
//
// Wire summary: every message starts with a 7-byte header (magic 0xC1,
// version 0x06, opcode, big-endian payload length), followed by the payload.
// All multi-byte integers on the wire are big-endian. The total message is
// bounded by MaxMessageSize (64 MiB).
package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"

	"github.com/kraytos17/cinder/internal/codec"
)

// Wire constants. Must stay in sync with the C++ K_* constants.
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

// Request flag bits (first payload byte).
const (
	FlagHasTTL       = 1 << 0
	FlagHasExpiresAt = 1 << 1
	FlagHasTrace     = 1 << 2
	FlagHasAuth      = 1 << 3
	FlagTombstone    = 1 << 4
)

// Response flag bits. Bit 0 carries version metadata; the rest mirror the
// request flags.
const (
	RespFlagVersionMeta = 1 << 0
	RespFlagExpiresAt   = 1 << 1
	RespFlagTrace       = 1 << 2
	RespFlagTombstone   = 1 << 4
)

// Opcode is a v6 request opcode (1-20). Response frames always carry 0x00.
type Opcode uint8

// Request opcodes for the v6 binary protocol (1-20). Members are
// self-describing numeric codes; semantics live in docs/protocol.md.
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

// String mirrors the C++ opcode names for logs and test failures.
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

// String mirrors cinder::Error::toString for logs and test failures.
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

// Error is a codec failure. Like the C++ side, every codec error carries
// StatusInvalidArgument; the message matches the C++ error text so
// cross-language debugging stays greppable.
type Error struct {
	Code Status
	Msg  string
}

func (e *Error) Error() string { return "protocol: " + e.Msg }

func errInvalid(msg string) error { return &Error{Code: StatusInvalidArgument, Msg: msg} }

// Request is a decoded v6 request. TTL (relative, client form) and
// ExpiresAt (absolute, replication form) are mutually exclusive in
// practice but the codec accepts both, exactly like the C++ encoder.
// A zero TTL means none (the old HasTTL bool was redundant: the wire
// bit derives from TTL > 0).
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

// payloadAcc accumulates the payload size with overflow-safe saturation,
// mirroring C++ addPayloadSize: both encoders share one budget invariant.
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

func parseFrame(frame []byte) (Opcode, []byte, error) {
	if len(frame) < FrameHeaderSize {
		return 0, nil, errInvalid("frame too small")
	}
	if frame[0] != Magic {
		return 0, nil, errInvalid("bad magic")
	}
	if frame[1] != Version {
		return 0, nil, errInvalid("bad version")
	}
	payloadLen := binary.BigEndian.Uint32(frame[3:7])
	if payloadLen > MaxPayloadSize {
		return 0, nil, errInvalid("payload too large")
	}
	if uint64(len(frame)-FrameHeaderSize) < uint64(payloadLen) {
		return 0, nil, errInvalid("truncated frame")
	}
	return Opcode(frame[2]), frame[FrameHeaderSize : FrameHeaderSize+payloadLen], nil
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

	var flags uint8
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
	w.U8(flags)
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

// DecodeRequest parses one complete v6 frame. It mirrors the C++ decoder's
// lenient tail: trace/auth bytes gated by their flag bits but shorter than
// the full field are silently ignored rather than rejected.
func DecodeRequest(frame []byte) (DecodedRequest, error) {
	var out DecodedRequest
	rawOpcode, payload, err := parseFrame(frame)
	if err != nil {
		return out, err
	}
	if rawOpcode < OpGet || rawOpcode > OpCounterAntiEntropy {
		return out, errInvalid("unknown opcode")
	}
	req := &out.Req
	req.Opcode = rawOpcode
	r := codec.NewReader(binary.BigEndian, payload)
	flags, ok := r.U8()
	if !ok {
		return out, errInvalid("truncated frame")
	}
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
		// Mirror the encoder (which gates span on trace_id != 0): a zero
		// trace carries no span, so decode->encode->decode is a fixed point.
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
	var flags uint8
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
	w.U8(flags)
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

// DecodeResponse parses one complete v6 response frame. Like the C++ side,
// the flags byte is optional (absent on pre-v3 frames); the value consumes
// all remaining bytes minus the trailing trace field, so has_val=1 with an
// empty body decodes as present-but-empty rather than absent.
func DecodeResponse(frame []byte) (Response, error) {
	var res Response
	_, payload, err := parseFrame(frame)
	if err != nil {
		return res, err
	}
	r := codec.NewReader(binary.BigEndian, payload)
	status, ok := r.U8()
	if !ok {
		return res, errInvalid("truncated frame")
	}
	res.Status = Status(status)
	var flags uint8
	if r.Remaining() > 0 {
		flags, ok = r.U8()
		if !ok {
			return res, errInvalid("truncated frame")
		}
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
