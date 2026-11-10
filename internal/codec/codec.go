// Package codec is the shared binary framing kit for the wire-adjacent
// codecs (protocol, persist, AE, counters). It replaces four copies of
// the same helpers: the putU8/putU32/putU64 triplets (persist, ae),
// the bounds-checked reader/writer method sets (protocol, counter), and
// the u32-length-prefixed blob chains (AE journal, counter codec).
//
// Shape notes: Reader/Writer are deliberately NOT generic over
// endianness — Go methods cannot introduce type parameters, and the
// order would still dispatch through the binary.ByteOrder interface
// either way, so a type parameter buys nothing. Generics earn their
// keep in the free functions instead: Append/Get collapse the
// per-width helpers into one function each, with the width taken from
// the value's type.
//
// Encodings use the specified byte order and append fields in wire order;
// package golden-vector and fuzz tests verify the resulting formats.
package codec

import (
	"encoding/binary"
)

// Uint covers the widths codecs traffic in. int/uint are excluded on
// purpose: their width is platform-dependent, and wire formats must not
// be.
type Uint interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

// Append writes v in order's encoding. One function for every width;
// the u32-length-prefix chains become Append(b, order, uint32(len(x)))
// or the Len/Blob helpers below. (Put + a stack scratch, because the
// ByteOrder interface carries no Append methods — only the concrete
// endian types do.)
//
// Hot-path note: the any-switch boxes v (1 alloc). Per-RPC writers must
// use Writer.U8/U32/U64, which route to the concrete putters below.
// Append is for cold paths (snapshots, journal recovery, one-shot
// encodes) where a scratch alloc is noise next to I/O.
func Append[T Uint](b []byte, order binary.ByteOrder, v T) []byte {
	switch w := any(v).(type) {
	case uint8:
		return putU8(b, w)
	case uint16:
		var tmp [2]byte
		if order == binary.LittleEndian {
			tmp[0], tmp[1] = byte(w), byte(w>>8)
		} else {
			tmp[0], tmp[1] = byte(w>>8), byte(w)
		}
		return append(b, tmp[:]...)
	case uint32:
		return putU32(b, order, w)
	case uint64:
		return putU64(b, order, w)
	}
	panic("codec: unreachable width")
}

// putU8/putU32/putU64 are the concrete, allocation-free appends. Writer
// methods route here (never through Append) so per-RPC framing stays at
// the old zero-scratch profile. Bytes are assembled manually rather than
// via order.PutUintXX on a scratch array: the scratch escapes through
// the interface call (measured: 1 alloc per append), while shifting
// never leaves the stack. Only LittleEndian/BigEndian exist, so the
// branch is exhaustive.
func putU8(b []byte, v uint8) []byte { return append(b, v) }

func putU32(b []byte, order binary.ByteOrder, v uint32) []byte {
	if order == binary.LittleEndian {
		return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func putU64(b []byte, order binary.ByteOrder, v uint64) []byte {
	if order == binary.LittleEndian {
		return append(b, byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
			byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
	}
	return append(b, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// AppendLen writes n as a u32 length prefix — the universal "len +
// bytes" framing used by the journal, the counter codecs, and the WAL.
func AppendLen(b []byte, order binary.ByteOrder, n int) []byte {
	return putU32(b, order, uint32(n))
}

// AppendBlob writes a u32-length-prefixed blob. B is []byte or string so
// both key forms append without a conversion at the call site.
func AppendBlob[B ~[]byte | ~string](b []byte, order binary.ByteOrder, blob B) []byte {
	b = AppendLen(b, order, len(blob))
	return append(b, blob...)
}

// Get reads a T from the front of buf, reporting short input instead of
// panicking like the bare binary.Uint* calls do. Cold paths only: the
// type switch boxes (1 alloc), like Append.
func Get[T Uint](order binary.ByteOrder, buf []byte) (T, bool) {
	var zero T
	switch any(zero).(type) {
	case uint8:
		if len(buf) < 1 {
			return zero, false
		}
		return T(buf[0]), true
	case uint16:
		if len(buf) < 2 {
			return zero, false
		}
		return T(order.Uint16(buf)), true
	case uint32:
		if len(buf) < 4 {
			return zero, false
		}
		return T(order.Uint32(buf)), true
	case uint64:
		if len(buf) < 8 {
			return zero, false
		}
		return T(order.Uint64(buf)), true
	}
	panic("codec: unreachable width")
}

// Reader is a bounds-checked cursor over a frame. It unifies the
// protocol/counter reader types method-for-method (u8/u32/u64/bytes)
// and adds Blob for the u32-length-prefixed fields both used to parse
// by hand.
type Reader struct {
	order binary.ByteOrder
	buf   []byte
	off   int
}

// NewReader cursors over buf in order's encoding. The value is the
// cursor: keep it in a local so method calls stay on the stack.
func NewReader(order binary.ByteOrder, buf []byte) Reader {
	return Reader{order: order, buf: buf}
}

// Remaining reports the unread bytes.
func (r *Reader) Remaining() int { return len(r.buf) - r.off }

// Pos reports the read offset, for record-boundary bookkeeping
// (checksums over decoded spans).
func (r *Reader) Pos() int { return r.off }

// SetPos seeks to an absolute offset.
func (r *Reader) SetPos(n int) { r.off = n }

// U8 reads one byte.
func (r *Reader) U8() (uint8, bool) {
	if r.Remaining() < 1 {
		return 0, false
	}
	v := r.buf[r.off]
	r.off++
	return v, true
}

// U32 reads four bytes in the reader's encoding.
func (r *Reader) U32() (uint32, bool) {
	if r.Remaining() < 4 {
		return 0, false
	}
	v := r.order.Uint32(r.buf[r.off:])
	r.off += 4
	return v, true
}

// U64 reads eight bytes in the reader's encoding.
func (r *Reader) U64() (uint64, bool) {
	if r.Remaining() < 8 {
		return 0, false
	}
	v := r.order.Uint64(r.buf[r.off:])
	r.off += 8
	return v, true
}

// Bytes reads n raw bytes (not a copy).
func (r *Reader) Bytes(n int) ([]byte, bool) {
	if n < 0 || r.Remaining() < n {
		return nil, false
	}
	v := r.buf[r.off : r.off+n]
	r.off += n
	return v, true
}

// Blob reads a u32-length-prefixed field (not a copy).
func (r *Reader) Blob() ([]byte, bool) {
	n, ok := r.U32()
	if !ok {
		return nil, false
	}
	return r.Bytes(int(n))
}

// Writer appends to a buffer in one encoding. It unifies the
// protocol writer methods and the persist/AE putU* free functions.
type Writer struct {
	order binary.ByteOrder
	buf   []byte
}

// NewWriter appends to buf (nil for fresh) in order's encoding. The
// value is the writer: keep it in a local so per-RPC use stays
// allocation-free like the hand-rolled writer it replaces.
func NewWriter(order binary.ByteOrder, buf []byte) Writer {
	return Writer{order: order, buf: buf}
}

// Reset reuses the writer over buf, preserving the buffer-reuse pattern
// on per-RPC hot paths.
func (w *Writer) Reset(buf []byte) { w.buf = buf }

// U8 appends one byte.
func (w *Writer) U8(v uint8) { w.buf = putU8(w.buf, v) }

// U32 appends four bytes in the writer's encoding.
func (w *Writer) U32(v uint32) { w.buf = putU32(w.buf, w.order, v) }

// U64 appends eight bytes in the writer's encoding.
func (w *Writer) U64(v uint64) { w.buf = putU64(w.buf, w.order, v) }

// Bytes appends raw bytes.
func (w *Writer) Bytes(b []byte) { w.buf = append(w.buf, b...) }

// Len appends a u32 length prefix.
func (w *Writer) Len(n int) { w.buf = AppendLen(w.buf, w.order, n) }

// Blob appends a u32-length-prefixed blob.
func (w *Writer) Blob(b []byte) { w.buf = AppendBlob(w.buf, w.order, b) }

// Out returns the accumulated buffer.
func (w *Writer) Out() []byte { return w.buf }
