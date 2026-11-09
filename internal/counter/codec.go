// Wire codecs for the state-based CRDT counters: byte-exact ports of
// cinder/store/crdt_counter.hpp (CRDC bodies) and the CAE1 bundle in
// src/node/anti_entropy/core.cpp. Network byte order is big-endian on
// both sides, matching internal/net/protocol.
//
// Layouts:
//
//	GCounter: magic "CRDC" u32 + kind u8(1) + count u32 + entries
//	PNCounter: magic "CRDC" u32 + kind u8(2) + pos-count u32 + pos entries
//	  + neg-count u32 + neg entries
//	entry: actor_len u32 + actor bytes + value u64
//	CAE1 bundle: magic "CAE1" u32 + count u32 + records
//	record: key_len u32 + key + state_len u32 + state(CRDC PNCounter)
//
// Decode is strict: wrong magic/kind, empty actor ids, a count that cannot
// fit in the remaining bytes, truncation, non-increasing bundle keys, or
// trailing garbage all fail.
package counter

import (
	"encoding/binary"
	"errors"
	"math"
)

// Magic and kind bytes, mirroring K_CRDT_COUNTER_MAGIC and friends.
const (
	MagicCRDC     uint32 = 0x43524443 // "CRDC"
	MagicCAE1     uint32 = 0x43414531 // "CAE1"
	KindGCounter  uint8  = 1
	KindPNCounter uint8  = 2
)

// minComponentBytes bounds a lying count before allocating: every entry
// costs at least actor_len u32 + value u64 (actor itself is non-empty).
const minComponentBytes = 4 + 8

// minRecordBytes bounds a lying bundle count: 1-byte key + CRDC header
// (magic u32 + kind u8).
const minRecordBytes = 2*4 + 2

// ErrDecode reports a malformed counter body or bundle.
var ErrDecode = errors.New("counter: invalid encoding")

// encodedSize returns the exact buffer need for one side's components.
func encodedComponentsSize(g GCounter) int {
	n := 4 // count u32
	for _, actor := range g.Actors() {
		n += 4 + len(actor) + 8
	}
	return n
}

// EncodeGCounter serializes g. Empty actor ids or u32-overflowing lengths
// cannot occur from a valid in-memory counter; they return nil.
func EncodeGCounter(g GCounter) []byte {
	if len(g.components) > math.MaxUint32 {
		return nil
	}
	need := 4 + 1 + encodedComponentsSize(g)
	buf := make([]byte, 0, need)
	buf = binary.BigEndian.AppendUint32(buf, MagicCRDC)
	buf = append(buf, KindGCounter)
	buf = appendComponents(buf, g)
	if buf == nil {
		return nil
	}
	return buf
}

// EncodePNCounter serializes p: the GCounter body for P, then for N (each
// with its own count; no second magic/kind).
func EncodePNCounter(p PNCounter) []byte {
	if len(p.Pos.components) > math.MaxUint32 || len(p.Neg.components) > math.MaxUint32 {
		return nil
	}
	need := 4 + 1 + encodedComponentsSize(p.Pos) + encodedComponentsSize(p.Neg)
	buf := make([]byte, 0, need)
	buf = binary.BigEndian.AppendUint32(buf, MagicCRDC)
	buf = append(buf, KindPNCounter)
	buf = appendComponents(buf, p.Pos)
	if buf == nil {
		return nil
	}
	buf = appendComponents(buf, p.Neg)
	if buf == nil {
		return nil
	}
	return buf
}

func appendComponents(buf []byte, g GCounter) []byte {
	actors := g.Actors()
	if len(actors) > math.MaxUint32 {
		return nil
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(actors)))
	for _, actor := range actors {
		if actor == "" || len(actor) > math.MaxUint32 {
			return nil
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(actor)))
		buf = append(buf, actor...)
		buf = binary.BigEndian.AppendUint64(buf, g.components[actor])
	}
	return buf
}

// DecodeGCounter parses a GCounter body, rejecting trailing garbage.
func DecodeGCounter(data []byte) (GCounter, error) {
	var g GCounter
	r := reader{buf: data}
	magic, ok := r.u32()
	if !ok || magic != MagicCRDC {
		return g, ErrDecode
	}
	kind, ok := r.u8()
	if !ok || kind != KindGCounter {
		return g, ErrDecode
	}
	if !decodeComponents(&r, &g) {
		return GCounter{}, ErrDecode
	}
	if r.remaining() != 0 {
		return GCounter{}, ErrDecode
	}
	return g, nil
}

// DecodePNCounter parses a PNCounter body into temporaries so a corrupt N
// body cannot leave a half-merged P side behind.
func DecodePNCounter(data []byte) (PNCounter, error) {
	var p PNCounter
	r := reader{buf: data}
	magic, ok := r.u32()
	if !ok || magic != MagicCRDC {
		return p, ErrDecode
	}
	kind, ok := r.u8()
	if !ok || kind != KindPNCounter {
		return p, ErrDecode
	}
	var pos, neg GCounter
	if !decodeComponents(&r, &pos) || !decodeComponents(&r, &neg) {
		return PNCounter{}, ErrDecode
	}
	if r.remaining() != 0 {
		return PNCounter{}, ErrDecode
	}
	p.Pos = pos
	p.Neg = neg
	return p, nil
}

func decodeComponents(r *reader, g *GCounter) bool {
	count, ok := r.u32()
	if !ok {
		return false
	}
	if uint64(count) > uint64(r.remaining())/uint64(minComponentBytes) {
		return false
	}
	for range count {
		ln, ok := r.u32()
		if !ok || ln == 0 {
			return false
		}
		actor, ok := r.bytes(int(ln))
		if !ok || len(actor) == 0 {
			return false
		}
		val, ok := r.u64()
		if !ok {
			return false
		}
		// Increment-from-empty reconstructs the component exactly
		// (0 + value always fits) and reuses the empty-actor guard.
		if err := g.Increment(string(actor), val); err != nil {
			return false
		}
	}
	return true
}

// BundleEntry is one CAE1 record: key plus its full counter state.
type BundleEntry struct {
	Key     string
	Counter PNCounter
}

// EncodeCounterBundle serializes entries (already sorted, cursor-selected)
// as a CAE1 bundle.
func EncodeCounterBundle(entries []BundleEntry) []byte {
	need := 4 + 4
	states := make([][]byte, len(entries))
	for i, e := range entries {
		if len(e.Key) == 0 || len(e.Key) > math.MaxUint32 {
			return nil
		}
		st := EncodePNCounter(e.Counter)
		if len(st) == 0 || len(st) > math.MaxUint32 {
			return nil
		}
		states[i] = st
		need += 4 + len(e.Key) + 4 + len(st)
	}
	if len(entries) > math.MaxUint32 {
		return nil
	}
	buf := make([]byte, 0, need)
	buf = binary.BigEndian.AppendUint32(buf, MagicCAE1)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	for i, e := range entries {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Key)))
		buf = append(buf, e.Key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(states[i])))
		buf = append(buf, states[i]...)
	}
	return buf
}

// DecodeCounterBundle parses a CAE1 bundle, requiring strictly increasing
// keys and no trailing garbage.
func DecodeCounterBundle(data []byte) ([]BundleEntry, error) {
	r := reader{buf: data}
	magic, ok := r.u32()
	if !ok || magic != MagicCAE1 {
		return nil, ErrDecode
	}
	count, ok := r.u32()
	if !ok {
		return nil, ErrDecode
	}
	if uint64(count) > uint64(r.remaining())/uint64(minRecordBytes) {
		return nil, ErrDecode
	}
	out := make([]BundleEntry, 0, count)
	var prev string
	hasPrev := false
	for range count {
		keyLen, ok := r.u32()
		if !ok || keyLen == 0 {
			return nil, ErrDecode
		}
		key, ok := r.bytes(int(keyLen))
		if !ok {
			return nil, ErrDecode
		}
		stateLen, ok := r.u32()
		if !ok || stateLen == 0 {
			return nil, ErrDecode
		}
		state, ok := r.bytes(int(stateLen))
		if !ok {
			return nil, ErrDecode
		}
		if hasPrev && string(key) <= prev {
			return nil, ErrDecode
		}
		c, err := DecodePNCounter(state)
		if err != nil {
			return nil, ErrDecode
		}
		prev, hasPrev = string(key), true
		out = append(out, BundleEntry{Key: string(key), Counter: c})
	}
	if r.remaining() != 0 {
		return nil, ErrDecode
	}
	return out, nil
}

// RecordBytes returns the encoded width of one bundle record.
func RecordBytes(key string, p PNCounter) (int, bool) {
	st := EncodePNCounter(p)
	if len(st) == 0 || len(key) == 0 || len(key) > math.MaxUint32 || len(st) > math.MaxUint32 {
		return 0, false
	}
	return 4 + len(key) + 4 + len(st), true
}

type reader struct {
	buf []byte
	off int
}

func (r *reader) remaining() int { return len(r.buf) - r.off }

func (r *reader) u8() (uint8, bool) {
	if r.remaining() < 1 {
		return 0, false
	}
	v := r.buf[r.off]
	r.off++
	return v, true
}

func (r *reader) u32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	v := binary.BigEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v, true
}

func (r *reader) u64() (uint64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	v := binary.BigEndian.Uint64(r.buf[r.off:])
	r.off += 8
	return v, true
}

func (r *reader) bytes(n int) ([]byte, bool) {
	if n < 0 || r.remaining() < n {
		return nil, false
	}
	v := r.buf[r.off : r.off+n]
	r.off += n
	return v, true
}
