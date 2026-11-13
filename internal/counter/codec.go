// Binary encodings for the state-based counters. CRDC stores GCounter and
// PNCounter state; CAE1 bundles counter states for anti-entropy transfer.
// All integers use big-endian byte order, matching the counter payloads in
// internal/net/protocol.
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

	"github.com/kraytos17/cinder/internal/codec"
)

// Magic and kind values identify the CRDC counter encoding and CAE1
// anti-entropy bundle.
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
func (g GCounter) encodedSize() int {
	n := 4
	for actor := range g.components {
		n += 4 + len(actor) + 8
	}
	return n
}

// EncodedSize returns the exact byte size of Encode without sorting actors
// or allocating. It is intended for transfer-budget selection.
func (g GCounter) EncodedSize() int {
	return 4 + 1 + g.encodedSize()
}

// EncodedSize returns the exact byte size of Encode without allocating.
func (p PNCounter) EncodedSize() int {
	return 4 + 1 + p.Pos.encodedSize() + p.Neg.encodedSize()
}

// Encode serializes g. Empty actor ids or u32-overflowing lengths
// cannot occur from a valid in-memory counter; they return nil.
func (g GCounter) Encode() []byte {
	if len(g.components) > math.MaxUint32 {
		return nil
	}

	need := 4 + 1 + g.encodedSize()
	buf := make([]byte, 0, need)
	buf = codec.Append(buf, binary.BigEndian, MagicCRDC)
	buf = append(buf, KindGCounter)
	buf = g.appendTo(buf)
	if buf == nil {
		return nil
	}
	return buf
}

// Encode serializes p: the GCounter body for P, then for N (each
// with its own count; no second magic/kind).
func (p PNCounter) Encode() []byte {
	if len(p.Pos.components) > math.MaxUint32 || len(p.Neg.components) > math.MaxUint32 {
		return nil
	}

	need := 4 + 1 + p.Pos.encodedSize() + p.Neg.encodedSize()
	buf := make([]byte, 0, need)
	buf = codec.Append(buf, binary.BigEndian, MagicCRDC)
	buf = append(buf, KindPNCounter)
	buf = p.Pos.appendTo(buf)
	if buf == nil {
		return nil
	}

	buf = p.Neg.appendTo(buf)
	if buf == nil {
		return nil
	}
	return buf
}

func (g GCounter) appendTo(buf []byte) []byte {
	actors := g.Actors()
	if len(actors) > math.MaxUint32 {
		return nil
	}

	buf = codec.AppendLen(buf, binary.BigEndian, len(actors))
	for _, actor := range actors {
		if actor == "" || len(actor) > math.MaxUint32 {
			return nil
		}

		buf = codec.AppendLen(buf, binary.BigEndian, len(actor))
		buf = append(buf, actor...)
		buf = codec.Append(buf, binary.BigEndian, g.components[actor])
	}
	return buf
}

// DecodeGCounter parses a GCounter body, rejecting trailing garbage.
func DecodeGCounter(data []byte) (GCounter, error) {
	var g GCounter
	r := codec.NewReader(binary.BigEndian, data)
	magic, ok := r.U32()
	if !ok || magic != MagicCRDC {
		return g, ErrDecode
	}

	kind, ok := r.U8()
	if !ok || kind != KindGCounter {
		return g, ErrDecode
	}
	if !g.decodeFrom(&r) {
		return GCounter{}, ErrDecode
	}
	if r.Remaining() != 0 {
		return GCounter{}, ErrDecode
	}
	return g, nil
}

// DecodePNCounter parses a PNCounter body into temporaries so a corrupt N
// body cannot leave a half-merged P side behind.
func DecodePNCounter(data []byte) (PNCounter, error) {
	var p PNCounter
	r := codec.NewReader(binary.BigEndian, data)
	magic, ok := r.U32()
	if !ok || magic != MagicCRDC {
		return p, ErrDecode
	}

	kind, ok := r.U8()
	if !ok || kind != KindPNCounter {
		return p, ErrDecode
	}

	var pos, neg GCounter
	if !pos.decodeFrom(&r) || !neg.decodeFrom(&r) {
		return PNCounter{}, ErrDecode
	}
	if r.Remaining() != 0 {
		return PNCounter{}, ErrDecode
	}

	p.Pos = pos
	p.Neg = neg
	return p, nil
}

func (g *GCounter) decodeFrom(r *codec.Reader) bool {
	count, ok := r.U32()
	if !ok {
		return false
	}
	if uint64(count) > uint64(r.Remaining())/uint64(minComponentBytes) {
		return false
	}
	for range count {
		actor, ok := r.Blob()
		if !ok || len(actor) == 0 {
			return false
		}

		val, ok := r.U64()
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

// EncodedBundleEntry is one already-encoded counter state selected for a
// bundle. Counter bytes must be a valid canonical CRDC PNCounter encoding.
type EncodedBundleEntry struct {
	Key   string
	State []byte
}

// EncodeCounterBundle serializes entries (already sorted, cursor-selected)
// as a CAE1 bundle.
func EncodeCounterBundle(entries []BundleEntry) []byte {
	encoded := make([]EncodedBundleEntry, len(entries))
	for i, entry := range entries {
		encoded[i] = EncodedBundleEntry{Key: entry.Key, State: entry.Counter.Encode()}
	}
	return EncodeEncodedCounterBundle(encoded)
}

// EncodeEncodedCounterBundle serializes already-encoded canonical counter
// states, avoiding a second clone/encode pass for selected store entries.
func EncodeEncodedCounterBundle(entries []EncodedBundleEntry) []byte {
	if uint64(len(entries)) > math.MaxUint32 {
		return nil
	}

	need := 8
	for i, entry := range entries {
		if len(entry.Key) == 0 || uint64(len(entry.Key)) > math.MaxUint32 || len(entry.State) == 0 || uint64(len(entry.State)) > math.MaxUint32 {
			return nil
		}
		if i > 0 && entries[i-1].Key >= entry.Key {
			return nil
		}
		need += 4 + len(entry.Key) + 4 + len(entry.State)
	}

	buf := make([]byte, 0, need)
	buf = codec.Append(buf, binary.BigEndian, MagicCAE1)
	buf = codec.AppendLen(buf, binary.BigEndian, len(entries))
	for _, entry := range entries {
		buf = codec.AppendLen(buf, binary.BigEndian, len(entry.Key))
		buf = append(buf, entry.Key...)
		buf = codec.AppendLen(buf, binary.BigEndian, len(entry.State))
		buf = append(buf, entry.State...)
	}
	return buf
}

// DecodeCounterBundle parses a CAE1 bundle, requiring strictly increasing
// keys and no trailing garbage.
func DecodeCounterBundle(data []byte) ([]BundleEntry, error) {
	r := codec.NewReader(binary.BigEndian, data)
	magic, ok := r.U32()
	if !ok || magic != MagicCAE1 {
		return nil, ErrDecode
	}

	count, ok := r.U32()
	if !ok {
		return nil, ErrDecode
	}
	if uint64(count) > uint64(r.Remaining())/uint64(minRecordBytes) {
		return nil, ErrDecode
	}

	out := make([]BundleEntry, 0, count)
	var prev string
	hasPrev := false
	for range count {
		key, ok := r.Blob()
		if !ok || len(key) == 0 {
			return nil, ErrDecode
		}

		state, ok := r.Blob()
		if !ok || len(state) == 0 {
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
	if r.Remaining() != 0 {
		return nil, ErrDecode
	}
	return out, nil
}

// BundleRecordSize returns the encoded width of one bundle record for
// key and p.
func (p PNCounter) BundleRecordSize(key string) (int, bool) {
	stateSize := p.EncodedSize()
	if len(key) == 0 || uint64(len(key)) > math.MaxUint32 || uint64(stateSize) > math.MaxUint32 {
		return 0, false
	}
	return 4 + len(key) + 4 + stateSize, true
}
