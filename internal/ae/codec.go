// Package ae implements Cinder's anti-entropy protocol (Go port).
//
// Wire schemas (little-endian, magic-prefixed blobs) are byte-identical
// with the C++ implementation: AEM3 group digests, AEB5 bucket refinement,
// AER6 digest responses, AEMS sync messages, AEI4 IBLT sketches. Unknown
// magics are rejected so mixed-version clusters fail rounds cleanly.
//
// The manager runs full-scan digests each round (behaviorally identical to
// the C++ P1 incremental accumulators, without the divert-while-scanning
// machinery); see README for the deliberate simplifications.
package ae

import (
	"encoding/binary"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/zeebo/xxh3"

	"github.com/kraytos17/cinder/internal/store"
)

// Magic prefixes (u32 LE). Each magic tags one AE payload shape;
// layouts live with their codecs below and in docs/protocol.md.
const (
	MagicDigest      uint32 = 0x41454D32 // "AEM2" legacy flat digest
	MagicGroupDigest uint32 = 0x41454D33 // "AEM3"
	MagicResponse    uint32 = 0x41455236 // "AER6"
	MagicRefinement  uint32 = 0x41454235 // "AEB5"
	MagicSync        uint32 = 0x41454D53 // "AEMS"
	MagicIBLT        uint32 = 0x41454934 // "AEI4"

	BucketsPerGroup = 16

	// IBLT sizing mirrors iblt::K_*.

	// IBLTCount is the sketch count per bucket batch.
	IBLTCount = 3
	// IBLTCellsMin is the smallest admissible sketch width.
	IBLTCellsMin = 3
	// IBLTCellsMax is the largest admissible sketch width.
	IBLTCellsMax = 4096
	// IBLTCells is the production sketch width.
	IBLTCells = 64
	// IBLTMinRows is the row count below which explicit metadata wins over sketches.
	IBLTMinRows   = 32
	IBLTCelBytes  = 6 * 8
	MaxMetaBytes  = 4 << 20
	RefinementKey = "bucket-refinement-v3"
)

// MetaRow is one metadata row: bucket id plus the LWW identity.
type MetaRow struct {
	Bucket    uint32
	Tombstone bool
	KeyHash   uint64
	Version   uint64
	Writer    uint64
}

// SyncItem is one full sync entry.
type SyncItem struct {
	Key   string
	Entry store.Entry
}

// SyncMessage is a decoded AEMS payload.
type SyncMessage struct {
	Entries []SyncItem
	Needs   []uint64
}

// SketchRef pairs a bucket with its encoded IBLT blob.
type SketchRef struct {
	Bucket uint32
	Blob   []byte
}

// BucketHash pairs a bucket id with its XOR-folded digest.
type BucketHash struct {
	Bucket uint32
	Hash   uint64
}

// DigestResponse is a decoded AER6 payload.
type DigestResponse struct {
	GroupHashes  []uint64
	BucketHashes []BucketHash // encode order preserved
	Covered      []uint32
	Rows         []MetaRow
	Sketches     []SketchRef
}

// Refinement is a decoded AEB5 request.
type Refinement struct {
	BucketHashes []BucketHash
	ForceMeta    bool
}

// IBLTItem is the full LWW identity tuple hashed into sketches.
type IBLTItem struct {
	KeyHash   uint64
	Version   uint64
	Writer    uint64
	Tombstone bool
}

// IBLTCell is one 48-byte sketch cell.
type IBLTCell struct {
	Count        int64
	KeyXOR       uint64
	VersionXOR   uint64
	WriterXOR    uint64
	TombstoneXOR uint64
	ChecksumXOR  uint64
}

// IBLTDiff is a fully peeled symmetric difference.
type IBLTDiff struct {
	RemoteOnly []IBLTItem
	LocalOnly  []IBLTItem
}

func putU8(b []byte, v uint8) []byte   { return append(b, v) }
func putU32(b []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(b, v) }
func putU64(b []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(b, v) }
func putI64(b []byte, v int64) []byte  { return binary.LittleEndian.AppendUint64(b, uint64(v)) }

type cursor struct {
	buf []byte
	off int
}

func (c *cursor) rem() int { return len(c.buf) - c.off }
func (c *cursor) u8() (uint8, bool) {
	if c.rem() < 1 {
		return 0, false
	}

	v := c.buf[c.off]
	c.off++
	return v, true
}

func (c *cursor) u32() (uint32, bool) {
	if c.rem() < 4 {
		return 0, false
	}

	v := binary.LittleEndian.Uint32(c.buf[c.off:])
	c.off += 4
	return v, true
}

func (c *cursor) u64() (uint64, bool) {
	if c.rem() < 8 {
		return 0, false
	}

	v := binary.LittleEndian.Uint64(c.buf[c.off:])
	c.off += 8
	return v, true
}

func (c *cursor) bytes(n int) ([]byte, bool) {
	if n < 0 || c.rem() < n {
		return nil, false
	}

	v := c.buf[c.off : c.off+n : c.off+n]
	c.off += n
	return v, true
}

// EncodeDigest serializes a legacy AEM2 flat digest.
func EncodeDigest(digest []uint64) []byte {
	out := make([]byte, 0, 8+8*len(digest))
	out = putU32(out, MagicDigest)
	out = putU32(out, uint32(len(digest)))
	for _, h := range digest {
		out = putU64(out, h)
	}
	return out
}

// DecodeDigest parses an AEM2 blob with exact size validation.
func DecodeDigest(data []byte, expected int) ([]uint64, bool) {
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicDigest {
		return nil, false
	}

	n, ok := c.u32()
	if !ok || int(n) != expected || c.rem() != int(n)*8 {
		return nil, false
	}

	out := make([]uint64, 0, n)
	for range n {
		h, ok := c.u64()
		if !ok {
			return nil, false
		}
		out = append(out, h)
	}
	return out, true
}

// GroupFold folds bucket hashes into group hashes: XXH3 over alternating
// (bucket u32, hash u64) pairs, including each absolute bucket index so
// the fold is placement-sensitive. Mirrors ae_codec::groupDigests.
func GroupFold(hashes []uint64) []uint64 {
	n := (len(hashes) + BucketsPerGroup - 1) / BucketsPerGroup
	out := make([]uint64, 0, n)
	for g := range n {
		begin := g * BucketsPerGroup
		end := min(begin+BucketsPerGroup, len(hashes))

		var mat []byte
		for b := begin; b < end; b++ {
			mat = putU32(mat, uint32(b))
			mat = putU64(mat, hashes[b])
		}
		out = append(out, xxh3.Hash(mat))
	}
	return out
}

// EncodeGroupDigest serializes an AEM3 group digest.
func EncodeGroupDigest(groups []uint64) []byte {
	out := make([]byte, 0, 8+8*len(groups))
	out = putU32(out, MagicGroupDigest)
	out = putU32(out, uint32(len(groups)))
	for _, h := range groups {
		out = putU64(out, h)
	}
	return out
}

// DecodeGroupDigest parses an AEM3 blob with exact size validation.
func DecodeGroupDigest(data []byte, expectedGroups int) ([]uint64, bool) {
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicGroupDigest {
		return nil, false
	}

	n, ok := c.u32()
	if !ok || int(n) != expectedGroups || c.rem() != int(n)*8 {
		return nil, false
	}

	out := make([]uint64, 0, n)
	for range n {
		h, ok := c.u64()
		if !ok {
			return nil, false
		}
		out = append(out, h)
	}
	return out, true
}

// EncodeRefinement serializes an AEB5 request.
func EncodeRefinement(r Refinement) []byte {
	out := make([]byte, 0, 12+12*len(r.BucketHashes))
	out = putU32(out, MagicRefinement)
	if r.ForceMeta {
		out = putU32(out, 1)
	} else {
		out = putU32(out, 0)
	}

	out = putU32(out, uint32(len(r.BucketHashes)))
	for _, bh := range r.BucketHashes {
		out = putU32(out, bh.Bucket)
		out = putU64(out, bh.Hash)
	}
	return out
}

// DecodeRefinement parses an AEB5 blob. Bucket ids must be < groups (the
// C++ bound: callers pass num_buckets_; ids beyond it are rejected).
func DecodeRefinement(data []byte, maxBucket uint32) (Refinement, bool) {
	var out Refinement
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicRefinement {
		return out, false
	}

	flags, ok := c.u32()
	if !ok || flags&^1 != 0 {
		return out, false
	}

	n, ok := c.u32()
	if !ok || c.rem() != int(n)*12 {
		return out, false
	}

	out.ForceMeta = flags&1 != 0
	for range n {
		b, ok := c.u32()
		if !ok || b >= maxBucket {
			return out, false
		}

		h, ok := c.u64()
		if !ok {
			return out, false
		}
		out.BucketHashes = append(out.BucketHashes, BucketHash{Bucket: b, Hash: h})
	}
	return out, true
}

// EncodeMetaRows serializes rows: [MAGIC? no magic][count u32][rows].
// Layout mirrors encodeMetaRows (count first, no envelope magic).
func EncodeMetaRows(rows []MetaRow) []byte {
	out := make([]byte, 0, 4+len(rows)*29)
	out = putU32(out, uint32(len(rows)))
	for _, r := range rows {
		out = putU32(out, r.Bucket)
		out = putU64(out, r.KeyHash)
		out = putU64(out, r.Version)
		out = putU64(out, r.Writer)
		if r.Tombstone {
			out = putU8(out, 1)
		} else {
			out = putU8(out, 0)
		}
	}
	return out
}

// DecodeMetaRows parses rows with exact size validation.
func DecodeMetaRows(data []byte) ([]MetaRow, bool) {
	c := &cursor{buf: data}
	n, ok := c.u32()
	if !ok || c.rem() != int(n)*29 {
		return nil, false
	}

	out := make([]MetaRow, 0, n)
	for range n {
		var r MetaRow
		var tomb uint8
		var ok bool
		if r.Bucket, ok = c.u32(); !ok {
			return nil, false
		}
		if r.KeyHash, ok = c.u64(); !ok {
			return nil, false
		}
		if r.Version, ok = c.u64(); !ok {
			return nil, false
		}
		if r.Writer, ok = c.u64(); !ok {
			return nil, false
		}
		if tomb, ok = c.u8(); !ok || tomb > 1 {
			return nil, false
		}

		r.Tombstone = tomb != 0
		out = append(out, r)
	}
	return out, true
}

// EncodeResponse serializes an AER6 digest response.
func EncodeResponse(r DigestResponse) []byte {
	rows := EncodeMetaRows(r.Rows)
	sketchSize := 0
	for _, s := range r.Sketches {
		sketchSize += 8 + len(s.Blob)
	}

	out := make([]byte, 0, 16+8*len(r.GroupHashes)+12*len(r.BucketHashes)+4*len(r.Covered)+4+len(rows)+4+sketchSize)
	out = putU32(out, MagicResponse)
	out = putU32(out, uint32(len(r.GroupHashes)))
	for _, h := range r.GroupHashes {
		out = putU64(out, h)
	}

	out = putU32(out, uint32(len(r.BucketHashes)))
	for _, bh := range r.BucketHashes {
		out = putU32(out, bh.Bucket)
		out = putU64(out, bh.Hash)
	}

	out = putU32(out, uint32(len(r.Covered)))
	for _, b := range r.Covered {
		out = putU32(out, b)
	}

	out = putU32(out, uint32(len(rows)))
	out = append(out, rows...)
	out = putU32(out, uint32(len(r.Sketches)))
	for _, s := range r.Sketches {
		out = putU32(out, s.Bucket)
		out = putU32(out, uint32(len(s.Blob)))
		out = append(out, s.Blob...)
	}
	return out
}

// DecodeResponse parses an AER6 response: bucket ids, row coverage, and
// sketch uniqueness are all validated like the C++ decoder.
func DecodeResponse(data []byte, expectedGroups int) (DigestResponse, bool) {
	var out DigestResponse
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicResponse {
		return out, false
	}

	gc, ok := c.u32()
	if !ok || int(gc) != expectedGroups {
		return out, false
	}

	out.GroupHashes = make([]uint64, 0, gc)
	for range gc {
		h, ok := c.u64()
		if !ok {
			return out, false
		}
		out.GroupHashes = append(out.GroupHashes, h)
	}

	bhc, ok := c.u32()
	if !ok || c.rem() < int(bhc)*12 {
		return out, false
	}
	for range bhc {
		b, ok := c.u32()
		if !ok {
			return out, false
		}

		h, ok := c.u64()
		if !ok {
			return out, false
		}
		out.BucketHashes = append(out.BucketHashes, BucketHash{Bucket: b, Hash: h})
	}

	cc, ok := c.u32()
	if !ok || c.rem() < int(cc)*4+4 {
		return out, false
	}

	covered := map[uint32]bool{}
	for range cc {
		b, ok := c.u32()
		if !ok {
			return out, false
		}

		out.Covered = append(out.Covered, b)
		covered[b] = true
	}

	rs, ok := c.u32()
	if !ok || c.rem() < int(rs)+4 {
		return out, false
	}

	rowsBytes, ok := c.bytes(int(rs))
	if !ok {
		return out, false
	}

	rows, ok := DecodeMetaRows(rowsBytes)
	if !ok {
		return out, false
	}

	out.Rows = rows
	sc, ok := c.u32()
	if !ok {
		return out, false
	}
	// C++ bound: sketch_count <= remaining / 8; decode-time exactness
	// below handles the rest.
	if uint64(sc) > uint64(c.rem())/8 {
		return out, false
	}

	seenSketch := map[uint32]bool{}
	for range sc {
		b, ok := c.u32()
		if !ok {
			return out, false
		}

		sz, ok := c.u32()
		if !ok {
			return out, false
		}

		blob, ok := c.bytes(int(sz))
		if !ok || DecodeIBLT(blob) == nil {
			return out, false
		}
		if !covered[b] || seenSketch[b] {
			return out, false
		}

		seenSketch[b] = true
		out.Sketches = append(out.Sketches, SketchRef{Bucket: b, Blob: append([]byte(nil), blob...)})
	}
	if c.rem() != 0 {
		return out, false
	}

	sortedCovered := append([]uint32(nil), out.Covered...)
	slices.Sort(sortedCovered)
	contains := func(b uint32) bool {
		i := sort.Search(len(sortedCovered), func(i int) bool { return sortedCovered[i] >= b })
		return i < len(sortedCovered) && sortedCovered[i] == b
	}

	for _, row := range out.Rows {
		if !contains(row.Bucket) {
			return out, false
		}
		if seenSketch[row.Bucket] {
			return out, false
		}
	}
	return out, true
}

// EncodeSync serializes an AEMS message: entries then the needlist.
// Entry layout: key_len u32, key, version u64, writer u64, flags u8
// (bit0=ttl, bit1=tombstone; other bits rejected on decode),
// expires_ms u64 if ttl, val_len u32, value.
func EncodeSync(msg SyncMessage) ([]byte, error) {
	size := 4 + 4 + 8*len(msg.Needs)
	for _, it := range msg.Entries {
		n := 4 + len(it.Key) + 8 + 8 + 1 + 4 + len(it.Entry.Value)
		if n < 0 {
			return nil, errTooLarge
		}

		size += n
		if it.Entry.HasTTL {
			size += 8
		}
		if size > MaxMetaBytes*4 {
			return nil, errTooLarge
		}
	}

	out := make([]byte, 0, size)
	out = putU32(out, MagicSync)
	out = putU32(out, uint32(len(msg.Entries)))
	for _, it := range msg.Entries {
		out = putU32(out, uint32(len(it.Key)))
		out = append(out, it.Key...)
		out = putU64(out, it.Entry.Version)
		out = putU64(out, it.Entry.WriterNodeHash)

		var flags uint8
		if it.Entry.HasTTL {
			flags |= 1
		}
		if it.Entry.Tombstone {
			flags |= 2
		}

		out = putU8(out, flags)
		if it.Entry.HasTTL {
			out = putU64(out, uint64(it.Entry.ExpiresAt.UnixMilli()))
		}

		out = putU32(out, uint32(len(it.Entry.Value)))
		out = append(out, it.Entry.Value...)
	}

	out = putU32(out, uint32(len(msg.Needs)))
	for _, h := range msg.Needs {
		out = putU64(out, h)
	}
	return out, nil
}

var errTooLarge = errors.New("ae: payload too large")

// DecodeSync parses an AEMS message with exact-size validation.
func DecodeSync(data []byte) (SyncMessage, bool) {
	var msg SyncMessage
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicSync {
		return msg, false
	}

	n, ok := c.u32()
	if !ok {
		return msg, false
	}
	for range n {
		var item SyncItem
		kl, ok := c.u32()
		if !ok {
			return msg, false
		}

		kb, ok := c.bytes(int(kl))
		if !ok {
			return msg, false
		}

		item.Key = string(append([]byte(nil), kb...))
		var e store.Entry
		ver, ok := c.u64()
		if !ok {
			return msg, false
		}

		writer, ok := c.u64()
		if !ok {
			return msg, false
		}

		flags, ok := c.u8()
		if !ok || flags&^3 != 0 {
			return msg, false
		}

		e.Version, e.WriterNodeHash = ver, writer
		e.Tombstone = flags&2 != 0
		if e.HasTTL = flags&1 != 0; e.HasTTL {
			ms, ok := c.u64()
			if !ok {
				return msg, false
			}
			e.ExpiresAt = time.UnixMilli(int64(ms)).UTC()
		}

		vl, ok := c.u32()
		if !ok {
			return msg, false
		}

		val, ok := c.bytes(int(vl))
		if !ok {
			return msg, false
		}

		e.Value = append([]byte(nil), val...)
		item.Entry = e
		msg.Entries = append(msg.Entries, item)
	}

	nc, ok := c.u32()
	if !ok || c.rem() != int(nc)*8 {
		return msg, false
	}
	for range nc {
		h, ok := c.u64()
		if !ok {
			return msg, false
		}
		msg.Needs = append(msg.Needs, h)
	}
	return msg, true
}

func ibltChecksum(keyHash, version, writer uint64, tomb bool) uint64 {
	var words [32]byte
	binary.LittleEndian.PutUint64(words[0:], keyHash)
	binary.LittleEndian.PutUint64(words[8:], version)
	binary.LittleEndian.PutUint64(words[16:], writer)
	if tomb {
		binary.LittleEndian.PutUint64(words[24:], 1)
	}
	return xxh3.Hash(words[:])
}

func ibltPositions(checksum uint64, count int) [3]int {
	var res [3]int
	for i := range 3 {
		c := checksum + uint64(i+1)*0x9E3779B97F4A7C15
		c ^= c >> 30
		c *= 0xBF58476D1CE4E5B9
		c ^= c >> 27
		c *= 0x94D049BB133111EB
		c ^= c >> 31
		pos := int(c % uint64(count))
		for j := range i {
			if res[j] == pos {
				pos++
				if pos == count {
					pos = 0
				}
			}
		}
		res[i] = pos
	}
	return res
}

func ibltApply(cell *IBLTCell, sign int64, row IBLTItem, checksum uint64) {
	cell.Count += sign
	cell.KeyXOR ^= row.KeyHash
	cell.VersionXOR ^= row.Version
	cell.WriterXOR ^= row.Writer
	if row.Tombstone {
		cell.TombstoneXOR ^= 1
	}
	cell.ChecksumXOR ^= checksum
}

func ibltPure(cell IBLTCell) bool {
	if cell.Count != 1 && cell.Count != -1 {
		return false
	}
	if cell.TombstoneXOR != 0 && cell.TombstoneXOR != 1 {
		return false
	}
	return cell.ChecksumXOR == ibltChecksum(cell.KeyXOR, cell.VersionXOR, cell.WriterXOR, cell.TombstoneXOR != 0)
}

// BuildIBLT inserts rows into a fresh cell_count-cell sketch.
func BuildIBLT(rows []IBLTItem, cells int) []IBLTCell {
	if cells < IBLTCellsMin || cells > IBLTCellsMax {
		return nil
	}

	sk := make([]IBLTCell, cells)
	for _, r := range rows {
		InsertIBLT(sk, r)
	}
	return sk
}

// InsertIBLT adds one item to an existing sketch.
func InsertIBLT(sketch []IBLTCell, row IBLTItem) {
	sum := ibltChecksum(row.KeyHash, row.Version, row.Writer, row.Tombstone)
	for _, pos := range ibltPositions(sum, len(sketch)) {
		ibltApply(&sketch[pos], 1, row, sum)
	}
}

// EncodeIBLT serializes a sketch: magic + cell count + 48-byte cells
// (count as u64 bits, then 5 xor sums).
func EncodeIBLT(cells []IBLTCell) []byte {
	if len(cells) < IBLTCellsMin || len(cells) > IBLTCellsMax {
		return nil
	}

	out := make([]byte, 0, 8+len(cells)*IBLTCelBytes)
	out = putU32(out, MagicIBLT)
	out = putU32(out, uint32(len(cells)))
	for _, c := range cells {
		out = putI64(out, c.Count)
		out = putU64(out, c.KeyXOR)
		out = putU64(out, c.VersionXOR)
		out = putU64(out, c.WriterXOR)
		out = putU64(out, c.TombstoneXOR)
		out = putU64(out, c.ChecksumXOR)
	}
	return out
}

// DecodeIBLT parses a sketch with exact-size validation plus the C++
// count-bounds sanity check. Returns nil on any invalid input.
func DecodeIBLT(data []byte) []IBLTCell {
	c := &cursor{buf: data}
	magic, ok := c.u32()
	if !ok || magic != MagicIBLT {
		return nil
	}

	n, ok := c.u32()
	if !ok || int(n) < IBLTCellsMin || int(n) > IBLTCellsMax || c.rem() != int(n)*IBLTCelBytes {
		return nil
	}

	out := make([]IBLTCell, 0, n)
	for range n {
		bits, ok := c.u64()
		if !ok {
			return nil
		}
		if bits < 1<<63 && bits > 1<<40 {
			return nil
		}
		if bits >= 1<<63 && (^uint64(0)-bits) < 1<<40 {
			return nil
		}

		var cell IBLTCell
		cell.Count = int64(bits)
		if cell.KeyXOR, ok = c.u64(); !ok {
			return nil
		}
		if cell.VersionXOR, ok = c.u64(); !ok {
			return nil
		}
		if cell.WriterXOR, ok = c.u64(); !ok {
			return nil
		}
		if cell.TombstoneXOR, ok = c.u64(); !ok {
			return nil
		}
		if cell.ChecksumXOR, ok = c.u64(); !ok {
			return nil
		}
		out = append(out, cell)
	}
	return out
}

// DifferenceIBLT subtracts local from remote and peels pure cells,
// returning the symmetric difference or false when peeling stalls.
// Mirrors iblt::difference, including the peel budget bound.
func DifferenceIBLT(remote, local []IBLTCell) (IBLTDiff, bool) {
	var zero IBLTDiff
	if len(remote) != len(local) || len(remote) < IBLTCellsMin || len(remote) > IBLTCellsMax {
		return zero, false
	}

	delta := make([]IBLTCell, len(remote))
	for i := range remote {
		delta[i].Count = remote[i].Count - local[i].Count
		delta[i].KeyXOR = remote[i].KeyXOR ^ local[i].KeyXOR
		delta[i].VersionXOR = remote[i].VersionXOR ^ local[i].VersionXOR
		delta[i].WriterXOR = remote[i].WriterXOR ^ local[i].WriterXOR
		delta[i].TombstoneXOR = remote[i].TombstoneXOR ^ local[i].TombstoneXOR
		delta[i].ChecksumXOR = remote[i].ChecksumXOR ^ local[i].ChecksumXOR
	}

	var diff IBLTDiff
	queued := make([]bool, len(delta))
	var queue []int
	for i := range delta {
		if ibltPure(delta[i]) {
			queue = append(queue, i)
			queued[i] = true
		}
	}

	peeled := 0
	budget := len(delta) * IBLTCount
	for cursor := 0; cursor < len(queue); cursor++ {
		pos := queue[cursor]
		cell := delta[pos]
		if !ibltPure(cell) {
			queued[pos] = false
			continue
		}
		if peeled++; peeled > budget {
			return zero, false
		}

		queued[pos] = false
		row := IBLTItem{KeyHash: cell.KeyXOR, Version: cell.VersionXOR, Writer: cell.WriterXOR, Tombstone: cell.TombstoneXOR != 0}
		sum := ibltChecksum(row.KeyHash, row.Version, row.Writer, row.Tombstone)
		if cell.Count == 1 {
			diff.RemoteOnly = append(diff.RemoteOnly, row)
			for _, t := range ibltPositions(sum, len(delta)) {
				ibltApply(&delta[t], -1, row, sum)
				if !queued[t] && ibltPure(delta[t]) {
					queue = append(queue, t)
					queued[t] = true
				}
			}
		} else {
			diff.LocalOnly = append(diff.LocalOnly, row)
			for _, t := range ibltPositions(sum, len(delta)) {
				ibltApply(&delta[t], 1, row, sum)
				if !queued[t] && ibltPure(delta[t]) {
					queue = append(queue, t)
					queued[t] = true
				}
			}
		}
	}
	for _, cell := range delta {
		if cell != (IBLTCell{}) {
			return zero, false
		}
	}
	return diff, true
}
