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

	"github.com/kraytos17/cinder/internal/codec"

	"github.com/zeebo/xxh3"

	"github.com/kraytos17/cinder/internal/expiry"
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

// Stamp returns the row's LWW identity. MetaRow names the writer field
// Writer while Entry/Response call it WriterNodeHash — this constructor
// is where that mapping is reviewed once, instead of at every
// hand-written literal (a transposition would silently corrupt repair
// ordering, and every field is uint64/bool).
func (r MetaRow) Stamp() store.Stamp {
	return store.Stamp{Version: r.Version, Writer: r.Writer, Tombstone: r.Tombstone}
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

func EncodeDigest(digest []uint64) []byte {
	out := make([]byte, 0, 8+8*len(digest))
	out = codec.Append(out, binary.LittleEndian, MagicDigest)
	out = codec.AppendLen(out, binary.LittleEndian, len(digest))
	for _, h := range digest {
		out = codec.Append(out, binary.LittleEndian, h)
	}
	return out
}

// DecodeDigest parses an AEM2 blob with exact size validation.
func DecodeDigest(data []byte, expected int) ([]uint64, bool) {
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicDigest {
		return nil, false
	}

	n, ok := c.U32()
	if !ok || int(n) != expected || c.Remaining() != int(n)*8 {
		return nil, false
	}

	out := make([]uint64, 0, n)
	for range n {
		h, ok := c.U64()
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
			mat = codec.Append(mat, binary.LittleEndian, uint32(b))
			mat = codec.Append(mat, binary.LittleEndian, hashes[b])
		}
		out = append(out, xxh3.Hash(mat))
	}
	return out
}

// EncodeGroupDigest serializes an AEM3 group digest.
func EncodeGroupDigest(groups []uint64) []byte {
	out := make([]byte, 0, 8+8*len(groups))
	out = codec.Append(out, binary.LittleEndian, MagicGroupDigest)
	out = codec.AppendLen(out, binary.LittleEndian, len(groups))
	for _, h := range groups {
		out = codec.Append(out, binary.LittleEndian, h)
	}
	return out
}

// DecodeGroupDigest parses an AEM3 blob with exact size validation.
func DecodeGroupDigest(data []byte, expectedGroups int) ([]uint64, bool) {
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicGroupDigest {
		return nil, false
	}

	n, ok := c.U32()
	if !ok || int(n) != expectedGroups || c.Remaining() != int(n)*8 {
		return nil, false
	}

	out := make([]uint64, 0, n)
	for range n {
		h, ok := c.U64()
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
	out = codec.Append(out, binary.LittleEndian, MagicRefinement)
	if r.ForceMeta {
		out = codec.Append(out, binary.LittleEndian, uint32(1))
	} else {
		out = codec.Append(out, binary.LittleEndian, uint32(0))
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(r.BucketHashes))
	for _, bh := range r.BucketHashes {
		out = codec.Append(out, binary.LittleEndian, bh.Bucket)
		out = codec.Append(out, binary.LittleEndian, bh.Hash)
	}
	return out
}

// DecodeRefinement parses an AEB5 blob. Bucket ids must be < groups (the
// C++ bound: callers pass num_buckets_; ids beyond it are rejected).
func DecodeRefinement(data []byte, maxBucket uint32) (Refinement, bool) {
	var out Refinement
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicRefinement {
		return out, false
	}

	flags, ok := c.U32()
	if !ok || flags&^1 != 0 {
		return out, false
	}

	n, ok := c.U32()
	if !ok || c.Remaining() != int(n)*12 {
		return out, false
	}

	out.ForceMeta = flags&1 != 0
	for range n {
		b, ok := c.U32()
		if !ok || b >= maxBucket {
			return out, false
		}

		h, ok := c.U64()
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
	out = codec.AppendLen(out, binary.LittleEndian, len(rows))
	for _, r := range rows {
		out = codec.Append(out, binary.LittleEndian, r.Bucket)
		out = codec.Append(out, binary.LittleEndian, r.KeyHash)
		out = codec.Append(out, binary.LittleEndian, r.Version)
		out = codec.Append(out, binary.LittleEndian, r.Writer)
		if r.Tombstone {
			out = codec.Append(out, binary.LittleEndian, uint8(1))
		} else {
			out = codec.Append(out, binary.LittleEndian, uint8(0))
		}
	}
	return out
}

// DecodeMetaRows parses rows with exact size validation.
func DecodeMetaRows(data []byte) ([]MetaRow, bool) {
	c := codec.NewReader(binary.LittleEndian, data)
	n, ok := c.U32()
	if !ok || c.Remaining() != int(n)*29 {
		return nil, false
	}

	out := make([]MetaRow, 0, n)
	for range n {
		var r MetaRow
		var tomb uint8
		var ok bool
		if r.Bucket, ok = c.U32(); !ok {
			return nil, false
		}
		if r.KeyHash, ok = c.U64(); !ok {
			return nil, false
		}
		if r.Version, ok = c.U64(); !ok {
			return nil, false
		}
		if r.Writer, ok = c.U64(); !ok {
			return nil, false
		}
		if tomb, ok = c.U8(); !ok || tomb > 1 {
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
	out = codec.Append(out, binary.LittleEndian, MagicResponse)
	out = codec.AppendLen(out, binary.LittleEndian, len(r.GroupHashes))
	for _, h := range r.GroupHashes {
		out = codec.Append(out, binary.LittleEndian, h)
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(r.BucketHashes))
	for _, bh := range r.BucketHashes {
		out = codec.Append(out, binary.LittleEndian, bh.Bucket)
		out = codec.Append(out, binary.LittleEndian, bh.Hash)
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(r.Covered))
	for _, b := range r.Covered {
		out = codec.Append(out, binary.LittleEndian, b)
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(rows))
	out = append(out, rows...)
	out = codec.AppendLen(out, binary.LittleEndian, len(r.Sketches))
	for _, s := range r.Sketches {
		out = codec.Append(out, binary.LittleEndian, s.Bucket)
		out = codec.AppendLen(out, binary.LittleEndian, len(s.Blob))
		out = append(out, s.Blob...)
	}
	return out
}

// DecodeResponse parses an AER6 response: bucket ids, row coverage, and
// sketch uniqueness are all validated like the C++ decoder.
func DecodeResponse(data []byte, expectedGroups int) (DigestResponse, bool) {
	var out DigestResponse
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicResponse {
		return out, false
	}

	gc, ok := c.U32()
	if !ok || int(gc) != expectedGroups {
		return out, false
	}

	out.GroupHashes = make([]uint64, 0, gc)
	for range gc {
		h, ok := c.U64()
		if !ok {
			return out, false
		}
		out.GroupHashes = append(out.GroupHashes, h)
	}

	bhc, ok := c.U32()
	if !ok || c.Remaining() < int(bhc)*12 {
		return out, false
	}
	for range bhc {
		b, ok := c.U32()
		if !ok {
			return out, false
		}

		h, ok := c.U64()
		if !ok {
			return out, false
		}
		out.BucketHashes = append(out.BucketHashes, BucketHash{Bucket: b, Hash: h})
	}

	cc, ok := c.U32()
	if !ok || c.Remaining() < int(cc)*4+4 {
		return out, false
	}

	covered := map[uint32]bool{}
	for range cc {
		b, ok := c.U32()
		if !ok {
			return out, false
		}

		out.Covered = append(out.Covered, b)
		covered[b] = true
	}

	rs, ok := c.U32()
	if !ok || c.Remaining() < int(rs)+4 {
		return out, false
	}

	rowsBytes, ok := c.Bytes(int(rs))
	if !ok {
		return out, false
	}

	rows, ok := DecodeMetaRows(rowsBytes)
	if !ok {
		return out, false
	}

	out.Rows = rows
	sc, ok := c.U32()
	if !ok {
		return out, false
	}
	// C++ bound: sketch_count <= remaining / 8; decode-time exactness
	// below handles the rest.
	if uint64(sc) > uint64(c.Remaining())/8 {
		return out, false
	}

	seenSketch := map[uint32]bool{}
	for range sc {
		b, ok := c.U32()
		if !ok {
			return out, false
		}

		sz, ok := c.U32()
		if !ok {
			return out, false
		}

		blob, ok := c.Bytes(int(sz))
		if !ok || DecodeIBLT(blob) == nil {
			return out, false
		}
		if !covered[b] || seenSketch[b] {
			return out, false
		}

		seenSketch[b] = true
		out.Sketches = append(out.Sketches, SketchRef{Bucket: b, Blob: append([]byte(nil), blob...)})
	}
	if c.Remaining() != 0 {
		return out, false
	}

	sortedCovered := append([]uint32(nil), out.Covered...)
	slices.Sort(sortedCovered)
	contains := func(b uint32) bool {
		i, _ := slices.BinarySearch(sortedCovered, b)
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
		if !it.Entry.Expiry.IsNone() {
			size += 8
		}
		if size > MaxMetaBytes*4 {
			return nil, errTooLarge
		}
	}

	out := make([]byte, 0, size)
	out = codec.Append(out, binary.LittleEndian, MagicSync)
	out = codec.AppendLen(out, binary.LittleEndian, len(msg.Entries))
	for _, it := range msg.Entries {
		out = codec.AppendLen(out, binary.LittleEndian, len(it.Key))
		out = append(out, it.Key...)
		out = codec.Append(out, binary.LittleEndian, it.Entry.Version)
		out = codec.Append(out, binary.LittleEndian, it.Entry.WriterNodeHash)

		var flags uint8
		if !it.Entry.Expiry.IsNone() {
			flags |= 1
		}
		if it.Entry.Tombstone {
			flags |= 2
		}

		out = codec.Append(out, binary.LittleEndian, flags)
		if !it.Entry.Expiry.IsNone() {
			out = codec.Append(out, binary.LittleEndian, uint64(it.Entry.Expiry.UnixMilli()))
		}

		out = codec.AppendLen(out, binary.LittleEndian, len(it.Entry.Value))
		out = append(out, it.Entry.Value...)
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(msg.Needs))
	for _, h := range msg.Needs {
		out = codec.Append(out, binary.LittleEndian, h)
	}
	return out, nil
}

var errTooLarge = errors.New("ae: payload too large")

// DecodeSync parses an AEMS message with exact-size validation.
func DecodeSync(data []byte) (SyncMessage, bool) {
	var msg SyncMessage
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicSync {
		return msg, false
	}

	n, ok := c.U32()
	if !ok {
		return msg, false
	}
	for range n {
		var item SyncItem
		kl, ok := c.U32()
		if !ok {
			return msg, false
		}

		kb, ok := c.Bytes(int(kl))
		if !ok {
			return msg, false
		}

		item.Key = string(append([]byte(nil), kb...))
		var e store.Entry
		ver, ok := c.U64()
		if !ok {
			return msg, false
		}

		writer, ok := c.U64()
		if !ok {
			return msg, false
		}

		flags, ok := c.U8()
		if !ok || flags&^3 != 0 {
			return msg, false
		}

		e.Version, e.WriterNodeHash = ver, writer
		e.Tombstone = flags&2 != 0
		if flags&1 != 0 {
			ms, ok := c.U64()
			if !ok {
				return msg, false
			}
			e.Expiry = expiry.FromUnixMilli(int64(ms))
		}

		vl, ok := c.U32()
		if !ok {
			return msg, false
		}

		val, ok := c.Bytes(int(vl))
		if !ok {
			return msg, false
		}

		e.Value = append([]byte(nil), val...)
		item.Entry = e
		msg.Entries = append(msg.Entries, item)
	}

	nc, ok := c.U32()
	if !ok || c.Remaining() != int(nc)*8 {
		return msg, false
	}
	for range nc {
		h, ok := c.U64()
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
	out = codec.Append(out, binary.LittleEndian, MagicIBLT)
	out = codec.AppendLen(out, binary.LittleEndian, len(cells))
	for _, c := range cells {
		out = codec.Append(out, binary.LittleEndian, uint64(c.Count))
		out = codec.Append(out, binary.LittleEndian, c.KeyXOR)
		out = codec.Append(out, binary.LittleEndian, c.VersionXOR)
		out = codec.Append(out, binary.LittleEndian, c.WriterXOR)
		out = codec.Append(out, binary.LittleEndian, c.TombstoneXOR)
		out = codec.Append(out, binary.LittleEndian, c.ChecksumXOR)
	}
	return out
}

// DecodeIBLT parses a sketch with exact-size validation plus the C++
// count-bounds sanity check. Returns nil on any invalid input.
func DecodeIBLT(data []byte) []IBLTCell {
	c := codec.NewReader(binary.LittleEndian, data)
	magic, ok := c.U32()
	if !ok || magic != MagicIBLT {
		return nil
	}

	n, ok := c.U32()
	if !ok || int(n) < IBLTCellsMin || int(n) > IBLTCellsMax || c.Remaining() != int(n)*IBLTCelBytes {
		return nil
	}

	out := make([]IBLTCell, 0, n)
	for range n {
		bits, ok := c.U64()
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
		if cell.KeyXOR, ok = c.U64(); !ok {
			return nil
		}
		if cell.VersionXOR, ok = c.U64(); !ok {
			return nil
		}
		if cell.WriterXOR, ok = c.U64(); !ok {
			return nil
		}
		if cell.TombstoneXOR, ok = c.U64(); !ok {
			return nil
		}
		if cell.ChecksumXOR, ok = c.U64(); !ok {
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
