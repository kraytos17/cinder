// Package persist implements Cinder's on-disk formats (Go port): the WAL
// ("WAL0", little-endian checksummed records) and snapshots ("CSNP").
//
// Files are byte-identical with the C++ writers, so a Go node can replay a
// C++ WAL/snapshot and vice versa. docs: include/cinder/store/wal.hpp,
// snapshot.hpp; AGENTS.md rule: the WAL buffer layout and checksums must
// stay byte-identical (fuzz: wal, snapshot, store_put).
package persist

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"

	"github.com/zeebo/xxh3"
)

const (
	walMagic   = 0x57414C30 // "WAL0"
	walVersion = 2

	snapMagic   = 0x43534E50 // "CSNP"
	snapVersion = 2

	maxU32 = uint64(1<<32 - 1)
)

// Op mirrors WalEntry::Op: the WAL record kind.
type Op uint8

const (
	// OpSet appends (or overwrites) a live value.
	OpSet Op = 1
	// OpDel hard-erases a key on replay.
	OpDel Op = 2
)

// Entry is one WAL record. Tombstoned Sets replay as versioned deletes;
// Del replays as a hard remove (never resurrects markers).
type Entry struct {
	Op        Op
	Key       []byte
	Value     []byte
	Version   uint64
	Writer    uint64
	ExpiresMs uint64 // wall-clock ms since epoch, 0 if no TTL
	HasTTL    bool
	Tombstone bool
}

func putU8(b []byte, v uint8) []byte   { return append(b, v) }
func putU32(b []byte, v uint32) []byte { return binary.LittleEndian.AppendUint32(b, v) }
func putU64(b []byte, v uint64) []byte { return binary.LittleEndian.AppendUint64(b, v) }

// encodeRecord serializes one v2 record plus its XXH3 checksum. The
// checksum covers exactly the on-disk record bytes, like the C++
// encodeWalRecord.
func encodeRecord(e Entry) ([]byte, error) {
	if uint64(len(e.Key)) > maxU32 || uint64(len(e.Value)) > maxU32 {
		return nil, errTooLarge
	}
	size := 1 + 4 + len(e.Key) + 4 + len(e.Value) + 8 + 8 + 8 + 1 + 1 + 8
	out := make([]byte, 0, size)
	start := len(out)
	out = putU8(out, uint8(e.Op))
	out = putU32(out, uint32(len(e.Key)))
	out = append(out, e.Key...)
	out = putU32(out, uint32(len(e.Value)))
	out = append(out, e.Value...)
	out = putU64(out, e.Version)
	out = putU64(out, e.Writer)
	out = putU64(out, e.ExpiresMs)
	if e.HasTTL {
		out = putU8(out, 1)
	} else {
		out = putU8(out, 0)
	}
	if e.Tombstone {
		out = putU8(out, 1)
	} else {
		out = putU8(out, 0)
	}
	out = putU64(out, xxh3.Hash(out[start:]))
	return out, nil
}

var errTooLarge = errors.New("persist: WAL entry too large")

// Writer appends checksummed v2 records. Open with Truncate for a fresh
// log or Append to resume across restarts (missing/empty file gets a fresh
// header; a present-but-invalid header truncates to a fresh log).
type Writer struct {
	mu sync.Mutex
	f  *os.File
	// size tracks appended bytes for the auto-compact budget (initialized
	// from the file on open-for-append).
	size int64
	// entries counts appended records for the entry-count budget.
	// Resumed logs start at 0: the byte budget (initialized from disk)
	// still bounds recovery replay, and the count converges after the
	// next compact rotates to a fresh file.
	entries uint64
}

// OpenWriter opens path for records. Truncate starts fresh; Append resumes.
func OpenWriter(path string, truncate bool) (*Writer, error) {
	var f *os.File
	var err error
	if truncate {
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, err
		}
		if err := writeHeader(f); err != nil {
			f.Close()
			return nil, err
		}
		return &Writer{f: f, size: 8}, nil
	}
	f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	// Probe the existing header like the C++ Append path.
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	needHeader := false
	if fi.Size() == 0 {
		needHeader = true
	} else {
		hdr := make([]byte, 8)
		rf, err := os.Open(path)
		if err != nil {
			f.Close()
			return nil, err
		}
		n, _ := rf.ReadAt(hdr, 0)
		rf.Close()
		if n < 8 || binary.LittleEndian.Uint32(hdr[:4]) != walMagic || !validWalVersion(binary.LittleEndian.Uint32(hdr[4:])) {
			needHeader = true // garbage file: start fresh
			f.Close()
			f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
			if err != nil {
				return nil, err
			}
		}
	}
	if needHeader {
		if err := writeHeader(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	// Size from disk: resumed logs count what is already there.
	var size int64
	if fi, err := f.Stat(); err == nil {
		size = fi.Size()
	}
	return &Writer{f: f, size: size}, nil
}

func validWalVersion(v uint32) bool { return v == 1 || v == 2 }

func writeHeader(f *os.File) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[:4], walMagic)
	binary.LittleEndian.PutUint32(hdr[4:], walVersion)
	_, err := f.Write(hdr[:])
	return err
}

// Append encodes one record and lands it with a single write. Like C++
// (O_APPEND, no userspace buffering), the bytes are in the kernel on
// return; only OS-crash durability needs Sync.
func (w *Writer) Append(e Entry) error {
	buf, err := encodeRecord(e)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.f.Write(buf)
	w.size += int64(n)
	if err == nil {
		w.entries++
	}
	return err
}

// AppendBatch encodes the whole batch first, then lands it with one write.
// On-disk bytes equal N sequential appends. Fails atomically before
// writing anything when an entry is oversized.
func (w *Writer) AppendBatch(es []Entry) (int, error) {
	if len(es) == 0 {
		return 0, nil
	}
	bufs := make([][]byte, len(es))
	total := 0
	for i, e := range es {
		buf, err := encodeRecord(e)
		if err != nil {
			return 0, err
		}
		bufs[i] = buf
		total += len(buf)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, buf := range bufs {
		n, err := w.f.Write(buf)
		w.size += int64(n)
		if err != nil {
			return 0, err
		}
		w.entries++
	}
	return total, nil
}

// Size reports appended bytes (initialized from disk on open-for-append),
// the auto-compact budget input. Over by at most one torn tail, which the
// reader truncates on recovery.
func (w *Writer) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Entries reports appended records since (re)creation, the entry-count
// budget input.
func (w *Writer) Entries() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entries
}

// Sync flushes kernel buffers to disk (fdatasync equivalent).
func (w *Writer) Sync() error { return w.f.Sync() }

// Close closes the file.
func (w *Writer) Close() error { return w.f.Close() }

// Name returns the file path.
func (w *Writer) Name() string { return w.f.Name() }

// Format enumerates accepted WAL layouts. Writers emit V2; readers accept
// Legacy (pre-magic, no checksums), V1, and V2.
type Format uint32

const (
	// FormatLegacy is the pre-magic WAL without checksums.
	FormatLegacy Format = 0
	// FormatV1 adds the magic header with checksummed records (no tombstone byte).
	FormatV1 Format = 1
	// FormatV2 is the current layout: checksums plus tombstone byte.
	FormatV2 Format = 2
)

// Reader replays one WAL file from a whole-file image. next() yields
// records until clean EOF or a torn tail; Corrupted reports structural
// invalidity (bad op, bad tombstone byte, checksum mismatch). ValidSize is
// the end offset of the last fully decoded record for torn-tail truncation.
type Reader struct {
	data      []byte
	pos       int
	commit    uint64
	format    Format
	corrupted bool
}

// OpenReader slurps path once; a missing file reads as empty.
func OpenReader(path string) (*Reader, error) {
	r := &Reader{}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return r, nil
		}
		return nil, err
	}
	r.data = data
	if len(data) < 4 {
		return r, nil // empty image: nothing to decode
	}
	if binary.LittleEndian.Uint32(data[:4]) != walMagic {
		return r, nil // legacy format: decode from 0, no checksums
	}
	if len(data) < 8 {
		return r, nil // torn header: nothing to decode
	}
	switch v := binary.LittleEndian.Uint32(data[4:8]); v {
	case 1:
		r.format = FormatV1
		r.pos = 8
	case 2:
		r.format = FormatV2
		r.pos = 8
	default:
		// Unknown version: fall through to legacy decoding from 0.
	}
	return r, nil
}

// HasMore reports buffered bytes remain.
func (r *Reader) HasMore() bool { return r.pos < len(r.data) }

// ValidSize is the end offset of the last fully decoded record.
func (r *Reader) ValidSize() uint64 { return r.commit }

// Corrupted reports structural invalidity seen by Next.
func (r *Reader) Corrupted() bool { return r.corrupted }

type cursor struct {
	buf []byte
	off int
}

func (c *cursor) u8() (uint8, bool) {
	if len(c.buf)-c.off < 1 {
		return 0, false
	}
	v := c.buf[c.off]
	c.off++
	return v, true
}

func (c *cursor) u32() (uint32, bool) {
	if len(c.buf)-c.off < 4 {
		return 0, false
	}
	v := binary.LittleEndian.Uint32(c.buf[c.off:])
	c.off += 4
	return v, true
}

func (c *cursor) u64() (uint64, bool) {
	if len(c.buf)-c.off < 8 {
		return 0, false
	}
	v := binary.LittleEndian.Uint64(c.buf[c.off:])
	c.off += 8
	return v, true
}

func (c *cursor) bytes(n int) ([]byte, bool) {
	if n < 0 || len(c.buf)-c.off < n {
		return nil, false
	}
	v := c.buf[c.off : c.off+n : c.off+n]
	c.off += n
	return v, true
}

// Next decodes one record. Truncation (torn crash tail) stops cleanly
// without setting Corrupted; semantic invalidity sets it.
func (r *Reader) Next() (Entry, bool) {
	var zero Entry
	c := &cursor{buf: r.data}
	c.off = r.pos
	start := c.off
	op, ok := c.u8()
	if !ok {
		return zero, false // clean EOF
	}
	if op != uint8(OpSet) && op != uint8(OpDel) {
		r.corrupted = true
		r.pos = c.off
		return zero, false
	}
	klen, ok := c.u32()
	if !ok {
		return zero, false
	}
	key, ok := c.bytes(int(klen))
	if !ok {
		return zero, false
	}
	vlen, ok := c.u32()
	if !ok {
		return zero, false
	}
	val, ok := c.bytes(int(vlen))
	if !ok {
		return zero, false
	}
	ver, ok := c.u64()
	if !ok {
		return zero, false
	}
	writer, ok := c.u64()
	if !ok {
		return zero, false
	}
	expires, ok := c.u64()
	if !ok {
		return zero, false
	}
	hasTTL, ok := c.u8()
	if !ok {
		return zero, false
	}
	var tomb uint8
	if r.format == FormatV2 {
		tomb, ok = c.u8()
		if !ok {
			return zero, false
		}
		if tomb > 1 {
			r.corrupted = true
			r.pos = c.off
			return zero, false
		}
	}
	if r.format != FormatLegacy {
		sum, ok := c.u64()
		if !ok {
			return zero, false // truncated checksum: torn tail
		}
		if xxh3.Hash(r.data[start:c.off-8]) != sum {
			r.corrupted = true
			r.pos = c.off
			return zero, false
		}
	}
	e := Entry{
		Op: Op(op), Key: append([]byte(nil), key...), Value: append([]byte(nil), val...),
		Version: ver, Writer: writer, ExpiresMs: expires,
		HasTTL: hasTTL != 0, Tombstone: tomb != 0,
	}
	r.pos = c.off
	r.commit = uint64(c.off)
	return e, true
}
