package persist

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/kraytos17/cinder/internal/codec"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/expiry"
)

// SnapEntry is one decoded snapshot row: key, value, LWW identity,
// TTL deadline, LFU frequency, and tombstone marker. Framing is the
// frozen CSNP layout v2 (klen u32, key, vlen u32, value, version u64,
// writer u64, expires_ms u64, has_ttl u8, freq u64, tombstone u8); V1
// omits the tombstone byte; V3 compressed blocks are rejected and V4
// carries the counter bundle alongside the KV rows.
type SnapEntry struct {
	Key     []byte
	Value   []byte
	Version uint64
	Writer  uint64
	// Expiry is the TTL deadline; zero means none. On disk this is
	// still (u64 ms, u8 flag), converted by the codec below.
	Expiry    expiry.Expiry
	Tombstone bool
	Freq      uint64 // LFU frequency; 0 for LRU
}

// SnapData is a decoded snapshot body.
type SnapData struct {
	NextVersion uint64
	Entries     []SnapEntry
	Counters    []counter.BundleEntry
}

func (e SnapEntry) encode() ([]byte, error) {
	if uint64(len(e.Key)) > maxU32 || uint64(len(e.Value)) > maxU32 {
		return nil, errTooLarge
	}

	out := make([]byte, 0, 4+len(e.Key)+4+len(e.Value)+8+8+8+1+8+1)
	return e.appendInto(out)
}

// appendInto serializes one row onto out without an intermediate buffer.
// It lets snapshot streaming reuse a scratch slice across entries.
func (e SnapEntry) appendInto(out []byte) ([]byte, error) {
	if uint64(len(e.Key)) > maxU32 || uint64(len(e.Value)) > maxU32 {
		return nil, errTooLarge
	}

	out = codec.AppendLen(out, binary.LittleEndian, len(e.Key))
	out = append(out, e.Key...)
	out = codec.AppendLen(out, binary.LittleEndian, len(e.Value))
	out = append(out, e.Value...)
	out = codec.Append(out, binary.LittleEndian, e.Version)
	out = codec.Append(out, binary.LittleEndian, e.Writer)
	out = codec.Append(out, binary.LittleEndian, uint64(e.Expiry.UnixMilli()))
	if e.Expiry.IsNone() {
		out = codec.Append(out, binary.LittleEndian, uint8(0))
	} else {
		out = codec.Append(out, binary.LittleEndian, uint8(1))
	}

	out = codec.Append(out, binary.LittleEndian, e.Freq)
	if e.Tombstone {
		out = codec.Append(out, binary.LittleEndian, uint8(1))
	} else {
		out = codec.Append(out, binary.LittleEndian, uint8(0))
	}
	return out, nil
}

// SnapWriter writes a snapshot to a temporary file and renames it into
// place only after the complete snapshot has been written.
type SnapWriter struct {
	path string
}

// WriteWithCounters publishes a snapshot containing both LWW entries and
// counter CRDT state. The legacy Write method keeps producing snapshots with
// an empty counter section.
func (w *SnapWriter) WriteWithCounters(nextVersion uint64, entries []SnapEntry, counters []counter.BundleEntry) error {
	return w.writeV4(nextVersion, entries, counters)
}

// NewSnapWriter returns a writer for path.
func NewSnapWriter(path string) *SnapWriter { return &SnapWriter{path: path} }

// Write serializes nextVersion + entries as v2 uncompressed and publishes
// atomically via rename. Entries stream through a buffered writer so peak
// memory stays O(buffer) instead of O(snapshot); the tmp file is synced
// before rename and the directory is synced after, so a published snapshot
// survives a crash (not just a clean shutdown).
func (w *SnapWriter) Write(nextVersion uint64, entries []SnapEntry) error {
	hdr := make([]byte, 0, 20)
	hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapMagic))
	hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapVersion))
	hdr = codec.Append(hdr, binary.LittleEndian, nextVersion)
	hdr = codec.AppendLen(hdr, binary.LittleEndian, len(entries))
	return w.stream(hdr, entries, nil)
}

func (w *SnapWriter) writeV4(nextVersion uint64, entries []SnapEntry, counters []counter.BundleEntry) error {
	counters = slices.Clone(counters)
	slices.SortFunc(counters, func(a, b counter.BundleEntry) int { return strings.Compare(a.Key, b.Key) })
	encodedCounters := counter.EncodeCounterBundle(counters)
	if encodedCounters == nil && len(counters) != 0 {
		return fmt.Errorf("persist: invalid counter snapshot state")
	}
	if len(counters) == 0 {
		encodedCounters = []byte{0x43, 0x41, 0x45, 0x31, 0, 0, 0, 0} // "CAE1", count=0
	}

	hdr := make([]byte, 0, 24)
	hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapMagic))
	hdr = codec.Append(hdr, binary.LittleEndian, uint32(snapV4))
	hdr = codec.Append(hdr, binary.LittleEndian, nextVersion)
	hdr = codec.AppendLen(hdr, binary.LittleEndian, len(entries))
	hdr = codec.AppendLen(hdr, binary.LittleEndian, len(encodedCounters))
	return w.stream(hdr, entries, encodedCounters)
}

// stream writes hdr, then every entry, then the optional counter trailer,
// to a tmp file with a single buffered writer. One scratch slice is reused
// across entries so per-entry allocation is a single grow of the encoded
// row, not a throwaway buffer plus a body append.
func (w *SnapWriter) stream(hdr []byte, entries []SnapEntry, trailer []byte) error {
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	bw := bufio.NewWriterSize(f, 1<<20)
	var scratch []byte
	failed := true
	defer func() {
		if failed {
			f.Close()
			os.Remove(tmp)
		}
	}()

	if _, err := bw.Write(hdr); err != nil {
		return err
	}
	for _, e := range entries {
		scratch, err = e.appendInto(scratch[:0])
		if err != nil {
			return err
		}
		if _, err := bw.Write(scratch); err != nil {
			return err
		}
	}
	if len(trailer) != 0 {
		if _, err := bw.Write(trailer); err != nil {
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return err
	}

	failed = false
	return fsyncDir(filepath.Dir(w.path))
}

// fsyncDir persists directory entries (new files, renames, unlinks) so a
// just-published snapshot or CURRENT pointer survives a crash. Best-effort
// on platforms where opening a directory is unsupported: callers treat a
// nil return as durable and log a non-nil one without failing the publish.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	defer d.Close()
	return d.Sync()
}

// snapFormat enumerates accepted snapshot layouts.
type snapFormat uint32

const (
	snapV1 snapFormat = 1
	snapV2 snapFormat = 2
	snapV3 snapFormat = 3
	snapV4 snapFormat = 4
)

var errUnsupportedSnapshot = errors.New("persist: compressed (v3) snapshots unsupported")

// SnapReader decodes snapshots. V1/V2 uncompressed read fully;
// V3 (compressed blocks) fails cleanly.
type SnapReader struct {
	path string
}

// NewSnapReader returns a reader for path.
func NewSnapReader(path string) *SnapReader { return &SnapReader{path: path} }

// ReadAll decodes the whole snapshot with a single file read and a single
// decode pass over the KV section.
func (r *SnapReader) ReadAll() (SnapData, error) {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		return SnapData{}, err
	}
	return decodeSnapshotAll(raw)
}

// decodeSnapshotAll is the single-pass ReadAll core: entries stream once,
// then the v4 counter bundle decodes from the same cursor without
// re-walking KV rows.
func decodeSnapshotAll(raw []byte) (SnapData, error) {
	var data SnapData
	next, counters, err := decodeSnapshotCollect(raw, func(e SnapEntry) bool {
		data.Entries = append(data.Entries, e)
		return true
	})
	if err != nil {
		return SnapData{}, err
	}

	data.NextVersion = next
	data.Counters = counters
	return data, nil
}

// decodeSnapshotCollect decodes headers + KV rows via fn, then decodes (but
// does not re-walk) the v4 counter bundle from the same cursor. It is the
// shared core for decodeSnapshot (streaming, counter-validating) and
// decodeSnapshotAll (collecting + returning counters).
func decodeSnapshotCollect(raw []byte, fn func(SnapEntry) bool) (uint64, []counter.BundleEntry, error) {
	if len(raw) < 20 {
		return 0, nil, fmt.Errorf("persist: truncated snapshot header at offset %d", len(raw))
	}
	if binary.LittleEndian.Uint32(raw[:4]) != snapMagic {
		return 0, nil, fmt.Errorf("persist: invalid snapshot magic at offset 0")
	}

	var format snapFormat
	switch v := binary.LittleEndian.Uint32(raw[4:8]); v {
	case 1:
		format = snapV1
	case 2:
		format = snapV2
	case 3:
		return 0, nil, errUnsupportedSnapshot
	case 4:
		format = snapV4
	default:
		return 0, nil, fmt.Errorf("persist: unsupported snapshot version %d at offset 4", v)
	}

	nextVersion := binary.LittleEndian.Uint64(raw[8:16])
	count := binary.LittleEndian.Uint32(raw[16:20])
	counterOffset := uint32(0)
	if format == snapV4 {
		if len(raw) < 24 {
			return 0, nil, fmt.Errorf("persist: truncated v4 snapshot header at offset %d", len(raw))
		}
		counterOffset = binary.LittleEndian.Uint32(raw[20:24])
	}

	c := codec.NewReader(binary.LittleEndian, raw)
	if format == snapV4 {
		c.SetPos(24)
	} else {
		c.SetPos(20)
	}

	stopped := false
	for i := range count {
		e, ok := decodeSnapEntry(&c, format)
		if !ok {
			return 0, nil, fmt.Errorf("persist: truncated snapshot entry %d at offset %d", i, c.Pos())
		}
		if !fn(e) {
			stopped = true
			break
		}
	}

	var counters []counter.BundleEntry
	if format == snapV4 && !stopped {
		counterStart := c.Pos()
		counterEnd := counterStart + int(counterOffset)
		if counterEnd > len(raw) {
			return 0, nil, fmt.Errorf("persist: truncated counter snapshot section at offset %d", counterStart)
		}

		bundle := raw[counterStart:counterEnd]
		if len(bundle) < 8 || binary.BigEndian.Uint32(bundle[:4]) != counter.MagicCAE1 {
			return 0, nil, fmt.Errorf("persist: invalid counter snapshot magic at offset %d", counterStart)
		}

		bcount := binary.BigEndian.Uint32(bundle[4:8])
		if bcount == 0 && len(bundle) == 8 {
			// Empty counter bundle is valid for a counter-enabled node.
		} else {
			decoded, err := counter.DecodeCounterBundle(bundle)
			if err != nil {
				return 0, nil, fmt.Errorf("persist: invalid counter snapshot section at offset %d: %w", counterStart, err)
			}
			counters = decoded
		}
		c.SetPos(counterEnd)
	}
	if !stopped && c.Remaining() != 0 {
		return 0, nil, fmt.Errorf("persist: trailing snapshot bytes at offset %d", c.Pos())
	}
	return nextVersion, counters, nil
}

// ReadStream invokes fn per entry without accumulating. Returning false
// stops early. The returned version is the header's next_version.
func (r *SnapReader) ReadStream(fn func(SnapEntry) bool) (uint64, error) {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		return 0, err
	}
	return decodeSnapshot(raw, fn)
}

// ReadCounters decodes the optional v4 counter section. Older snapshots
// return no counters. It re-reads the file for callers that only need
// counters; ReadAll and recovery use the single-pass collect path.
func (r *SnapReader) ReadCounters() ([]counter.BundleEntry, error) {
	raw, err := os.ReadFile(r.path)
	if err != nil {
		return nil, err
	}
	return decodeSnapshotCounters(raw)
}

// decodeSnapshot is the byte-level ReadStream core (file I/O split out so
// fuzz can feed arbitrary images without temp files per input).
func decodeSnapshot(raw []byte, fn func(SnapEntry) bool) (uint64, error) {
	next, _, err := decodeSnapshotCollect(raw, fn)
	return next, err
}

func decodeSnapshotCounters(raw []byte) ([]counter.BundleEntry, error) {
	if len(raw) < 20 || binary.LittleEndian.Uint32(raw[:4]) != snapMagic {
		return nil, fmt.Errorf("persist: invalid snapshot header")
	}

	version := binary.LittleEndian.Uint32(raw[4:8])
	if version != uint32(snapV4) {
		return nil, nil
	}
	if len(raw) < 24 {
		return nil, fmt.Errorf("persist: truncated v3 snapshot header")
	}

	kvCount := binary.LittleEndian.Uint32(raw[16:20])
	counterSize := binary.LittleEndian.Uint32(raw[20:24])
	c := codec.NewReader(binary.LittleEndian, raw)
	c.SetPos(24)
	for range kvCount {
		if _, ok := decodeSnapEntry(&c, snapV4); !ok {
			return nil, fmt.Errorf("persist: truncated v3 snapshot KV section")
		}
	}

	end := c.Pos() + int(counterSize)
	if end > len(raw) {
		return nil, fmt.Errorf("persist: truncated counter snapshot section")
	}

	bundle := raw[c.Pos():end]
	if len(bundle) == 8 && binary.BigEndian.Uint32(bundle[:4]) == counter.MagicCAE1 && binary.BigEndian.Uint32(bundle[4:8]) == 0 {
		return nil, nil
	}
	return counter.DecodeCounterBundle(bundle)
}

func decodeSnapEntry(c *codec.Reader, format snapFormat) (SnapEntry, bool) {
	var e SnapEntry
	klen, ok := c.U32()
	if !ok {
		return e, false
	}

	key, ok := c.Bytes(int(klen))
	if !ok {
		return e, false
	}

	vlen, ok := c.U32()
	if !ok {
		return e, false
	}

	val, ok := c.Bytes(int(vlen))
	if !ok {
		return e, false
	}

	ver, ok := c.U64()
	if !ok {
		return e, false
	}

	writer, ok := c.U64()
	if !ok {
		return e, false
	}

	expires, ok := c.U64()
	if !ok {
		return e, false
	}

	hasTTL, ok := c.U8()
	if !ok {
		return e, false
	}

	freq, ok := c.U64()
	if !ok {
		return e, false
	}

	var tomb uint8
	if format >= snapV2 {
		tomb, ok = c.U8()
		if !ok || tomb > 1 {
			return e, false
		}
	}

	kCopy, vCopy := clonePair(key, val)
	e.Key = kCopy
	e.Value = vCopy
	e.Version, e.Writer = ver, writer
	if hasTTL != 0 {
		// At, not FromUnixMilli: a set flag with ms=0 still means an
		// (already elapsed) deadline, matching the old handling.
		e.Expiry = expiry.At(time.UnixMilli(int64(expires)).UTC())
	}

	e.Freq = freq
	e.Tombstone = tomb != 0
	return e, true
}
