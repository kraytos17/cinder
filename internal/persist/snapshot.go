package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// Snapshot entry framing (frozen CSNP layout, v2): klen u32, key, vlen
// u32, value, version u64, writer u64, expires_ms u64, has_ttl u8,
// freq u64, tombstone u8. V1 omits the tombstone byte; V3 (compressed
// blocks) is rejected — writers here emit V2 uncompressed, matching the
// C++ default.
type SnapEntry struct {
	Key       []byte
	Value     []byte
	Version   uint64
	Writer    uint64
	ExpiresMs uint64
	HasTTL    bool
	Tombstone bool
	Freq      uint64 // LFU frequency; 0 for LRU
}

// SnapData is a decoded snapshot body.
type SnapData struct {
	NextVersion uint64
	Entries     []SnapEntry
}

func encodeSnapEntry(e SnapEntry) ([]byte, error) {
	if uint64(len(e.Key)) > maxU32 || uint64(len(e.Value)) > maxU32 {
		return nil, errTooLarge
	}
	out := make([]byte, 0, 4+len(e.Key)+4+len(e.Value)+8+8+8+1+8+1)
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
	out = putU64(out, e.Freq)
	if e.Tombstone {
		out = putU8(out, 1)
	} else {
		out = putU8(out, 0)
	}
	return out, nil
}

// SnapWriter writes one atomic snapshot: .tmp file + rename, like C++.
type SnapWriter struct {
	path string
}

// NewSnapWriter returns a writer for path.
func NewSnapWriter(path string) *SnapWriter { return &SnapWriter{path: path} }

// Write serializes nextVersion + entries as v2 uncompressed and publishes
// atomically via rename.
func (w *SnapWriter) Write(nextVersion uint64, entries []SnapEntry) error {
	var body []byte
	for _, e := range entries {
		buf, err := encodeSnapEntry(e)
		if err != nil {
			return err
		}
		body = append(body, buf...)
	}
	hdr := make([]byte, 0, 20)
	hdr = putU32(hdr, snapMagic)
	hdr = putU32(hdr, snapVersion)
	hdr = putU64(hdr, nextVersion)
	hdr = putU32(hdr, uint32(len(entries)))
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, append(hdr, body...), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}

// snapFormat enumerates accepted snapshot layouts.
type snapFormat uint32

const (
	snapV1 snapFormat = 1
	snapV2 snapFormat = 2
	snapV3 snapFormat = 3
)

var errUnsupportedSnapshot = errors.New("persist: compressed (v3) snapshots unsupported")

// SnapReader decodes snapshots. V1/V2 uncompressed read fully;
// V3 (compressed blocks) fails cleanly.
type SnapReader struct {
	path string
}

// NewSnapReader returns a reader for path.
func NewSnapReader(path string) *SnapReader { return &SnapReader{path: path} }

// ReadAll decodes the whole snapshot.
func (r *SnapReader) ReadAll() (SnapData, error) {
	var data SnapData
	next, err := r.ReadStream(func(e SnapEntry) bool {
		data.Entries = append(data.Entries, e)
		return true
	})
	if err != nil {
		return SnapData{}, err
	}
	data.NextVersion = next
	return data, nil
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

// decodeSnapshot is the byte-level ReadStream core (file I/O split out so
// fuzz can feed arbitrary images without temp files per input).
func decodeSnapshot(raw []byte, fn func(SnapEntry) bool) (uint64, error) {
	if len(raw) < 20 {
		return 0, fmt.Errorf("persist: truncated snapshot header")
	}
	if binary.LittleEndian.Uint32(raw[:4]) != snapMagic {
		return 0, fmt.Errorf("persist: invalid snapshot magic")
	}
	var format snapFormat
	switch v := binary.LittleEndian.Uint32(raw[4:8]); v {
	case 1:
		format = snapV1
	case 2:
		format = snapV2
	case 3:
		return 0, errUnsupportedSnapshot
	default:
		return 0, fmt.Errorf("persist: unsupported snapshot version %d", v)
	}
	nextVersion := binary.LittleEndian.Uint64(raw[8:16])
	count := binary.LittleEndian.Uint32(raw[16:20])
	c := &cursor{buf: raw}
	c.off = 20
	for i := range count {
		e, ok := decodeSnapEntry(c, format)
		if !ok {
			return 0, fmt.Errorf("persist: truncated snapshot entry %d", i)
		}
		if !fn(e) {
			break
		}
	}
	return nextVersion, nil
}

func decodeSnapEntry(c *cursor, format snapFormat) (SnapEntry, bool) {
	var e SnapEntry
	klen, ok := c.u32()
	if !ok {
		return e, false
	}
	key, ok := c.bytes(int(klen))
	if !ok {
		return e, false
	}
	vlen, ok := c.u32()
	if !ok {
		return e, false
	}
	val, ok := c.bytes(int(vlen))
	if !ok {
		return e, false
	}
	ver, ok := c.u64()
	if !ok {
		return e, false
	}
	writer, ok := c.u64()
	if !ok {
		return e, false
	}
	expires, ok := c.u64()
	if !ok {
		return e, false
	}
	hasTTL, ok := c.u8()
	if !ok {
		return e, false
	}
	freq, ok := c.u64()
	if !ok {
		return e, false
	}
	var tomb uint8
	if format >= snapV2 {
		tomb, ok = c.u8()
		if !ok || tomb > 1 {
			return e, false
		}
	}
	e.Key = append([]byte(nil), key...)
	e.Value = append([]byte(nil), val...)
	e.Version, e.Writer, e.ExpiresMs = ver, writer, expires
	e.HasTTL = hasTTL != 0
	e.Freq = freq
	e.Tombstone = tomb != 0
	return e, true
}
