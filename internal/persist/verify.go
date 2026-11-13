package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/kraytos17/cinder/internal/codec"
	"github.com/kraytos17/cinder/internal/counter"
)

// FileReport describes one persisted file without changing it.
type FileReport struct {
	Path           string
	Kind           string
	Format         string
	Bytes          int64
	KVRecords      uint64
	CounterRecords uint64
	ValidBytes     uint64
	TornTail       bool
	Error          string
	ErrorOffset    int64
}

// VerifyReport summarizes a read-only data-directory scan.
type VerifyReport struct {
	Directory      string
	Files          []FileReport
	KVRecords      uint64
	CounterRecords uint64
	Errors         []string
	TornTails      []string
}

// VerifyDataDir inspects recognized snapshots and WAL files in dir. It never
// truncates, upgrades, or otherwise modifies input files. A torn WAL tail is
// reported separately from corruption because normal recovery truncates it.
func VerifyDataDir(dir string) (VerifyReport, error) {
	report := VerifyReport{Directory: dir}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return report, err
	}

	var walPaths, snapshots []string
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(dir, name)
		if name == walFileName || isWALSegmentName(name) {
			walPaths = append(walPaths, path)
		}
		if isSnapshotName(name) {
			snapshots = append(snapshots, path)
		}
	}

	slices.Sort(walPaths)
	slices.Sort(snapshots)
	for _, path := range snapshots {
		file := verifySnapshot(path)
		report.add(file)
	}
	for _, path := range walPaths {
		file := verifyWAL(path)
		report.add(file)
	}
	return report, nil
}

func (r *VerifyReport) add(file FileReport) {
	r.Files = append(r.Files, file)
	r.KVRecords += file.KVRecords
	r.CounterRecords += file.CounterRecords
	if file.TornTail {
		r.TornTails = append(r.TornTails, file.Path)
	}
	if file.Error != "" {
		r.Errors = append(r.Errors, file.Error)
	}
}

func verifyWAL(path string) FileReport {
	file := FileReport{Path: path, Kind: "wal", ErrorOffset: -1}
	info, err := os.Stat(path)
	if err != nil {
		file.Error = fmt.Sprintf("%s: stat: %v", path, err)
		return file
	}

	file.Bytes = info.Size()
	raw, err := os.ReadFile(path)
	if err != nil {
		file.Error = fmt.Sprintf("%s: read: %v", path, err)
		return file
	}
	if len(raw) >= 4 && binary.LittleEndian.Uint32(raw[:4]) == walMagic {
		if len(raw) < 8 {
			file.Format = "header-torn"
			file.TornTail = true
			file.ErrorOffset = 0
			return file
		}

		version := binary.LittleEndian.Uint32(raw[4:8])
		if !validWalVersion(version) {
			file.Format = fmt.Sprintf("v%d", version)
			file.ErrorOffset = 4
			file.Error = fmt.Sprintf("%s: unsupported WAL version %d at offset 4", path, version)
			return file
		}
	}

	r, err := OpenReader(path)
	if err != nil {
		file.Error = fmt.Sprintf("%s: open: %v", path, err)
		return file
	}

	file.Format = walFormatName(r.format)
	if err := r.HeaderError(); err != nil {
		file.ErrorOffset = r.ErrorOffset()
		file.Error = fmt.Sprintf("%s: %v at offset %d", path, err, file.ErrorOffset)
		return file
	}
	for r.HasMore() {
		e, ok := r.Next()
		if !ok {
			if r.Corrupted() {
				file.ErrorOffset = r.ErrorOffset()
				file.Error = fmt.Sprintf("%s: corrupt WAL record at offset %d", path, file.ErrorOffset)
			} else {
				file.TornTail = true
				file.ErrorOffset = int64(r.ValidSize())
			}
			break
		}
		switch e.Op {
		case OpSet, OpDel:
			file.KVRecords++
		case OpCounterState, OpCounterRemove:
			file.CounterRecords++
			if e.Op == OpCounterState {
				if _, err := counter.DecodePNCounter(e.Counter); err != nil {
					file.ErrorOffset = int64(r.ValidSize())
					file.Error = fmt.Sprintf("%s: invalid counter state at record ending offset %d: %v", path, file.ErrorOffset, err)
				}
			}
		}
	}

	file.ValidBytes = r.ValidSize()
	if !file.TornTail && file.Error == "" {
		file.ValidBytes = uint64(file.Bytes)
	}
	return file
}

func walFormatName(format Format) string {
	switch format {
	case FormatLegacy:
		return "legacy"
	case FormatV1:
		return "v1"
	case FormatV2:
		return "v2"
	case FormatV3:
		return "v3"
	default:
		return "unknown"
	}
}

func verifySnapshot(path string) FileReport {
	file := FileReport{Path: path, Kind: "snapshot", ErrorOffset: -1}
	info, err := os.Stat(path)
	if err != nil {
		file.Error = fmt.Sprintf("%s: stat: %v", path, err)
		return file
	}

	file.Bytes = info.Size()
	raw, err := os.ReadFile(path)
	if err != nil {
		file.Error = fmt.Sprintf("%s: read: %v", path, err)
		return file
	}

	version, kvCount, counterCount, scanErr := inspectSnapshot(raw)
	file.Format = fmt.Sprintf("v%d", version)
	file.KVRecords = kvCount
	file.CounterRecords = counterCount
	if scanErr != nil {
		file.Error = fmt.Sprintf("%s: %v", path, scanErr)
	}
	return file
}

func inspectSnapshot(raw []byte) (uint32, uint64, uint64, error) {
	if len(raw) < 20 {
		return 0, 0, 0, fmt.Errorf("truncated snapshot header at offset %d", len(raw))
	}
	if binary.LittleEndian.Uint32(raw[:4]) != snapMagic {
		return 0, 0, 0, errors.New("invalid snapshot magic at offset 0")
	}

	version := binary.LittleEndian.Uint32(raw[4:8])
	if version != uint32(snapV1) && version != uint32(snapV2) && version != uint32(snapV4) {
		return version, 0, 0, fmt.Errorf("unsupported snapshot version %d at offset 4", version)
	}

	count := binary.LittleEndian.Uint32(raw[16:20])
	start := 20
	var counterBytes uint32
	format := snapFormat(version)
	if format == snapV4 {
		if len(raw) < 24 {
			return version, 0, 0, errors.New("truncated v4 snapshot header at offset 20")
		}

		counterBytes = binary.LittleEndian.Uint32(raw[20:24])
		start = 24
	}

	c := codec.NewReader(binary.LittleEndian, raw)
	c.SetPos(start)
	for i := range count {
		if _, ok := decodeSnapEntry(&c, format); !ok {
			return version, uint64(i), 0, fmt.Errorf("truncated snapshot KV entry %d at offset %d", i, c.Pos())
		}
	}
	if format != snapV4 {
		if c.Remaining() != 0 {
			return version, uint64(count), 0, fmt.Errorf("trailing snapshot bytes at offset %d", c.Pos())
		}
		return version, uint64(count), 0, nil
	}

	end := c.Pos() + int(counterBytes)
	if end > len(raw) {
		return version, uint64(count), 0, fmt.Errorf("truncated counter section at offset %d", c.Pos())
	}

	bundle := raw[c.Pos():end]
	var counters []counter.BundleEntry
	emptyBundle := len(bundle) == 8 && binary.BigEndian.Uint32(bundle[:4]) == counter.MagicCAE1 && binary.BigEndian.Uint32(bundle[4:8]) == 0
	if !emptyBundle {
		decoded, err := counter.DecodeCounterBundle(bundle)
		if err != nil {
			return version, uint64(count), 0, fmt.Errorf("invalid counter section at offset %d: %w", c.Pos(), err)
		}
		counters = decoded
	}
	if end != len(raw) {
		return version, uint64(count), uint64(len(counters)), fmt.Errorf("trailing snapshot bytes at offset %d", end)
	}
	return version, uint64(count), uint64(len(counters)), nil
}

func isWALSegmentName(name string) bool {
	if !strings.HasPrefix(name, walSegmentPre) || !strings.HasSuffix(name, walSegmentSuf) {
		return false
	}

	mid := strings.TrimSuffix(strings.TrimPrefix(name, walSegmentPre), walSegmentSuf)
	_, err := strconv.ParseUint(mid, 10, 64)
	return err == nil
}
