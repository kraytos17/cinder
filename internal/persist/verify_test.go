package persist

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
)

func TestVerifyDataDirReportsValidCounts(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWriter(filepath.Join(dir, walFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	var state counter.PNCounter
	if err := state.Increment("node-a", 3); err != nil {
		t.Fatal(err)
	}
	rec, err := CounterEntry("count", state, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	report, err := VerifyDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 0 || report.KVRecords != 1 || report.CounterRecords != 1 || len(report.Files) != 1 {
		t.Fatalf("verify report = %+v", report)
	}
}

func TestVerifyDataDirReportsCorruptWALOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFileName)
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	report, err := VerifyDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], path) || !strings.Contains(report.Errors[0], "offset 8") {
		t.Fatalf("errors = %v, want file and record offset", report.Errors)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(raw) {
		t.Fatal("verification modified corrupt WAL")
	}
}

func TestVerifyDataDirReportsTornTailWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFileName)
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw[:len(raw)-2], 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	report, err := VerifyDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 0 || len(report.TornTails) != 1 || !report.Files[0].TornTail {
		t.Fatalf("verify report = %+v", report)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("verification truncated torn WAL tail")
	}
}

func TestVerifyDataDirReportsSnapshotCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, snapshotPre+"1"+snapshotSuf)
	if err := NewSnapWriter(path).Write(1, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], "invalid snapshot magic at offset 0") {
		t.Fatalf("errors=%v", report.Errors)
	}
}

func TestVerifyDataDirReportsUnsupportedWALHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFileName)
	data := make([]byte, 8)
	binary.LittleEndian.PutUint32(data[:4], walMagic)
	binary.LittleEndian.PutUint32(data[4:], 99)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Errors) != 1 || !strings.Contains(report.Errors[0], "unsupported WAL version 99 at offset 4") {
		t.Fatalf("errors=%v", report.Errors)
	}
}
