package persist

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kraytos17/cinder/internal/store"
)

// BenchmarkSnapshotWriteRead10k measures a 10k×256B snapshot publish +
// single-pass ReadAll. Before the streaming rewrite this held the whole
// image in RAM twice (body + WriteFile copy) and slurped the file twice.
func BenchmarkSnapshotWriteRead10k(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.dat")
	entries := make([]SnapEntry, 10000)
	for i := range entries {
		entries[i] = SnapEntry{
			Key:     fmt.Appendf(nil, "snap-key-%d", i),
			Value:   make([]byte, 256),
			Version: uint64(i + 1),
			Writer:  42,
		}
	}
	w := NewSnapWriter(path)
	b.ReportAllocs()

	for b.Loop() {
		if err := w.Write(uint64(len(entries)+1), entries); err != nil {
			b.Fatal(err)
		}
		r := NewSnapReader(path)
		data, err := r.ReadAll()
		if err != nil {
			b.Fatal(err)
		}
		if len(data.Entries) != len(entries) {
			b.Fatalf("entries = %d, want %d", len(data.Entries), len(entries))
		}
	}
}

// BenchmarkRecover10k measures WAL AppendBatch + full Recover replay for
// 10k×256B records, covering the toStore ownership move (3→2 copies).
func BenchmarkRecover10k(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		dir := b.TempDir()
		w, err := OpenWriter(filepath.Join(dir, walFileName), true)
		if err != nil {
			b.Fatal(err)
		}
		batch := make([]Entry, 1024)
		for base := 0; base < 10000; base += len(batch) {
			for j := range batch {
				n := base + j
				batch[j] = Entry{Op: OpSet, Key: fmt.Appendf(nil, "rec-key-%d", n), Value: make([]byte, 256), Version: uint64(n + 1), Writer: 7}
			}
			if _, err := w.AppendBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
		w.Close()
		m := NewManager(dir, store.NewLRUStore(64<<20, nil), nil)
		if err := m.Recover(); err != nil {
			b.Fatal(err)
		}
		m.Close()
	}
}

// BenchmarkOnWriteNoPersistence measures the disabled-persistence fast
// path: no WAL means no value copy and no lock-held memcpy.
func BenchmarkOnWriteNoPersistence(b *testing.B) {
	m := NewManager(b.TempDir(), store.NewLRUStore(1<<20, nil), nil)
	val := make([]byte, 256)
	e := store.Entry{Value: val, Version: 1}
	b.ReportAllocs()
	for b.Loop() {
		if err := m.OnWrite("k", e, false); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSyncPeriodicIdle measures the idle-tick skip: with no appends
// since the last sync there must be no underlying fsync syscall.
func BenchmarkSyncPeriodicIdle(b *testing.B) {
	dir := b.TempDir()
	m := NewManager(dir, store.NewLRUStore(1<<20, nil), nil)
	if err := m.Recover(); err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	calls := 0
	m.syncWAL = func() error { calls++; return nil }
	b.ReportAllocs()
	for b.Loop() {
		if err := m.SyncPeriodic(); err != nil {
			b.Fatal(err)
		}
	}
	if calls != 0 {
		b.Fatalf("idle SyncPeriodic issued %d syncs, want 0", calls)
	}
}
