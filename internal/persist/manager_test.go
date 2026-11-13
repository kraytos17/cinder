package persist

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	return store.NewLRUStore(1<<20, nil)
}

func wire(t *testing.T, st *store.Store, m *Manager) {
	t.Helper()
	st.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
		return m.OnWrite(key, e, hardDelete)
	})
}

func TestRecoverRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	wire(t, st, m)

	if err := st.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	putVer(t, st, "ver", "vv", 5, 9)
	if err := st.PutWithTTL("ttl", []byte("t"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := st.RemoveVersioned("gone", 7, 11); err != nil {
		t.Fatal(err)
	}
	if err := st.Put("del", []byte("d")); err != nil {
		t.Fatal(err)
	}
	if !st.Remove("del") {
		t.Fatal("remove")
	}
	if err := m.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	st2 := testStore(t)
	m2 := NewManager(dir, st2, nil)
	mustRecover(t, m2)
	if got, ok := st2.Get("k"); !ok || string(got) != "v" {
		t.Fatalf("k = %q %v", got, ok)
	}
	if e, ok := st2.GetVersioned("ver"); !ok || string(e.Value) != "vv" || e.Version != 5 || e.WriterNodeHash != 9 {
		t.Fatalf("ver = %+v %v", e, ok)
	}
	if _, ok := st2.Get("ttl"); !ok {
		t.Fatal("ttl missing")
	}
	if _, ok := st2.Get("gone"); ok {
		t.Fatal("tombstone reads live")
	}
	if tomb, ok := st2.GetVersioned("gone"); !ok || !tomb.Tombstone || tomb.Version != 7 {
		t.Fatalf("tomb = %+v %v", tomb, ok)
	}
	if _, ok := st2.GetVersioned("del"); ok {
		t.Fatal("hard remove must leave no marker")
	}
	// Version clock continuity: mints advance past recovered versions.
	if v := st2.MintVersion(); v <= 7 {
		t.Fatalf("minted %d, want > 7", v)
	}
}

func TestRecoverSkipsExpired(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	wire(t, st, m)
	if err := st.PutWithTTL("short", []byte("x"), 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	st2 := testStore(t)
	m2 := NewManager(dir, st2, nil)
	mustRecover(t, m2)
	if st2.Size() != 0 {
		t.Fatalf("expired entry resurrected, size = %d", st2.Size())
	}
}

func TestRecoverTruncatesEveryTornTailOffset(t *testing.T) {
	first, err := (Entry{Op: OpSet, Key: []byte("kept"), Value: []byte("v1"), Version: 1}).encode()
	if err != nil {
		t.Fatal(err)
	}
	last, err := (Entry{Op: OpSet, Key: []byte("torn"), Value: []byte("v2"), Version: 2}).encode()
	if err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	basePath := filepath.Join(baseDir, walFileName)
	w, err := OpenWriter(basePath, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("kept"), Value: []byte("v1"), Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("torn"), Value: []byte("v2"), Version: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(basePath)
	if err != nil {
		t.Fatal(err)
	}
	validPrefix := uint64(8 + len(first))

	// Every prefix ending within the final record (including exactly at its
	// start) must recover the complete prefix and truncate to its watermark.
	for cut := len(image) - len(last); cut < len(image); cut++ {
		t.Run(fmt.Sprintf("cut-%d", cut), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, walFileName)
			if err := os.WriteFile(path, image[:cut], 0o644); err != nil {
				t.Fatal(err)
			}

			st := testStore(t)
			m := NewManager(dir, st, nil)
			if err := m.Recover(); err != nil {
				t.Fatalf("recover torn tail: %v", err)
			}
			if got, ok := st.Get("kept"); !ok || string(got) != "v1" {
				t.Fatalf("complete prefix lost: value=%q present=%v", got, ok)
			}
			if _, ok := st.Get("torn"); ok {
				t.Fatal("partial final record was replayed")
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := uint64(info.Size()); got != validPrefix {
				t.Fatalf("recovered WAL size=%d, want valid prefix %d", got, validPrefix)
			}
			if err := m.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRecoverRejectsCorruptWALRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFileName)
	w, err := OpenWriter(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Op: OpSet, Key: []byte("key"), Value: []byte("value"), Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	image[len(image)-1] ^= 0xff
	if err := os.WriteFile(path, image, 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewManager(dir, testStore(t), nil)
	if err := m.Recover(); err == nil {
		t.Fatal("recovery accepted a checksum-corrupt WAL record")
	}
}

func TestCompactRotates(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	wire(t, st, m)
	for i := range 10 {
		key := string(rune('a' + i))
		if err := st.Put(key, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}
	// Snapshot exists and CURRENT points at it; WAL is fresh (empty).
	snaps := snapshotsIn(t, dir)
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %v", snaps)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil || string(raw) != snaps[0] {
		t.Fatalf("CURRENT = %q, err %v", raw, err)
	}
	r, _ := OpenReader(filepath.Join(dir, "wal.log"))
	n := 0
	for r.HasMore() {
		if _, ok := r.Next(); !ok {
			break
		}
		n++
	}
	if n != 0 {
		t.Fatalf("fresh WAL has %d records", n)
	}
	// Compact twice more: pruning keeps the newest two.
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}
	// Force distinct names (nanos may collide on coarse clocks).
	time.Sleep(time.Millisecond)
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}
	if snaps := snapshotsIn(t, dir); len(snaps) != 2 {
		t.Fatalf("snapshots = %v", snaps)
	}
	// Everything still serves from a fresh recovery.
	st2 := testStore(t)
	m2 := NewManager(dir, st2, nil)
	mustRecover(t, m2)
	if st2.Size() != 10 {
		t.Fatalf("size = %d", st2.Size())
	}
}

func mustRecover(t *testing.T, m *Manager) {
	t.Helper()
	if err := m.Recover(); err != nil {
		t.Fatal(err)
	}
}

func putVer(t *testing.T, st *store.Store, key, value string, ver, writer uint64) {
	t.Helper()
	e := store.Entry{Value: []byte(value), Version: ver, WriterNodeHash: writer}
	if err := st.PutVersioned(key, e); err != nil {
		t.Fatal(err)
	}
}

func snapshotsIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if len(e.Name()) > len("snapshot_.dat") && e.Name()[:9] == "snapshot_" {
			out = append(out, e.Name())
		}
	}
	return out
}

func TestCompactRestoresLFUFrequency(t *testing.T) {
	dir := t.TempDir()
	st := store.NewLFUStore(1<<20, nil)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	wire(t, st, m)
	if err := st.Put("hot", []byte("v")); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, ok := st.Get("hot"); !ok {
			t.Fatal("hot key missing")
		}
	}
	if got := st.EntryFreq("hot"); got != 5 {
		t.Fatalf("pre-compact freq=%d, want 5", got)
	}
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}

	st2 := store.NewLFUStore(1<<20, nil)
	m2 := NewManager(dir, st2, nil)
	mustRecover(t, m2)
	if got := st2.EntryFreq("hot"); got != 5 {
		t.Fatalf("restored freq=%d, want 5", got)
	}
}
