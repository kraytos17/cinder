package persist

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

func TestCompactWALSyncFailurePreservesActiveLog(t *testing.T) {
	testCompactWALRotationFailure(t, "sync", func(m *Manager, wantErr error) {
		m.syncWAL = func() error { return wantErr }
	}, func(m *Manager) { m.syncWAL = nil })
}

func TestCompactWALCloseFailurePreservesActiveLog(t *testing.T) {
	testCompactWALRotationFailure(t, "close", func(m *Manager, wantErr error) {
		m.closeWAL = func() error { return wantErr }
	}, func(m *Manager) { m.closeWAL = nil })
}

func testCompactWALRotationFailure(t *testing.T, operation string, inject func(*Manager, error), reset func(*Manager)) {
	t.Helper()
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	defer m.Close()
	wire(t, st, m)
	if err := st.Put("before", []byte("durable-source")); err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("injected WAL " + operation + " failure")
	inject(m, wantErr)
	if err := m.Compact(); !errors.Is(err, wantErr) {
		t.Fatalf("Compact error = %v, want injected %s failure", err, operation)
	}
	if m.wal == nil {
		t.Fatalf("Compact discarded the active WAL after %s failure", operation)
	}
	if err := st.Put("after", []byte("still-writable")); err != nil {
		t.Fatalf("write after failed compact: %v", err)
	}

	// The snapshot was published, but the old WAL remains attached; recovery
	// must see the snapshot state and post-failure WAL append.
	reset(m)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := testStore(t)
	r := NewManager(dir, recovered, nil)
	if err := r.Recover(); err != nil {
		t.Fatalf("recover after failed compact: %v", err)
	}
	defer r.Close()
	for key, want := range map[string]string{"before": "durable-source", "after": "still-writable"} {
		if got, ok := recovered.Get(key); !ok || string(got) != want {
			t.Fatalf("recovered %q = %q, present=%v; want %q", key, got, ok, want)
		}
	}
}

func TestManagerCloseReturnsWALCloseFailureAndDetaches(t *testing.T) {
	dir := t.TempDir()
	m := NewManager(dir, testStore(t), nil)
	if err := m.Recover(); err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	wantErr := errors.New("injected WAL close failure")
	m.closeWAL = func() error { return wantErr }
	if err := m.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("Close error = %v, want injected close failure", err)
	}
	m.closeWAL = nil
	if m.wal != nil {
		t.Fatal("Close retained the WAL writer after attempting to close it")
	}
	// A detached manager must not append through the closed writer.
	if err := m.OnWrite("after-close", store.Entry{Value: []byte("v")}, false); err != nil {
		t.Fatalf("OnWrite with detached WAL: %v", err)
	}
}

// TestMaybeCompactInterval fires once the snapshot interval elapses and
// resets the countdown afterwards.
func TestMaybeCompactInterval(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	clk := store.NewManualClock(time.Now())
	m := NewManager(dir, st, clk.Now)
	mustRecover(t, m)
	wire(t, st, m)

	if err := st.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if m.MaybeCompact() {
		t.Fatal("compacted before the interval elapsed")
	}
	clk.Advance(DefaultSnapshotInterval + time.Second)
	if !m.MaybeCompact() {
		t.Fatal("interval elapsed without compacting")
	}
	// Snapshot landed; a second call is quiet until the next interval.
	matched, _ := filepath.Glob(filepath.Join(dir, snapshotPre+"*"+snapshotSuf))
	if len(matched) == 0 {
		t.Fatal("no snapshot written")
	}
	if m.MaybeCompact() {
		t.Fatal("compacted twice without a new interval")
	}
}

// TestMaybeCompactSize fires once the active WAL passes the byte budget.
func TestMaybeCompactSize(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	m.SetSnapshotInterval(0) // time trigger off; size trigger decides
	m.SetMaxWALBytes(1024)
	mustRecover(t, m)
	wire(t, st, m)

	fired := false
	for i := 0; i < 200 && !fired; i++ {
		if err := st.Put(string(rune('a'+i%26))+string(rune('0'+i%10)), make([]byte, 256)); err != nil {
			t.Fatal(err)
		}
		fired = m.MaybeCompact()
	}
	if !fired {
		t.Fatal("64 KiB of appends never tripped a 1 KiB budget")
	}
	if m.MaybeCompact() {
		t.Fatal("fresh WAL must be under budget right after compact")
	}
}

// TestMaybeCompactDisabled stays quiet with both triggers off.
func TestMaybeCompactDisabled(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	m.SetSnapshotInterval(0)
	m.SetMaxWALBytes(0)
	mustRecover(t, m)
	wire(t, st, m)
	for range 50 {
		if err := st.Put("k", make([]byte, 1024)); err != nil {
			t.Fatal(err)
		}
		if m.MaybeCompact() {
			t.Fatal("compacted with all triggers disabled")
		}
	}
}

// TestMaybeCompactNoWAL stays quiet on a fresh manager: empty WAL is
// under budget and the (long) interval has not elapsed.
func TestMaybeCompactNoWAL(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	clk := store.NewManualClock(time.Now())
	m := NewManager(dir, st, clk.Now)
	m.SetSnapshotInterval(time.Hour)
	mustRecover(t, m)
	if m.MaybeCompact() {
		t.Fatal("compacted with an empty WAL and no elapsed interval")
	}
}

func TestReplayDelRespectsLWW(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	m := NewManager(dir, st, nil)
	mustRecover(t, m)
	wire(t, st, m)
	putVer(t, st, "k", "v", 5, 9)
	if !st.Remove("k") {
		t.Fatal("remove failed")
	}
	putVer(t, st, "k", "v2", 9, 9)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	st2 := testStore(t)
	m2 := NewManager(dir, st2, nil)
	mustRecover(t, m2)
	if got, ok := st2.Get("k"); !ok || string(got) != "v2" {
		t.Fatalf("newer write lost to replayed delete: %q %v", got, ok)
	}
}

// TestMaybeCompactEntries fires once the WAL record count passes the
// entry budget, independent of bytes and interval.
func TestMaybeCompactEntries(t *testing.T) {
	dir := t.TempDir()
	st := testStore(t)
	clk := store.NewManualClock(time.Now())
	m := NewManager(dir, st, clk.Now)
	mustRecover(t, m)
	wire(t, st, m)
	m.SetSnapshotInterval(0)
	m.SetMaxWALBytes(0)
	m.SetMaxWALEntries(3)

	if err := st.Put("a", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := st.Put("b", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if m.MaybeCompact() {
		t.Fatal("compacted before the entry budget filled")
	}
	if err := st.Put("c", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !m.MaybeCompact() {
		t.Fatal("entry budget filled without compacting")
	}
	// Rotation resets the count: quiet until 3 more records land.
	if m.MaybeCompact() {
		t.Fatal("compacted twice without new records")
	}
	// Zero disables the count trigger (bytes/interval already off).
	m.SetMaxWALEntries(0)
	if err := st.Put("d", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if m.MaybeCompact() {
		t.Fatal("compacted with all triggers disabled")
	}
}
