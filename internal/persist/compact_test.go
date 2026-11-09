package persist

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/store"
)

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
