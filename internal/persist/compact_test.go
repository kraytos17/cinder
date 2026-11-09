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
