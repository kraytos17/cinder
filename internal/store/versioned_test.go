package store

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
)

func makeVersioned(value string, version, writer uint64) Entry {
	return Entry{Value: []byte(value), Version: version, WriterNodeHash: writer}
}

func TestVersionedNewerWins(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v1", 1, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVersioned("k", makeVersioned("v2", 2, 10)); err != nil {
		t.Fatal(err)
	}
	e, ok := s.GetVersioned("k")
	if !ok || string(e.Value) != "v2" || e.Version != 2 {
		t.Fatalf("got %+v, %v", e, ok)
	}
}

func TestVersionedStaleIgnored(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v2", 2, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVersioned("k", makeVersioned("v1", 1, 10)); err != nil {
		t.Fatal(err)
	}
	e, ok := s.GetVersioned("k")
	if !ok || string(e.Value) != "v2" {
		t.Fatalf("got %+v, %v", e, ok)
	}
}

func TestVersionedTiebreakWriterHash(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("w1", 1, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVersioned("k", makeVersioned("w0", 1, 5)); err != nil {
		t.Fatal(err)
	}
	if e, _ := s.GetVersioned("k"); string(e.Value) != "w1" {
		t.Fatalf("lower writer must lose, got %q", e.Value)
	}
	if err := s.PutVersioned("k", makeVersioned("w2", 1, 20)); err != nil {
		t.Fatal(err)
	}
	if e, _ := s.GetVersioned("k"); string(e.Value) != "w2" {
		t.Fatalf("higher writer must win, got %q", e.Value)
	}
}

func TestVersionedPutAssignsMonotonicVersion(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("k1", []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k1", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k2", []byte("c")); err != nil {
		t.Fatal(err)
	}
	k1, _ := s.GetVersioned("k1")
	k2, _ := s.GetVersioned("k2")
	if k1.Version <= 1 {
		t.Fatalf("overwrite must carry higher version, got %d", k1.Version)
	}
	if k1.Version == k2.Version {
		t.Fatal("versions must differ")
	}
}

func TestVersionedTTLExpires(t *testing.T) {
	s := NewLRUStore(1024, nil)
	e := makeVersioned("v", 1, 10)
	e.Expiry = expiry.At(time.Now().Add(-time.Second))
	if err := s.PutVersioned("k", e); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetVersioned("k"); ok {
		t.Fatal("expected miss on expired entry")
	}
}

func TestVersionedGetMissing(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if _, ok := s.GetVersioned("missing"); ok {
		t.Fatal("expected miss")
	}
}

func TestVersionedMintMonotonic(t *testing.T) {
	s := NewLRUStore(1024, nil)
	a := s.MintVersion()
	b := s.MintVersion()
	if b <= a {
		t.Fatal("versions must increase")
	}
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	c := s.MintVersion()
	if c <= b {
		t.Fatal("mint must advance past put")
	}
	k, _ := s.GetVersioned("k")
	if k.Version <= a {
		t.Fatal("put version must exceed earlier mint")
	}
}

func TestVersionedLamportBump(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if v := s.MintVersion(); v < 6 {
		t.Fatalf("mint must advance past observed version, got %d", v)
	}
	s2 := NewLRUStore(1024, nil)
	if err := s2.PutVersioned("k", makeVersioned("high", 100, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s2.PutVersioned("k", makeVersioned("stale", 2, 10)); err != nil {
		t.Fatal(err)
	}
	if v := s2.MintVersion(); v < 101 {
		t.Fatalf("stale write must not rewind the clock, got %d", v)
	}
}

func TestVersionedRestartSeedWins(t *testing.T) {
	clock := NewManualClock(time.Now())
	a := NewLRUStore(1024, clock)
	old := a.MintVersion()
	clock.Advance(5 * time.Second)
	b := NewLRUStore(1024, clock)
	if b.MintVersion() <= old {
		t.Fatal("fresh store must seed past the old clock")
	}
	c := NewLRUStore(1024, clock)
	if err := c.PutVersioned("k", makeVersioned("old", old, 10)); err != nil {
		t.Fatal(err)
	}
	if err := c.PutVersioned("k", makeVersioned("new", b.MintVersion(), 10)); err != nil {
		t.Fatal(err)
	}
	if e, _ := c.GetVersioned("k"); string(e.Value) != "new" {
		t.Fatalf("got %q", e.Value)
	}
}

func TestVersionedForEach(t *testing.T) {
	s := NewLRUStore(1<<20, nil)
	for i := range 10 {
		if err := s.Put(fmt.Sprintf("key%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	visited := map[string]string{}
	s.ForEach(func(key string, e Entry) { visited[key] = string(e.Value) })
	if len(visited) != 10 {
		t.Fatalf("visited %d keys", len(visited))
	}
	for i := range 10 {
		if visited[fmt.Sprintf("key%d", i)] != fmt.Sprintf("v%d", i) {
			t.Fatalf("missing key%d", i)
		}
	}
}

func TestVersionedForEachSkipsExpired(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1<<20, clock)
	e := makeVersioned("gone", 1, 0)
	e.Expiry = expiry.At(clock.Now().Add(-time.Second))
	if err := s.PutVersioned("gone", e); err != nil {
		t.Fatal(err)
	}
	count := 0
	s.ForEach(func(string, Entry) { count++ })
	if count != 0 {
		t.Fatalf("visited %d expired entries", count)
	}
}

func TestVersionedEvictExpiredPurges(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.PutWithTTL("b", []byte("2"), time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(3500 * time.Millisecond)
	if n := s.EvictExpired(); n != 2 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedEvictExpiredReschedules(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if n := s.EvictExpired(); n != 0 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
	clock.Advance(4 * time.Second)
	if n := s.EvictExpired(); n != 1 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedEvictExpiredWrapLongTTL(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), 300*time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(300 * time.Second)
	if n := s.EvictExpired(); n != 1 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedOverwriteMovesWheelSlot(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(1500 * time.Millisecond)
	if err := s.PutWithTTL("a", []byte("2"), time.Second); err != nil {
		t.Fatal(err)
	}
	clock.Advance(600 * time.Millisecond)
	if n := s.EvictExpired(); n != 0 {
		t.Fatalf("evicted = %d (re-scheduled, not expired)", n)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
	clock.Advance(500 * time.Millisecond)
	if n := s.EvictExpired(); n != 1 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedRemoveClearsWheelSlot(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), time.Second); err != nil {
		t.Fatal(err)
	}
	if !s.Remove("a") {
		t.Fatal("expected remove=true")
	}
	clock.Advance(2 * time.Second)
	if n := s.EvictExpired(); n != 0 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedNonTTLOverwriteClearsWheel(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("a", []byte("2")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	if n := s.EvictExpired(); n != 0 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
	got, ok := s.Get("a")
	if !ok || string(got) != "2" {
		t.Fatalf("got %q, %v", got, ok)
	}
}

func TestVersionedRemovePlantsTombstone(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutVersioned("k", makeVersioned("v", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("k", 7, 11); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("live reads must hide the delete")
	}
	tomb, ok := s.GetVersioned("k")
	if !ok || !tomb.Tombstone || tomb.Version != 7 || tomb.WriterNodeHash != 11 {
		t.Fatalf("got %+v, %v", tomb, ok)
	}
}

func TestVersionedStaleDeleteLoses(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("k", 3, 99); err != nil {
		t.Fatal(err)
	}
	e, ok := s.GetVersioned("k")
	if !ok || e.Tombstone || string(e.Value) != "v" {
		t.Fatalf("got %+v, %v", e, ok)
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatal("value must survive a stale delete")
	}
}

func TestVersionedPutAfterDeleteResurrects(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v1", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("k", 7, 11); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("delete must hide the value")
	}
	if err := s.PutVersioned("k", makeVersioned("v2", 9, 10)); err != nil {
		t.Fatal(err)
	}
	e, ok := s.GetVersioned("k")
	if !ok || e.Tombstone || string(e.Value) != "v2" {
		t.Fatalf("got %+v, %v", e, ok)
	}
}

func TestVersionedDeleteMissingPlantsTombstone(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.RemoveVersioned("ghost", 4, 12); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("ghost"); ok {
		t.Fatal("ghost must read missing")
	}
	tomb, ok := s.GetVersioned("ghost")
	if !ok || !tomb.Tombstone || tomb.Version != 4 {
		t.Fatalf("got %+v, %v", tomb, ok)
	}
}

func TestVersionedTombstoneWinsTies(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("live", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("k", 1, 0); err != nil {
		t.Fatal(err)
	}
	tomb, ok := s.GetVersioned("k")
	if !ok || !tomb.Tombstone {
		t.Fatalf("got %+v, %v", tomb, ok)
	}
	if err := s.PutVersioned("j", makeVersioned("live", 1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("j", 1, 0); err != nil {
		t.Fatal(err)
	}
	liveTie := makeVersioned("live-again", 1, 0)
	if err := s.PutVersioned("j", liveTie); err != nil {
		t.Fatal(err)
	}
	kept, ok := s.GetVersioned("j")
	if !ok || !kept.Tombstone {
		t.Fatalf("tied live write must not clear the marker: %+v, %v", kept, ok)
	}
}

func TestVersionedTombstoneGCExpires(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	s.SetTombstoneGCInterval(100 * time.Millisecond)
	if err := s.PutVersioned("k", makeVersioned("v", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("k", 7, 11); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
	clock.Advance(50 * time.Millisecond)
	if n := s.EvictExpired(); n != 0 {
		t.Fatalf("evicted = %d", n)
	}
	if _, ok := s.GetVersioned("k"); !ok {
		t.Fatal("marker must survive grace")
	}
	clock.Advance(60 * time.Millisecond)
	if n := s.EvictExpired(); n != 1 {
		t.Fatalf("evicted = %d", n)
	}
	if _, ok := s.GetVersioned("k"); ok {
		t.Fatal("marker must be reaped")
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedEvictionSparesTombstones(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(512, clock)
	s.SetTombstoneGCInterval(600 * time.Second)
	if err := s.PutVersioned("live-1", makeVersioned("a", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.PutVersioned("doomed", makeVersioned("b", 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveVersioned("doomed", 2, 1); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		e := Entry{Value: bytes.Repeat([]byte{byte('x' + i%26)}, 64), Version: uint64(i) + 10, WriterNodeHash: 1}
		_ = s.PutVersioned(fmt.Sprintf("pressure-%d", i), e)
	}
	tomb, ok := s.GetVersioned("doomed")
	if !ok || !tomb.Tombstone {
		t.Fatalf("tombstone evicted under pressure: %+v, %v", tomb, ok)
	}
}

func TestVersionedHardRemoveErases(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.PutVersioned("k", makeVersioned("v", 5, 10)); err != nil {
		t.Fatal(err)
	}
	if !s.Remove("k") {
		t.Fatal("expected remove=true")
	}
	if _, ok := s.GetVersioned("k"); ok {
		t.Fatal("hard remove must leave no marker")
	}
	if s.Size() != 0 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestVersionedGetVersionedSkipsLRUTouch(t *testing.T) {
	// Capacity fits two entries; a versioned read must not promote, so
	// the untouched key stays the eviction victim.
	s := NewLRUStore(uint64(2*(1+100+NodeOverhead)+50), nil)
	big := make([]byte, 100)
	if err := s.Put("a", big); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", big); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.GetVersioned("a"); !ok {
		t.Fatal("expected hit")
	}
	if err := s.Put("c", big); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("versioned read must not promote: a should be evicted")
	}
}
