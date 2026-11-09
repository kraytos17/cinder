package store

import (
	"sync"
	"testing"
	"time"
)

type sinkEvent struct {
	key     string
	packed  uint64
	writer  uint64
	removed bool
}

type sinkLog struct {
	mu     sync.Mutex
	events []sinkEvent
}

func (l *sinkLog) fn(key string, packed, writer uint64, removed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, sinkEvent{key, packed, writer, removed})
}

func (l *sinkLog) all() []sinkEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]sinkEvent(nil), l.events...)
}

func TestMutationSinkPutRemove(t *testing.T) {
	s := NewLRUStore(1024, nil)
	log := &sinkLog{}
	s.SetMutationSink(log.fn)

	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if !s.Remove("k") {
		t.Fatal("expected remove=true")
	}
	events := log.all()
	if len(events) != 2 {
		t.Fatalf("events = %+v", events)
	}
	if events[0].key != "k" || events[0].removed {
		t.Fatalf("put must emit added: %+v", events[0])
	}
	if events[1].key != "k" || !events[1].removed {
		t.Fatalf("remove must emit removed: %+v", events[1])
	}
	if events[0].packed != events[1].packed {
		t.Fatal("remove must carry the same identity the put added")
	}
}

func TestMutationSinkOverwriteEmitsRemoveThenAdd(t *testing.T) {
	s := NewLRUStore(4096, nil)
	log := &sinkLog{}
	s.SetMutationSink(log.fn)

	if err := s.Put("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	events := log.all()
	if len(events) != 3 {
		t.Fatalf("events = %+v", events)
	}
	if events[1].removed != true || events[2].removed != false {
		t.Fatalf("overwrite must emit removed then added: %+v", events)
	}
	if events[0].packed != events[1].packed {
		t.Fatal("overwrite remove must carry the old identity")
	}
}

func TestMutationSinkTombstoneIdentity(t *testing.T) {
	s := NewLRUStore(1024, nil)
	log := &sinkLog{}
	s.SetMutationSink(log.fn)

	if err := s.RemoveVersioned("k", 7, 11); err != nil {
		t.Fatal(err)
	}
	events := log.all()
	if len(events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	// version 7, has_ttl always set on tombstones, tombstone bit set.
	if want := uint64(7<<2 | 1 | 2); events[0].packed != want {
		t.Fatalf("packed = %d, want %d", events[0].packed, want)
	}
	if events[0].writer != 11 || events[0].removed {
		t.Fatalf("event = %+v", events[0])
	}
}

func TestMutationSinkCapacityEvictAndExpiry(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	log := &sinkLog{}
	s.SetMutationSink(log.fn)

	if err := s.PutWithTTL("a", []byte("1"), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("big", make([]byte, 900)); err != nil {
		t.Fatal(err)
	}
	// a (1+1+120) + big (3+900+120=1023) = 1145 > 1024: capacity evicts a.
	found := false
	for _, e := range log.all() {
		if e.key == "a" && e.removed {
			found = true
		}
	}
	if !found {
		t.Fatalf("capacity eviction must emit removed: %+v", log.all())
	}

	if err := s.PutWithTTL("b", []byte("2"), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Millisecond)
	if n := s.EvictExpired(); n < 1 {
		t.Fatalf("evicted = %d", n)
	}
	found = false
	for _, e := range log.all() {
		if e.key == "b" && e.removed {
			found = true
		}
	}
	if !found {
		t.Fatalf("expiry must emit removed: %+v", log.all())
	}
}

func TestMutationSinkUnset(t *testing.T) {
	s := NewLRUStore(1024, nil)
	log := &sinkLog{}
	s.SetMutationSink(log.fn)
	s.SetMutationSink(nil) // unregister
	if err := s.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if len(log.all()) != 0 {
		t.Fatal("unregistered sink must stay silent")
	}
}
