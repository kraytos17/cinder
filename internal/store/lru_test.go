package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLRUPutAndGet(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok || string(got) != "value" {
		t.Fatalf("got %q, %v", got, ok)
	}
}

func TestLRUGetMissing(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if _, ok := s.Get("missing"); ok {
		t.Fatal("expected miss")
	}
}

func TestLRUOverwrite(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("key", []byte("value1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("key", []byte("value2")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("key")
	if !ok || string(got) != "value2" {
		t.Fatalf("got %q, %v", got, ok)
	}
}

func TestLRURemove(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("key", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !s.Remove("key") {
		t.Fatal("expected remove=true")
	}
	if _, ok := s.Get("key"); ok {
		t.Fatal("expected miss after remove")
	}
	if s.Remove("missing") {
		t.Fatal("expected remove=false")
	}
}

func TestLRUSize(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if s.Size() != 0 {
		t.Fatal("expected empty")
	}
	if err := s.Put("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 2 {
		t.Fatalf("size = %d", s.Size())
	}
	s.Remove("a")
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestLRUEvictionOnCapacity(t *testing.T) {
	s := NewLRUStore(100, nil)
	for i := range 100 {
		// Oversized entries are rejected; the bound below holds either way.
		_ = s.Put(fmt.Sprintf("%d", i), fmt.Appendf(nil, "%050d", 0))
	}
	if s.Size() > 3 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestLRUTTLExpiry(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("key", []byte("value"), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("key"); !ok {
		t.Fatal("expected hit before expiry")
	}
	clock.Advance(20 * time.Millisecond)
	if _, ok := s.Get("key"); ok {
		t.Fatal("expected miss after expiry")
	}
}

func TestLRUValueLargerThanCapacity(t *testing.T) {
	s := NewLRUStore(50, nil)
	if err := s.Put("key", make([]byte, 100)); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("expected ErrCapacityExceeded, got %v", err)
	}
}

func TestLRUEvictExpired(t *testing.T) {
	clock := NewManualClock(time.Now())
	s := NewLRUStore(1024, clock)
	if err := s.PutWithTTL("a", []byte("1"), 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Millisecond)
	if n := s.EvictExpired(); n != 1 {
		t.Fatalf("evicted = %d", n)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("a should be gone")
	}
	if _, ok := s.Get("b"); !ok {
		t.Fatal("b should survive")
	}
}

func TestLRUOrder(t *testing.T) {
	s := NewLRUStore(3500, nil)
	big := make([]byte, 1000)
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Put(k, big); err != nil {
			t.Fatal(err)
		}
	}
	s.Get("a") // a is now MRU; b is the victim
	if err := s.Put("d", big); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	if _, ok := s.Get("d"); !ok {
		t.Fatal("d should survive")
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
}

func TestLRUConcurrentGetPutRemove(t *testing.T) {
	s := NewLRUStore(4096, nil)
	var wg sync.WaitGroup
	for th := range 4 {
		wg.Add(1)
		go func(th int) {
			defer wg.Done()
			for i := range 5000 {
				key := fmt.Sprintf("k%d", (th*5000+i)%512)
				switch i % 4 {
				case 0:
					_ = s.Put(key, fmt.Appendf(nil, "v%d", i))
				case 1:
					s.GetVersioned(key)
				case 2:
					s.Remove(key)
				default:
					s.Get(key)
				}
				if n := s.Size(); n > 512+4 {
					t.Errorf("size = %d", n)
					return
				}
			}
		}(th)
	}
	wg.Wait()
}

func TestLRUEmptyKey(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("", []byte("value")); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("")
	if !ok || string(got) != "value" {
		t.Fatalf("got %q, %v", got, ok)
	}
}

func TestLRUMaxSizeValue(t *testing.T) {
	s := NewLRUStore(200, nil)
	if err := s.Put("k", make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	if s.Size() != 1 {
		t.Fatalf("size = %d", s.Size())
	}
}

func TestLRUMultipleOverwrites(t *testing.T) {
	s := NewLRUStore(1024, nil)
	for i := range 100 {
		if err := s.Put("key", fmt.Appendf(nil, "value%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	got, ok := s.Get("key")
	if !ok || string(got) != "value99" {
		t.Fatalf("got %q, %v", got, ok)
	}
}

func TestLRUGetReturnsCopy(t *testing.T) {
	s := NewLRUStore(1024, nil)
	if err := s.Put("k", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get("k")
	got[0] = 'X'
	again, _ := s.Get("k")
	if string(again) != "abc" {
		t.Fatal("get must return a copy")
	}
}
