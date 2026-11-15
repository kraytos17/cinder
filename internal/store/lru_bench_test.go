package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
)

// BenchmarkStorePut measures fresh inserts (256 B values, distinct keys).
func BenchmarkStorePut(b *testing.B) {
	s := NewLRUStore(1<<26, nil)
	val := make([]byte, 256)
	// Prebuilt keys: key formatting is harness overhead, not store cost.
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("put-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := s.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStorePutTTL measures inserts carrying a deadline.
func BenchmarkStorePutTTL(b *testing.B) {
	s := NewLRUStore(1<<26, nil)
	val := make([]byte, 256)
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("ttl-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := s.PutWithTTL(keys[i], val, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreOverwrite compares the common update path for entries with
// and without TTL. The value/key stay fixed so this isolates deadline and
// wheel bookkeeping rather than map growth or key formatting.
func BenchmarkStoreOverwrite(b *testing.B) {
	for _, ttl := range []struct {
		name string
		d    time.Duration
	}{
		{"none", 0},
		{"short-wheel", 5 * time.Second},
		{"long-heap", 10 * time.Minute},
	} {
		b.Run(ttl.name, func(b *testing.B) {
			s := NewLRUStore(1<<20, nil)
			value := make([]byte, 64)
			if ttl.d > 0 {
				if err := s.PutWithTTL("hot", value, ttl.d); err != nil {
					b.Fatal(err)
				}
			} else if err := s.Put("hot", value); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				entry := Entry{Value: value, Version: uint64(i + 2)}
				if ttl.d > 0 {
					entry.Expiry = expiry.At(s.clock.Now().Add(ttl.d))
				}
				if err := s.PutVersioned("hot", entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStoreExpiryWheel isolates wheel insert, replacement, and removal
// costs across short-slot and long-heap deadlines. The store is reused on one
// key; changing the version prevents the losing-write fast path.
func BenchmarkStoreExpiryWheel(b *testing.B) {
	for _, ttl := range []struct {
		name string
		d    time.Duration
	}{
		{"short-slot", 5 * time.Second},
		{"long-heap", 10 * time.Minute},
	} {
		b.Run(ttl.name+"/replace", func(b *testing.B) {
			s := NewLRUStore(1<<20, nil)
			value := []byte("x")
			b.ReportAllocs()
			for i := range b.N {
				entry := Entry{Value: value, Version: uint64(i + 1), Expiry: expiry.At(s.clock.Now().Add(ttl.d))}
				if err := s.PutVersioned("hot", entry); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(ttl.name+"/insert-remove", func(b *testing.B) {
			s := NewLRUStore(1<<20, nil)
			value := []byte("x")
			b.ReportAllocs()
			for i := range b.N {
				entry := Entry{Value: value, Version: uint64(i + 1), Expiry: expiry.At(s.clock.Now().Add(ttl.d))}
				if err := s.PutVersioned("hot", entry); err != nil {
					b.Fatal(err)
				}
				if !s.Remove("hot") {
					b.Fatal("entry unexpectedly absent")
				}
			}
		})
	}
}

// BenchmarkWheelReplaceSlot isolates replacement as short-term slot
// occupancy grows. Each occupied slot has the same population; the target is
// placed in the last occupied slot to expose the slot-lookup cost.
func BenchmarkWheelReplaceSlot(b *testing.B) {
	for _, occupiedSlots := range []int{1, 16, 64, wheelSlots} {
		b.Run(fmt.Sprintf("occupied-slots=%d", occupiedSlots), func(b *testing.B) {
			w := NewWheel()
			const entriesPerSlot = 8
			for slot := range occupiedSlots {
				ttlTicks := uint64(slot)
				if ttlTicks == 0 {
					ttlTicks = wheelSlots
				}
				for entry := range entriesPerSlot {
					id := 10_000 + slot*entriesPerSlot + entry
					w.Insert(id, ttlTicks)
				}
			}
			targetID := 1
			targetTicks := uint64(occupiedSlots - 1)
			if targetTicks == 0 {
				targetTicks = wheelSlots
			}
			w.Insert(targetID, targetTicks)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				w.Insert(targetID, targetTicks)
			}
		})
	}
}

// BenchmarkWheelReplaceHeap measures replacement for long deadlines, where
// Wheel.Remove locates the old schedule through its id->heap-index map.
func BenchmarkWheelReplaceHeap(b *testing.B) {
	w := NewWheel()
	for i := range 1024 {
		w.Insert(10_000+i, uint64(wheelSlots+1+i))
	}
	w.Insert(1, wheelSlots+1025)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		w.Insert(1, uint64(wheelSlots+1025+i%1024))
	}
}

// BenchmarkWheelRemoveAbsent covers the common idempotent no-op case.
func BenchmarkWheelRemoveAbsent(b *testing.B) {
	w := NewWheel()
	b.ReportAllocs()
	for b.Loop() {
		w.Remove(1)
	}
}

// BenchmarkStoreGetHit cycles reads over 100k resident keys.
func BenchmarkStoreGetHit(b *testing.B) {
	s := NewLRUStore(1<<26, nil)
	val := make([]byte, 256)
	const resident = 100_000
	keys := make([]string, resident)
	for i := range keys {
		keys[i] = fmt.Sprintf("hit-key-%d", i)
	}
	for _, k := range keys {
		if err := s.Put(k, val); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if v, ok := s.Get(keys[i%resident]); !ok || len(v) != 256 {
			b.Fatal("miss")
		}
	}
}

// BenchmarkStoreGetMiss measures the miss path (index lookup, no copy).
func BenchmarkStoreGetMiss(b *testing.B) {
	s := NewLRUStore(1<<26, nil)
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("absent-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, ok := s.Get(keys[i]); ok {
			b.Fatal("hit")
		}
	}
}

// BenchmarkStoreEvictPressure forces eviction on every insert past a tiny
// budget: the LRU unlink + wheel accounting path.
func BenchmarkStoreEvictPressure(b *testing.B) {
	s := NewLRUStore(1<<16, nil) // 64 KiB: every insert evicts
	val := make([]byte, 256)
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("evict-key-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if err := s.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
}
