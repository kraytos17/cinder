package store

import (
	"fmt"
	"testing"
	"time"
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
		v, ok := s.Get(keys[i%resident])
		if !ok || len(v) != 256 {
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
