package store

import (
	"errors"
	"testing"
	"time"
)

// FuzzStoreSequence feeds op sequences to a small store under a manual
// clock, mirroring the C++ store_put fuzz: small capacity forces eviction
// pressure, a small key pool forces collisions, and every op carries
// invariants — no panics, Put results are nil or ErrCapacityExceeded
// only, and a resident key's version never decreases across a mutating op
// (sampled around the op so silent capacity eviction between observations
// cannot false-positive: absent before or after means no constraint).
func FuzzStoreSequence(f *testing.F) {
	f.Add([]byte{0x03, 'a', 'b', 'c', 0x00, 0x01, 0x05, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90})
	f.Add([]byte{})
	f.Add([]byte{0x01, 'k'})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}
		clk := NewManualClock(time.Unix(1_700_000_000, 0))
		st := NewLRUStore(4096, clk)
		poolSize := int(data[0]%8) + 1
		keys := make([]string, poolSize)
		for i := range keys {
			keys[i] = string([]byte{byte('a' + i), byte('0' + i%10)})
		}
		pos := 1
		next := func(n int) []byte {
			if pos >= len(data) {
				return nil
			}
			end := min(pos+n, len(data))
			out := data[pos:end]
			pos = end
			return out
		}
		u64 := func(b []byte) uint64 {
			var v uint64
			for _, c := range b {
				v = v*31 + uint64(c)
			}
			return v
		}
		ops := 1 + int(data[0])%64
		for range ops {
			ob := next(1)
			if len(ob) == 0 {
				return
			}
			key := keys[int(ob[0])%len(keys)]
			arg := next(8)
			switch ob[0] % 7 {
			case 0: // Put, occasionally oversized for the reject path.
				val := make([]byte, int(u64(arg)%300))
				for j := range val {
					val[j] = byte(j)
				}
				if len(arg) > 0 && arg[0]%8 == 0 {
					val = make([]byte, 16_384)
				}
				before, hadBefore := st.GetVersioned(key)
				err := st.Put(key, val)
				if err != nil && !errors.Is(err, ErrCapacityExceeded) {
					t.Fatalf("put err = %v", err)
				}
				if hadBefore {
					if after, ok := st.GetVersioned(key); ok {
						if CompareLWW(stampOf(after), stampOf(before)) == LWWLess {
							t.Fatalf("version decreased on %q: %+v -> %+v", key, before, after)
						}
					}
				}
			case 1: // PutVersioned with fuzzer-built identity.
				e := Entry{Value: arg, Version: u64(arg) &^ (1 << 63), WriterNodeHash: u64(arg)}
				if len(arg) > 0 && arg[0]%2 == 0 {
					e.HasTTL = true
					e.ExpiresAt = clk.Now().Add(time.Duration(u64(arg)%500_000) * time.Millisecond)
				}
				before, hadBefore := st.GetVersioned(key)
				err := st.PutVersioned(key, e)
				if err != nil && !errors.Is(err, ErrCapacityExceeded) {
					t.Fatalf("putVersioned err = %v", err)
				}
				if hadBefore {
					if after, ok := st.GetVersioned(key); ok {
						if CompareLWW(stampOf(after), stampOf(before)) == LWWLess {
							t.Fatalf("version decreased on %q", key)
						}
					}
				}
			case 2: // RemoveVersioned plants a tombstone, unless it loses
				// LWW to a newer resident (correct no-op, mirroring
				// putVersioned's stale-reject).
				rv := u64(arg) &^ (1 << 63)
				err := st.RemoveVersioned(key, rv, u64(arg))
				if err != nil && !errors.Is(err, ErrCapacityExceeded) {
					t.Fatalf("removeVersioned err = %v", err)
				}
				if err != nil {
					break
				}
				if e, ok := st.GetVersioned(key); ok && !e.Tombstone {
					if e.Version <= rv {
						t.Fatalf("remove left live entry on %q: %+v (remove v%d)", key, e, rv)
					}
				}
			case 3: // Remove hard-erases.
				st.Remove(key)
				if _, ok := st.GetVersioned(key); ok {
					t.Fatalf("remove left entry on %q", key)
				}
			case 4: // Reads never panic; expired reads reap.
				_, _ = st.Get(key)
				_, _ = st.GetVersioned(key)
			case 5: // Advance the clock to fire TTLs and GC.
				clk.Advance(time.Duration(u64(arg)%10_000) * time.Millisecond)
				st.EvictExpired()
			default: // Capacity change never evicts.
				before := st.Size()
				st.SetCapacity(1 + u64(arg)%8192)
				if after := st.Size(); after > before {
					t.Fatalf("SetCapacity grew the store: %d -> %d", before, after)
				}
			}
		}
	})
}
