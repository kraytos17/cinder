package repl

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// BenchmarkHintPush measures queueing distinct keys (the outage path).
func BenchmarkHintPush(b *testing.B) {
	q := NewHintQueue()
	exp := expiry.At(time.Now().Add(time.Hour))
	hintKeys := make([][]byte, b.N)
	for i := range hintKeys {
		hintKeys[i] = fmt.Appendf(nil, "hint-key-%d", i)
	}
	b.ResetTimer()
	for i := range b.N {
		q.Push(Hint{
			Target:    "n2",
			Req:       protocol.Request{Opcode: protocol.OpReplicate, Key: hintKeys[i], Value: make([]byte, 64)},
			ExpiresAt: exp,
		})
	}
}

// BenchmarkHintPushContended isolates mutex contention: a small rotating
// key set keeps the queue short (coalescing scan trivial), so scaling
// across producer counts reflects the lock, not O(n) scans.
func BenchmarkHintPushContended(b *testing.B) {
	exp := expiry.At(time.Now().Add(time.Hour))
	for _, procs := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("procs%d", procs), func(b *testing.B) {
			q := NewHintQueue()
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for p := range procs {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for i := p; i < b.N; i += procs {
						q.Push(Hint{
							Target:    "n2",
							Req:       protocol.Request{Opcode: protocol.OpReplicate, Key: fmt.Appendf(nil, "k%d", i%8), Value: make([]byte, 64)},
							ExpiresAt: exp,
						})
					}
				}(p)
			}
			wg.Wait()
		})
	}
}

// BenchmarkHintPushSnapshotMixed models a replay tick racing producers:
// each round pushes 256 fresh hints from 4 producers, then snapshots.
func BenchmarkHintPushSnapshotMixed(b *testing.B) {
	exp := expiry.At(time.Now().Add(time.Hour))
	b.ReportAllocs()
	for b.Loop() {
		q := NewHintQueue()
		var wg sync.WaitGroup
		for p := range 4 {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				for i := range 64 {
					q.Push(Hint{
						Target:    "n2",
						Req:       protocol.Request{Opcode: protocol.OpReplicate, Key: fmt.Appendf(nil, "m%d", p*64+i), Value: make([]byte, 64)},
						ExpiresAt: exp,
					})
				}
			}(p)
		}
		wg.Wait()
		if got := q.Snapshot(time.Now(), ReplayBatch); len(got) != 256 {
			b.Fatalf("snapshot = %d, want 256", len(got))
		}
	}
}

// BenchmarkHintSnapshot measures the replay-scan over a full queue.
func BenchmarkHintSnapshot(b *testing.B) {
	q := NewHintQueue()
	exp := expiry.At(time.Now().Add(time.Hour))
	snapKeys := make([][]byte, 1024)
	for i := range snapKeys {
		snapKeys[i] = fmt.Appendf(nil, "hint-key-%d", i)
	}
	for i := range snapKeys {
		q.Push(Hint{
			Target:    "n2",
			Req:       protocol.Request{Opcode: protocol.OpReplicate, Key: snapKeys[i], Value: make([]byte, 64)},
			ExpiresAt: exp,
		})
	}

	for b.Loop() {
		if got := q.Snapshot(time.Now(), ReplayBatch); len(got) == 0 {
			b.Fatal("empty snapshot")
		}
	}
}
