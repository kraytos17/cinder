package repl

import (
	"fmt"
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
