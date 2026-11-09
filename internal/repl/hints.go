// Package repl implements quorum replication with hinted handoff (Go port).
//
// It mirrors ReplicationManager: async best-effort fan-out vs quorum
// W = R/2+1 acknowledgement (local write counts), LWW-max quorum reads with
// background repair, bounded hint queue with TTL, and singleflight read
// coalescing. The C++ async callbacks become goroutine fan-out; completion
// semantics are identical: async commits inline, quorum fails closed.
package repl

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// Consistency modes mirror ConsistencyMode.
type Mode int

const (
	// Async commits locally, then fans out best-effort with hints on failure.
	Async Mode = iota
	// Quorum requires W = R/2+1 acknowledgements including the local write.
	Quorum
)

// Sentinel errors. Servers map ErrQuorum to NotReady and ErrNotFound to
// NotFound, mirroring the C++ status contract.
var (
	ErrNotFound = errors.New("repl: key not found")
	ErrQuorum   = errors.New("repl: quorum not reached")
)

// Transport sends one request to a node. *client.Pool satisfies it.
type Transport interface {
	Send(node string, req protocol.Request) (protocol.Response, error)
}

// HintTTL bounds how long an undelivered write waits for its replica,
// mirroring K_HINT_TTL (30s).
const HintTTL = 30 * time.Second

// ReplayBatch caps hints replayed per call (FIFO), mirroring K_REPLAY_BATCH.
const ReplayBatch = 256

// HintQueue is a bounded FIFO of undelivered writes: 1024 slots and a
// 64 MiB estimated-byte budget, drop-oldest past either bound. A newer
// LWW stamp for the same (target, key) replaces the queued hint; an older
// one is dropped. Expired hints are reaped on snapshot. Mirrors HintQueue.
type HintQueue struct {
	mu      sync.Mutex
	items   []Hint
	bytes   uint64
	maxByte uint64
	dropped uint64
}

// Hint is one queued write.
type Hint struct {
	ID        uint64 // stable handle for replay removal
	Target    string
	Req       protocol.Request
	ExpiresAt time.Time
}

const (
	maxHints       = 1024
	defaultMaxByte = 64 << 20
	hintOverhead   = 128 // per-hint bookkeeping estimate (Go-side accounting)
)

var hintIDs atomic.Uint64

// NewHintQueue returns an empty queue with the default 64 MiB budget.
func NewHintQueue() *HintQueue {
	return &HintQueue{maxByte: defaultMaxByte}
}

// SetMaxBytes caps estimated bytes (drop-oldest past it). Restart-only in
// C++; here it applies immediately to subsequent pushes.
func (q *HintQueue) SetMaxBytes(n uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxByte = n
}

func hintBytes(h Hint) uint64 {
	return hintOverhead + uint64(len(h.Target)+len(h.Req.Key)+len(h.Req.Value))
}

// Push queues h, coalescing same (target, key) by LWW stamp and evicting
// oldest past either bound. Oversized hints are dropped outright.
func (q *HintQueue) Push(h Hint) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	incoming := hintBytes(h)
	if incoming > q.maxByte {
		q.dropped++
		return false
	}
	for i, old := range q.items {
		if old.Target == h.Target && string(old.Req.Key) == string(h.Req.Key) {
			if !lwwCoversReq(h.Req, old.Req) {
				return false
			}
			q.bytes -= hintBytes(old)
			q.items = append(q.items[:i], q.items[i+1:]...)
			break
		}
	}
	for len(q.items) >= maxHints || q.bytes+incoming > q.maxByte {
		q.bytes -= hintBytes(q.items[0])
		q.items = q.items[1:]
		q.dropped++
	}
	if h.ID == 0 {
		h.ID = hintIDs.Add(1)
	}
	q.items = append(q.items, h)
	q.bytes += incoming
	return true
}

// Snapshot copies up to max live hints FIFO, reaping expired ones. Entries
// past the bound stay queued for a later call.
func (q *HintQueue) Snapshot(now time.Time, max int) []Hint {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []Hint
	kept := q.items[:0]
	for _, h := range q.items {
		if !h.ExpiresAt.After(now) {
			q.bytes -= hintBytes(h)
			continue
		}
		if len(out) < max {
			out = append(out, h)
		}
		kept = append(kept, h)
	}
	q.items = kept
	return out
}

// Remove drops the hint with the given handle after a successful replay.
// Unknown handles (superseded by coalescing) are ignored.
func (q *HintQueue) Remove(id uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, h := range q.items {
		if h.ID == id {
			q.bytes -= hintBytes(h)
			q.items = append(q.items[:i], q.items[i+1:]...)
			return
		}
	}
}

// RemoveMatch drops the first hint for (target, key, version), mirroring
// the C++ DEL replay path.
func (q *HintQueue) RemoveMatch(target, key string, version uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, h := range q.items {
		if h.Target == target && string(h.Req.Key) == key && h.Req.Version == version {
			q.bytes -= hintBytes(h)
			q.items = append(q.items[:i], q.items[i+1:]...)
			return
		}
	}
}

// Len returns the queued hint count.
func (q *HintQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Dropped returns the lifetime drop-oldest/oversize count.
func (q *HintQueue) Dropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// Bytes returns the live estimated byte depth.
func (q *HintQueue) Bytes() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

func lwwCoversReq(a, b protocol.Request) bool {
	as := store.Stamp{Version: a.Version, Writer: a.WriterNodeHash, Tombstone: a.Tombstone}
	bs := store.Stamp{Version: b.Version, Writer: b.WriterNodeHash, Tombstone: b.Tombstone}
	return store.LWWCovers(as, bs)
}
