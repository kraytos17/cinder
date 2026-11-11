// Package repl implements asynchronous and quorum replication, hinted
// handoff, and quorum reads with LWW reduction and background repair.
// Writes and reads fan out through a transport.Sender; concurrent reads
// for the same key and quorum share one in-flight operation.
package repl

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/transport"
)

// Mode selects whether a write returns after local commit and best-effort
// fan-out, or waits for quorum acknowledgements.
type Mode int

const (
	// Async commits locally, then fans out best-effort with hints on failure.
	Async Mode = iota
	// Quorum requires W = R/2+1 acknowledgements including the local write.
	Quorum
)

// ParseMode parses a consistency mode from its canonical lowercase name.
func ParseMode(s string) (Mode, error) {
	switch s {
	case "async":
		return Async, nil
	case "quorum":
		return Quorum, nil
	default:
		return 0, fmt.Errorf("repl: unknown consistency mode %q", s)
	}
}

// Valid reports whether m is a defined consistency mode.
func (m Mode) Valid() bool { return m == Async || m == Quorum }

// String returns the stable lowercase mode name used in logs.
func (m Mode) String() string {
	if m == Quorum {
		return "quorum"
	}
	return "async"
}

// Sentinel errors. Servers map ErrQuorum to NotReady and ErrNotFound to
// NotFound, respectively.
var (
	ErrNotFound            = errors.New("repl: key not found")
	ErrQuorum              = errors.New("repl: quorum not reached")
	ErrInvalidReplicaCount = errors.New("repl: invalid replica count")
	ErrInvalidMode         = errors.New("repl: invalid consistency mode")
)

// Transport is the shared inter-node send contract (see
// internal/transport); *client.Pool satisfies it. Send must be bounded:
// the manager parks one goroutine per replica until Send returns
// (client.Pool enforces RPCTimeout, 5s in production). An unbounded
// transport parks fan-out goroutines for the transport's lifetime.
type Transport = transport.Sender

// HintTTL bounds how long an undelivered write remains eligible for replay.
const HintTTL = 30 * time.Second

// ReplayBatch caps the number of FIFO hints considered by one replay call.
const ReplayBatch = 256

// HintQueue is a bounded FIFO of undelivered writes: 1024 slots and a
// 64 MiB estimated-byte budget, drop-oldest past either bound. A newer
// LWW stamp for the same (target, key) replaces the queued hint; an older
// one is dropped. Expired hints are reaped while taking a queue snapshot.
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
	ExpiresAt expiry.Expiry
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

// SetMaxBytes caps estimated queued bytes, dropping the oldest entries as
// needed. The new limit applies immediately to subsequent pushes.
func (q *HintQueue) SetMaxBytes(n uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxByte = n
}

// Bytes estimates the queued size of h for budget accounting.
func (h Hint) Bytes() uint64 {
	return hintOverhead + uint64(len(h.Target)+len(h.Req.Key)+len(h.Req.Value))
}

// Stamp returns the LWW identity used for coalescing.
func (h Hint) Stamp() store.Stamp {
	return store.Stamp{Version: h.Req.Version, Writer: h.Req.WriterNodeHash, Tombstone: h.Req.Tombstone}
}

// Covers reports whether h supersedes old for the same target and key.
func (h Hint) Covers(old Hint) bool {
	return store.LWWCovers(h.Stamp(), old.Stamp())
}

// Push queues h, coalescing same (target, key) by LWW stamp and evicting
// oldest past either bound. Oversized hints are dropped outright.
func (q *HintQueue) Push(h Hint) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	incoming := h.Bytes()
	if incoming > q.maxByte {
		q.dropped++
		return false
	}
	for i, old := range q.items {
		if old.Target == h.Target && string(old.Req.Key) == string(h.Req.Key) {
			if !h.Covers(old) {
				return false
			}

			q.bytes -= old.Bytes()
			q.items = append(q.items[:i], q.items[i+1:]...)
			break
		}
	}
	for len(q.items) >= maxHints || q.bytes+incoming > q.maxByte {
		q.bytes -= q.items[0].Bytes()
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
func (q *HintQueue) Snapshot(now time.Time, limit int) []Hint {
	q.mu.Lock()
	defer q.mu.Unlock()

	var out []Hint
	kept := q.items[:0]
	for _, h := range q.items {
		if h.ExpiresAt.Expired(now) {
			q.bytes -= h.Bytes()
			continue
		}
		if len(out) < limit {
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
			q.bytes -= h.Bytes()
			q.items = append(q.items[:i], q.items[i+1:]...)
			return
		}
	}
}

// RemoveMatch drops the first hint matching target, key, and version.
func (q *HintQueue) RemoveMatch(target, key string, version uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, h := range q.items {
		if h.Target == target && string(h.Req.Key) == key && h.Req.Version == version {
			q.bytes -= h.Bytes()
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
