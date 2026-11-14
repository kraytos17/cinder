// Package repl implements asynchronous and quorum replication, hinted
// handoff, and quorum reads with LWW reduction and background repair.
// Writes and reads fan out through a transport.Sender; concurrent reads
// for the same key and quorum share one in-flight operation.
package repl

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
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

// maxReplayWorkers bounds ReplayHints fan-out. A full batch strides across
// this many workers instead of firing one goroutine per hint.
const maxReplayWorkers = 16

// HintQueue is a bounded FIFO of undelivered writes: 1024 slots and a
// 64 MiB estimated-byte budget, drop-oldest past either bound. A newer
// LWW stamp for the same (target, key) replaces the queued hint; an older
// one is dropped. Expired hints are reaped while taking a queue snapshot.
//
// Storage is a ring (head + count over buf): head-drop eviction is O(1)
// with no memmove and no backing-array slide, unlike slice reslicing
// (which advances the array start and forces periodic whole-array
// reallocs) or per-push memmove (184 KB copied per eviction at full
// depth). Middle deletes (coalescing) still shift, but those are rare;
// eviction is the per-push cost when full.
type HintQueue struct {
	mu      sync.Mutex
	buf     []Hint
	head    int
	count   int
	bytes   uint64
	maxByte uint64
	dropped uint64
}

// HintDel is one journaled replay deletion: the hint adsorbed by a
// successful replay, batched into a single journal write per replay round.
// Key aliases the hint's request slice (encoded synchronously by AppendDels,
// never retained), so collecting dels costs no conversion alloc.
type HintDel struct {
	Target  string
	Key     []byte
	Version uint64
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

// phys maps logical index i (0 <= i < count) to its ring slot.
// Caller holds q.mu and guarantees the queue is non-empty.
func (q *HintQueue) phys(i int) int {
	return (q.head + i) % len(q.buf)
}

// grow doubles ring capacity up to maxHints, preserving FIFO order at
// head 0. Caller holds q.mu and guarantees count == len(buf).
func (q *HintQueue) grow() {
	// Clamp growth to [16, maxHints]: start at one chunk, double while
	// small, never exceed the queue bound.
	n := min(max(2*len(q.buf), 16), maxHints)
	nb := make([]Hint, n)
	for i := range q.count {
		nb[i] = q.buf[q.phys(i)]
	}

	clear(q.buf)
	q.buf = nb
	q.head = 0
}

// evictHead drops the oldest hint in O(1) and zeroes its slot so the
// evicted slices are collectable immediately. Caller holds q.mu.
func (q *HintQueue) evictHead() {
	q.bytes -= q.buf[q.head].Bytes()
	clear(q.buf[q.head : q.head+1])
	q.head++
	if q.head >= len(q.buf) {
		q.head = 0
	}

	q.count--
	q.dropped++
}

// removeAt drops logical index i, shifting the tail left, and zeroes the
// vacated slot. Caller holds q.mu. Used for rare middle deletes
// (coalescing, single Remove); bulk removal uses RemoveIDs.
func (q *HintQueue) removeAt(i int) {
	for j := i; j+1 < q.count; j++ {
		q.buf[q.phys(j)] = q.buf[q.phys(j+1)]
	}

	tail := q.phys(q.count - 1)
	clear(q.buf[tail : tail+1])
	q.count--
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
	for i := range q.count {
		old := q.buf[q.phys(i)]
		if old.Target == h.Target && bytes.Equal(old.Req.Key, h.Req.Key) {
			if !h.Covers(old) {
				return false
			}

			q.bytes -= old.Bytes()
			q.removeAt(i)
			break
		}
	}
	for q.count >= maxHints || q.bytes+incoming > q.maxByte {
		q.evictHead()
	}
	if h.ID == 0 {
		h.ID = hintIDs.Add(1)
	}
	if q.count == len(q.buf) {
		q.grow()
	}

	q.buf[q.phys(q.count)] = h
	q.count++
	q.bytes += incoming
	return true
}

// Snapshot copies up to max live hints FIFO, reaping expired ones. Entries
// past the bound stay queued for a later call. The out slice is presized;
// when nothing expired the ring is untouched (no rebuild alloc).
func (q *HintQueue) Snapshot(now time.Time, limit int) []Hint {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := min(q.count, limit)
	out := make([]Hint, 0, n)
	expired := 0
	for i := range q.count {
		h := q.buf[q.phys(i)]
		if h.ExpiresAt.Expired(now) {
			q.bytes -= h.Bytes()
			expired++
			continue
		}
		if len(out) < limit {
			out = append(out, h)
		}
	}
	if expired == 0 {
		return out
	}

	// Rebuild only when expiry actually dropped entries: linearize the
	// survivors into fresh storage (one alloc per expiry tick, not per
	// snapshot) so the ring stays compact and later evictions stay O(1).
	kept := make([]Hint, 0, q.count-expired)
	for i := range q.count {
		h := q.buf[q.phys(i)]
		if h.ExpiresAt.Expired(now) {
			continue
		}
		kept = append(kept, h)
	}

	clear(q.buf)
	q.buf = kept
	q.head = 0
	q.count = len(kept)
	return out
}

// Remove drops the hint with the given handle after a successful replay.
// Unknown handles (superseded by coalescing) are ignored.
func (q *HintQueue) Remove(id uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.count {
		if q.buf[q.phys(i)].ID == id {
			q.bytes -= q.buf[q.phys(i)].Bytes()
			q.removeAt(i)
			return
		}
	}
}

// RemoveIDs drops every hint whose handle is in ids in a single pass and
// a single lock hold, replacing N serialized O(n) Remove scans during
// replay (16 workers hammering one mutex). Order of the survivors is
// preserved.
//
// Lookup is in-place sort + binary search: zero allocations beyond the
// kept slice (which this function allocates anyway). A map set would cost
// an allocation per call for at most 256 lookups, and integer binary
// search over a sorted 256-element slice beats hashing at this size while
// staying obviously correct regardless of queue order.
func (q *HintQueue) RemoveIDs(ids []uint64) {
	if len(ids) == 0 {
		return
	}

	slices.Sort(ids)
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.count == 0 {
		return
	}

	kept := make([]Hint, 0, q.count)
	for i := range q.count {
		h := q.buf[q.phys(i)]
		if _, found := slices.BinarySearch(ids, h.ID); found {
			q.bytes -= h.Bytes()
			continue
		}
		kept = append(kept, h)
	}

	clear(q.buf)
	q.buf = kept
	q.head = 0
	q.count = len(kept)
}

// RemoveMatch drops the first hint matching target, key, and version.
func (q *HintQueue) RemoveMatch(target, key string, version uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.count {
		h := q.buf[q.phys(i)]
		if h.Target == target && string(h.Req.Key) == key && h.Req.Version == version {
			q.bytes -= h.Bytes()
			q.removeAt(i)
			return
		}
	}
}

// Len returns the queued hint count.
func (q *HintQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
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
