package repl

import (
	"hash/fnv"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"

	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/store"
)

// Manager coordinates writes to the primary plus replicas. The store is
// the single version authority; every replica apply is idempotent LWW.
type Manager struct {
	local     *store.Store
	self      string
	writer    uint64
	clock     store.Clock
	transport Transport
	hints     *HintQueue
	journal   HintJournal
	replaying atomic.Bool
	flights   singleflight.Group
	met       atomic.Pointer[metrics.Collector]
	logger    atomic.Pointer[zerolog.Logger]
}

var nopLogger = log.Nop()

// HintJournal persists hints across restarts. AppendAdd records a queued
// hint; AppendDel records a successful replay so recovery drops it.
// NeedsCompaction/Compact bound the file: past the record budget the
// journal is rewritten from the live queue (mirrors the C++ 4,096 rule).
type HintJournal interface {
	AppendAdd(h Hint)
	AppendDel(target, key string, version uint64)
	NeedsCompaction() bool
	Compact(q *HintQueue) error
}

// SetHintJournal enables durable hints (nil disables).
func (m *Manager) SetHintJournal(j HintJournal) {
	m.journal = j
}

// SetMetrics wires the observability collector (nil disables).
func (m *Manager) SetMetrics(c *metrics.Collector) {
	m.met.Store(c)
}

// SetLogger installs the operational logger (nil selects Nop).
func (m *Manager) SetLogger(l *zerolog.Logger) {
	if l == nil {
		nop := log.Nop()
		l = &nop
	}
	m.logger.Store(l)
}

func (m *Manager) metrics() *metrics.Collector { return m.met.Load() }

// log returns the installed logger (Nop when unset, so direct
// constructions stay silent without a SetLogger call).
func (m *Manager) log() *zerolog.Logger {
	if l := m.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// NewManager returns a replication manager. Transport may be nil only when
// every call passes an empty replica list.
func NewManager(local *store.Store, self string, clock store.Clock, t Transport) *Manager {
	if clock == nil {
		clock = store.SystemClock{}
	}
	h := fnv.New64a()
	h.Write([]byte(self))
	return &Manager{local: local, self: self, writer: h.Sum64(), clock: clock, transport: t, hints: NewHintQueue()}
}

// HintCount returns the queued hint count.
func (m *Manager) HintCount() int { return m.hints.Len() }

// HintBytes returns the live estimated hint byte depth.
func (m *Manager) HintBytes() uint64 { return m.hints.Bytes() }

// HintsDropped returns the lifetime drop count.
func (m *Manager) HintsDropped() uint64 { return m.hints.Dropped() }

// SetMaxHintBytes caps hint queue estimated bytes.
func (m *Manager) SetMaxHintBytes(n uint64) { m.hints.SetMaxBytes(n) }

// Local exposes the underlying store for serving and tests.
func (m *Manager) Local() *store.Store { return m.local }

// HintQueue exposes the hint queue for journal recovery wiring.
func (m *Manager) HintQueue() *HintQueue { return m.hints }

// WriterHash is the stable per-node LWW tiebreaker (FNV-1a of the node id).
func (m *Manager) WriterHash() uint64 { return m.writer }

// Write stores value locally, then fans out Replicate to replicas.
// Async returns after the local commit; quorum requires W acknowledgements
// including the local write and fails closed with ErrQuorum.
func (m *Manager) Write(key string, value []byte, ttl time.Duration, hasTTL bool, replicas []string, mode Mode, traceID, spanID uint64) error {
	version := m.local.MintVersion()
	e := store.Entry{Value: append([]byte(nil), value...), Version: version, WriterNodeHash: m.writer}
	if hasTTL {
		e.HasTTL = true
		e.ExpiresAt = m.clock.Now().Add(ttl)
	}
	if err := m.local.PutVersioned(key, e); err != nil {
		return err
	}
	req := protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: append([]byte(nil), value...),
		Version: version, WriterNodeHash: m.writer,
		TraceID: traceID, SpanID: spanID,
	}
	if hasTTL {
		req.HasExpiresAt = true
		req.ExpiresAt = e.ExpiresAt
	}
	if mode == Async {
		m.fanOutAsync(key, req, replicas)
		if mt := m.metrics(); mt != nil {
			mt.AsyncWrites.Add(1)
		}
		return nil
	}
	err := m.fanOutQuorum(req, replicas)
	if mt := m.metrics(); mt != nil {
		if err != nil {
			mt.QuorumWritesErr.Add(1)
		} else {
			mt.QuorumWritesOK.Add(1)
		}
	}
	if err != nil {
		if e := m.log().Warn(); e != nil {
			log.Fields(e, req.TraceID, req.SpanID).Str("key", key).Msg("quorum write failed")
		}
	}
	return err
}

// Delete tombstones locally, then fans out. Mirrors deleteAsync: a quorum
// delete returns success to no one — a quorum holding the tombstone means
// later reads observe the delete.
func (m *Manager) Delete(key string, replicas []string, mode Mode, traceID, spanID uint64) error {
	version := m.local.MintVersion()
	if err := m.local.RemoveVersioned(key, version, m.writer); err != nil {
		return err
	}
	tomb, _ := m.local.GetVersioned(key)
	req := protocol.Request{
		Opcode: protocol.OpHint, Key: []byte(key), Tombstone: true,
		Version: version, WriterNodeHash: m.writer,
		TraceID: traceID, SpanID: spanID,
	}
	if tomb.HasTTL {
		req.HasExpiresAt = true
		req.ExpiresAt = tomb.ExpiresAt
	}
	if mode == Async {
		m.fanOutAsync(key, req, replicas)
		if mt := m.metrics(); mt != nil {
			mt.AsyncDeletes.Add(1)
		}
		return nil
	}
	err := m.fanOutQuorum(req, replicas)
	if mt := m.metrics(); mt != nil {
		if err != nil {
			mt.QuorumDeletesEr.Add(1)
		} else {
			mt.QuorumDeletesOK.Add(1)
		}
	}
	if err != nil {
		if e := m.log().Warn(); e != nil {
			log.Fields(e, req.TraceID, req.SpanID).Str("key", key).Msg("quorum delete failed")
		}
	}
	return err
}

type fanResult struct {
	node string
	res  protocol.Response
	err  error
}

// fanOutAsync sends req to every replica concurrently and returns
// immediately; failures enqueue hints in the background. Mirrors the C++
// async path, where on_done runs inline after the local commit.
func (m *Manager) fanOutAsync(key string, req protocol.Request, replicas []string) {
	if e := m.log().Debug(); e != nil {
		log.Fields(e, req.TraceID, req.SpanID).
			Str("key", key).
			Strs("replicas", replicas).
			Msg("replicating")
	}
	out := m.launch(req, replicas)
	go func() {
		// Counted drain: launch sends exactly len(replicas) results and
		// never closes the channel, so ranging would park forever.
		for range replicas {
			if r := <-out; r.err != nil {
				m.enqueueHint(r.node, req)
			}
		}
	}()
}

// fanOutQuorum reduces one fan-out to W = total/2+1 including the local
// write. Success fires at W without waiting out stragglers, but a
// background drain continues so late failures still enqueue hints.
// Impossibility (acks + pending < W) fails fast with ErrQuorum.
func (m *Manager) fanOutQuorum(req protocol.Request, replicas []string) error {
	total := len(replicas) + 1
	w := total/2 + 1
	if w <= 1 {
		// No replicas: the local ack alone satisfies W == 1.
		return nil
	}
	out := m.launch(req, replicas)
	acks := 1 // local write
	pending := len(replicas)
	for pending > 0 {
		r := <-out
		pending--
		if r.err != nil {
			m.enqueueHint(r.node, req)
		} else {
			acks++
		}
		if acks >= w {
			go drain(out, pending, func(r fanResult) {
				if r.err != nil {
					m.enqueueHint(r.node, req)
				}
			})
			return nil
		}
		if acks+pending < w {
			go drain(out, pending, func(r fanResult) {
				if r.err != nil {
					m.enqueueHint(r.node, req)
				}
			})
			return ErrQuorum
		}
	}
	return ErrQuorum
}

// launch sends req to every replica concurrently; the caller drains exactly
// len(replicas) results.
func (m *Manager) launch(req protocol.Request, replicas []string) <-chan fanResult {
	out := make(chan fanResult, len(replicas))
	for _, node := range replicas {
		go func(node string) {
			res, err := m.transport.Send(node, req)
			out <- fanResult{node: node, res: res, err: err}
		}(node)
	}
	return out
}

func drain(out <-chan fanResult, n int, fn func(fanResult)) {
	for range n {
		fn(<-out)
	}
}

func (m *Manager) enqueueHint(node string, req protocol.Request) {
	h := Hint{Target: node, Req: req, ExpiresAt: m.clock.Now().Add(HintTTL)}
	if mt := m.metrics(); mt != nil {
		mt.ReplicaUnreach.Add(1)
		mt.HintsEnqueued.Add(1)
	}
	m.hints.Push(h)
	// Funnel the queue's own drop count (oversize + evictions, including
	// pushes that bypass the manager) into the collector without
	// double-counting: monotonic max-sync.
	if mt := m.metrics(); mt != nil {
		for {
			cur := mt.HintsDropped.Load()
			qd := m.hints.Dropped()
			if qd <= cur || mt.HintsDropped.CompareAndSwap(cur, qd) {
				break
			}
		}
		if peak := m.hints.Bytes(); peak > mt.HintBytesPeak.Load() {
			for {
				cur := mt.HintBytesPeak.Load()
				if peak <= cur || mt.HintBytesPeak.CompareAndSwap(cur, peak) {
					break
				}
			}
		}
	}
	if m.journal != nil {
		m.journal.AppendAdd(h)
		// Bound the file like C++ appendAdd does: past the record budget
		// rewrite from the live snapshot. Synchronous (brief lock hold),
		// so a long outage cannot grow hints.log without bound.
		if m.journal.NeedsCompaction() {
			// Best-effort: failure leaves the old journal intact and
			// the next enqueue retries.
			_ = m.journal.Compact(m.hints)
		}
	}
}

// Read performs a quorum read: local getVersioned plus concurrent
// GetVersioned fan-out, LWW-max reduce over R votes, background repair of
// stale replicas (and a local heal). Reads coalesce per (key, R): the
// first caller runs the fan-out, concurrent callers share its result.
func (m *Manager) Read(key string, replicas []string, r int, traceID, spanID uint64) (store.Entry, error) {
	if len(replicas) == 0 || r <= 1 {
		if e, ok := m.local.GetVersioned(key); ok {
			return e, nil
		}
		return store.Entry{}, ErrNotFound
	}
	if mt := m.metrics(); mt != nil {
		mt.QuorumReads.Add(1)
	}
	return m.readCoalesced(key, r, func() (store.Entry, error) {
		return m.readOnce(key, replicas, r, traceID, spanID)
	})
}

// readCoalesced runs one quorum fan-out per (key, R) and shares its
// result with concurrent readers, via x/sync singleflight (stdlib-adjacent
// shared implementation replacing the ported flightRegistry). shared=true
// marks a waiter, which is exactly the ReadsCoalesced signal; singleflight
// forgets the flight before waking waiters, so a waiter starting a new
// read begins a fresh flight — the same guarantee the old registry's
// delete-before-close gave.
func (m *Manager) readCoalesced(key string, r int, run func() (store.Entry, error)) (store.Entry, error) {
	v, err, shared := m.flights.Do(flightKey(key, r), func() (any, error) {
		return run()
	})
	if shared {
		if mt := m.metrics(); mt != nil {
			mt.ReadsCoalesced.Add(1)
		}
	}
	if err != nil {
		return store.Entry{}, err
	}
	e, _ := v.(store.Entry)
	return e, nil
}

// flightKey namespaces one coalesced flight. '\x00' cannot appear in a
// way that collides: keys are arbitrary bytes-as-string, so the length
// prefix keeps (key, R) pairs unambiguous.
func flightKey(key string, r int) string {
	return strconv.Itoa(r) + "\x00" + key
}

func (m *Manager) readOnce(key string, replicas []string, r int, traceID, spanID uint64) (store.Entry, error) {
	rd := &reducer{r: r, pending: len(replicas)}
	if e, ok := m.local.GetVersioned(key); ok {
		rd.noteLocal(e)
	}
	req := protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key), TraceID: traceID, SpanID: spanID}
	out := m.launch(req, replicas)
	for rd.pending > 0 && rd.acks < r {
		res := <-out
		rd.pending--
		rd.noteRemote(res.res, res.err)
	}
	// Drain stragglers in the background so no send outlives the operation.
	// Post-decision arrivals are ignored entirely (the reducer already
	// fired); the loop only drains the channel.
	go func() {
		for range rd.pending {
			<-out
		}
	}()
	best, ok := rd.winner()
	if !ok {
		// Served miss, not a failure: the quorum answered, the key is
		// simply absent everywhere.
		return store.Entry{}, ErrNotFound
	}
	if rd.repair {
		// Self-heal the local store when a replica was ahead, then write
		// the winner back everywhere best-effort.
		_ = m.local.PutVersioned(key, best)
		m.repair(key, replicas, best, traceID, spanID)
		if mt := m.metrics(); mt != nil {
			mt.ReadRepairs.Add(1)
		}
	}
	return best, nil
}

// repair writes the LWW winner back to every replica best-effort (no
// hints), mirroring sendRepairFanOut.
func (m *Manager) repair(key string, replicas []string, winner store.Entry, traceID, spanID uint64) {
	req := protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: append([]byte(nil), winner.Value...),
		Version: winner.Version, WriterNodeHash: winner.WriterNodeHash, Tombstone: winner.Tombstone,
		TraceID: traceID, SpanID: spanID,
	}
	if winner.HasTTL {
		req.HasExpiresAt = true
		req.ExpiresAt = winner.ExpiresAt
	}
	out := m.launch(req, replicas)
	go func() {
		for range replicas {
			<-out
		}
	}()
}

// reducer accumulates local + replica votes into one LWW winner. Only
// OK-with-version votes count as acks; NotFound marks repair; transport
// failures are ignored. Tombstone votes carry no value: the marker itself
// is the vote, and mapping it to NotFound is the caller's job.
type reducer struct {
	r       int
	pending int
	acks    int
	best    store.Entry
	hasBest bool
	repair  bool
}

func (d *reducer) noteLocal(e store.Entry) {
	d.acks++
	d.best = e.Clone()
	d.hasBest = true
}

func (d *reducer) noteRemote(res protocol.Response, err error) {
	if err != nil {
		return
	}
	if res.Status == protocol.StatusOK && res.Version != 0 {
		d.acks++
		vote := store.Stamp{Version: res.Version, Writer: res.WriterNodeHash, Tombstone: res.Tombstone}
		if !d.hasBest {
			d.best = voteToEntry(res)
			d.hasBest = true
			d.repair = true
			return
		}
		cur := store.Stamp{Version: d.best.Version, Writer: d.best.WriterNodeHash, Tombstone: d.best.Tombstone}
		if store.LWWWinsOver(vote, cur) {
			d.best = voteToEntry(res)
			d.repair = true
		} else if store.CompareLWW(vote, cur) != store.LWWEqual {
			d.repair = true
		}
	} else if res.Status == protocol.StatusNotFound {
		d.repair = true
	}
}

func voteToEntry(res protocol.Response) store.Entry {
	e := store.Entry{
		Version: res.Version, WriterNodeHash: res.WriterNodeHash, Tombstone: res.Tombstone,
		HasTTL: res.HasExpiresAt, ExpiresAt: res.ExpiresAt,
	}
	if res.HasValue {
		e.Value = append([]byte(nil), res.Value...)
	}
	return e
}

func (d *reducer) winner() (store.Entry, bool) {
	if !d.hasBest {
		return store.Entry{}, false
	}
	return d.best, true
}

// ReplayHints retries queued hints against healthy replicas, at most
// ReplayBatch FIFO. Concurrent calls collapse to zero; returns the replay
// count. Expired hints are dropped.
func (m *Manager) ReplayHints() int {
	if !m.replaying.CompareAndSwap(false, true) {
		return 0
	}
	defer m.replaying.Store(false)
	batch := m.hints.Snapshot(m.clock.Now(), ReplayBatch)
	var wg sync.WaitGroup
	var replayed atomic.Int64
	for _, h := range batch {
		wg.Add(1)
		go func(h Hint) {
			defer wg.Done()
			if _, err := m.transport.Send(h.Target, h.Req); err == nil {
				m.hints.Remove(h.ID)
				if m.journal != nil {
					m.journal.AppendDel(h.Target, string(h.Req.Key), h.Req.Version)
				}
				replayed.Add(1)
			}
		}(h)
	}
	wg.Wait()
	n := int(replayed.Load())
	if mt := m.metrics(); mt != nil {
		mt.HintsReplayed.Add(uint64(n))
	}
	return n
}
