package repl

// Outbound writes: quorum/async fan-out, hint enqueueing, and hint
// replay. The fan-out plumbing (fanResult/launch/drain) also serves
// the single quorum read in read.go.

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kraytos17/cinder/internal/expiry"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/store"
)

// Write commits the value locally before sending it to replicas. Async
// mode returns after the local commit; Quorum mode waits for the required
// acknowledgements, including the local write.

func (m *Manager) Write(key string, value []byte, ttl time.Duration, replicas []string, mode Mode, traceID, spanID uint64) error {
	if !mode.Valid() {
		return ErrInvalidMode
	}
	version := m.local.MintVersion()
	e := store.Entry{Value: append([]byte(nil), value...), Version: version, WriterNodeHash: m.writer}
	if ttl > 0 {
		// At, not After: matches store admission (zero is handled by
		// the caller passing none).
		e.Expiry = expiry.At(m.clock.Now().Add(ttl))
	}
	if err := m.local.PutVersioned(key, e); err != nil {
		return err
	}
	req := protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: append([]byte(nil), value...),
		Version: version, WriterNodeHash: m.writer,
		TraceID: traceID, SpanID: spanID,
	}
	if ttl > 0 {
		req.ExpiresAt = e.Expiry
	}
	if mode == Async {
		m.fanOutAsync(key, req, replicas)
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.AsyncWrites.Add(1)
		})
		return nil
	}
	err := m.fanOutQuorum(req, replicas)
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		if err != nil {
			mt.QuorumWritesErr.Add(1)
		} else {
			mt.QuorumWritesOK.Add(1)
		}
	})
	if err != nil {
		if e := m.log().Warn(); e != nil {
			log.Fields(e, req.TraceID, req.SpanID).Str("key", key).Msg("quorum write failed")
		}
	}
	return err
}

// Delete commits a local tombstone before sending it to replicas. Quorum
// mode succeeds only when enough replicas store the tombstone, so later
// quorum reads observe the deletion.
func (m *Manager) Delete(key string, replicas []string, mode Mode, traceID, spanID uint64) error {
	if !mode.Valid() {
		return ErrInvalidMode
	}
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
	if !tomb.Expiry.IsNone() {
		req.ExpiresAt = tomb.Expiry
	}
	if mode == Async {
		m.fanOutAsync(key, req, replicas)
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.AsyncDeletes.Add(1)
		})
		return nil
	}
	err := m.fanOutQuorum(req, replicas)
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		if err != nil {
			mt.QuorumDeletesEr.Add(1)
		} else {
			mt.QuorumDeletesOK.Add(1)
		}
	})
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
// immediately. Failed sends enqueue hints for later replay.

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
	h := Hint{Target: node, Req: req, ExpiresAt: expiry.At(m.clock.Now().Add(HintTTL))}
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.ReplicaUnreach.Add(1)
		mt.HintsEnqueued.Add(1)
	})
	m.hints.Push(h)
	// Funnel the queue's own drop count (oversize + evictions, including
	// pushes that bypass the manager) into the collector without
	// double-counting: monotonic max-sync.
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		obs.RetryMax(mt.HintsDropped.Load, mt.HintsDropped.CompareAndSwap, m.hints.Dropped())
		if peak := m.hints.Bytes(); peak > mt.HintBytesPeak.Load() {
			obs.RetryMax(mt.HintBytesPeak.Load, mt.HintBytesPeak.CompareAndSwap, peak)
		}
	})
	if m.journal != nil {
		m.journal.AppendAdd(h)
		// Bound the journal: past the record budget, rewrite it from the
		// live snapshot. Compaction is synchronous, so hints.log remains
		// bounded during a prolonged outage.
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
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.HintsReplayed.Add(uint64(n))
	})
	return n
}
