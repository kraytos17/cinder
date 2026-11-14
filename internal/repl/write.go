package repl

import (
	"bytes"
	"sync"
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
// acknowledgements, including the local write
func (m *Manager) Write(key string, value []byte, ttl time.Duration, replicas []string, mode Mode, traceID, spanID uint64) error {
	if !mode.Valid() {
		return ErrInvalidMode
	}

	version := m.local.MintVersion()
	// One copy serves both the local commit and the fan-out request:
	// PutVersioned clones into the store before returning, so sharing
	// the backing array between e.Value and req.Value is safe. The
	// encoder copies into frames and the hint queue retains req.
	valueCopy := bytes.Clone(value)
	e := store.Entry{Value: valueCopy, Version: version, WriterNodeHash: m.writer}
	if ttl > 0 {
		// At, not After: matches store admission (zero is handled by
		// the caller passing none).
		e.Expiry = expiry.At(m.clock.Now().Add(ttl))
	}
	if err := m.local.PutVersioned(key, e); err != nil {
		return err
	}

	req := protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: valueCopy,
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
// immediately. Failed sends enqueue hints for later replay. The
// single-replica case (RF=2) uses one background goroutine instead of the
// channel + sender + drainer trio.
func (m *Manager) fanOutAsync(key string, req protocol.Request, replicas []string) {
	if e := m.log().Debug(); e != nil {
		log.Fields(e, req.TraceID, req.SpanID).
			Str("key", key).
			Strs("replicas", replicas).
			Msg("replicating")
	}
	if len(replicas) == 1 {
		go func() {
			if _, err := m.transport.Send(replicas[0], req); err != nil {
				m.enqueueHint(replicas[0], req)
			}
		}()
		return
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
	if len(replicas) == 1 {
		// Fast path (RF=2): one inline Send, no channel or goroutines.
		if _, err := m.transport.Send(replicas[0], req); err != nil {
			m.enqueueHint(replicas[0], req)
			return ErrQuorum
		}
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
			if pending > 0 {
				go m.drainHints(req, out, pending)
			}
			return nil
		}
		if acks+pending < w {
			if pending > 0 {
				go m.drainHints(req, out, pending)
			}
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

// drainHints consumes straggler results after the quorum decision fired,
// queueing hints for late failures. Late failures count separately from
// in-window failures: the client already got its answer, but a replica
// is behind and will need a hint replay.
func (m *Manager) drainHints(req protocol.Request, out <-chan fanResult, n int) {
	drain(out, n, func(r fanResult) {
		if r.err != nil {
			m.enqueueHint(r.node, req)
			obs.IfPresent(&m.met, func(mt *metrics.Collector) { mt.QuorumLateFail.Add(1) })
		}
	})
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
			m.journal.Compact(m.hints)
		}
	}
}

func (m *Manager) ReplayHints() int {
	if !m.replaying.CompareAndSwap(false, true) {
		return 0
	}

	defer m.replaying.Store(false)
	batch := m.hints.Snapshot(m.clock.Now(), ReplayBatch)
	// Bounded workers instead of one goroutine per hint. Chunked
	// strides keep every hint attempted exactly once with far less
	// goroutine/channel churn and a smoother pool load. Successes are
	// collected per worker and applied as one RemoveIDs pass plus one
	// journal batch: per-success Remove scans would serialize 16 workers
	// on the queue mutex (256 O(n) scans) and per-success journal writes
	// would cost a lock+write each.
	workers := replayWorkers(len(batch))
	type outcome struct {
		ids  []uint64
		dels []HintDel
	}

	won := make([]outcome, workers)
	if workers > 0 {
		share := (len(batch) + workers - 1) / workers
		for w := range won {
			won[w].ids = make([]uint64, 0, share)
			if m.journal != nil {
				won[w].dels = make([]HintDel, 0, share)
			}
		}
	}

	journaled := m.journal != nil
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(batch); i += workers {
				h := batch[i]
				if _, err := m.transport.Send(h.Target, h.Req); err != nil {
					continue
				}

				won[w].ids = append(won[w].ids, h.ID)
				if journaled {
					won[w].dels = append(won[w].dels, HintDel{
						Target: h.Target, Key: h.Req.Key, Version: h.Req.Version,
					})
				}
			}
		}(w)
	}

	wg.Wait()
	// Presized to the batch: every worker outcome lands here, so this
	// avoids growth reallocs on every replay round. The dels buffer is
	// only allocated when a journal exists to receive it.
	ids := make([]uint64, 0, len(batch))
	var dels []HintDel
	if journaled {
		dels = make([]HintDel, 0, len(batch))
	}
	for w := range won {
		ids = append(ids, won[w].ids...)
		dels = append(dels, won[w].dels...)
	}

	m.hints.RemoveIDs(ids)
	if m.journal != nil {
		m.journal.AppendDels(dels)
	}

	n := len(ids)
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.HintsReplayed.Add(uint64(n))
	})
	return n
}

// replayWorkers bounds replay fan-out: one worker per hint up to the cap,
// then strided sharing. The cap keeps full-batch replays off the pool's
// hot path without serializing small batches.
func replayWorkers(n int) int {
	return min(max(n, 0), maxReplayWorkers)
}
