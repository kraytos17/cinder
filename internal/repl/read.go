package repl

import (
	"strconv"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/store"
)

// Read performs a local read or a quorum read, depending on the replica set
// and requested quorum. Concurrent quorum reads for the same key and R
// share one in-flight fan-out.
func (m *Manager) Read(key string, replicas []string, r hashring.ReplicaCount, traceID, spanID uint64) (store.Entry, error) {
	if r < 1 || r > hashring.MaxReplicas {
		return store.Entry{}, ErrInvalidReplicaCount
	}
	if len(replicas) == 0 || r <= 1 {
		if e, ok := m.local.GetVersioned(key); ok {
			return e, nil
		}
		return store.Entry{}, ErrNotFound
	}

	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.QuorumReads.Add(1)
	})
	return m.readCoalesced(key, r, func() (store.Entry, error) {
		return m.readOnce(key, replicas, r, traceID, spanID)
	})
}

// readCoalesced runs one quorum fan-out per (key, R) and shares its
// result with concurrent readers via x/sync/singleflight. shared reports
// that multiple callers used the same in-flight operation and drives the
// ReadsCoalesced metric.

func (m *Manager) readCoalesced(key string, r hashring.ReplicaCount, run func() (store.Entry, error)) (store.Entry, error) {
	v, err, shared := m.flights.Do(flightKey(key, r), func() (any, error) {
		return run()
	})
	if shared {
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.ReadsCoalesced.Add(1)
		})
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
func flightKey(key string, r hashring.ReplicaCount) string {
	return strconv.Itoa(int(r)) + "\x00" + key
}

func (m *Manager) readOnce(key string, replicas []string, r hashring.ReplicaCount, traceID, spanID uint64) (store.Entry, error) {
	rd := &reducer{r: int(r), pending: len(replicas)}
	if e, ok := m.local.GetVersioned(key); ok {
		rd.noteLocal(e)
	}

	req := protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key), TraceID: traceID, SpanID: spanID}
	out := m.launch(req, replicas)
	for rd.pending > 0 && rd.acks < int(r) {
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
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.ReadRepairs.Add(1)
		})
	}
	return best, nil
}

// repair writes the LWW winner back to every replica on a best-effort
// basis. Failed repairs are not queued as hints; later reads or
// anti-entropy can repair them.
func (m *Manager) repair(key string, replicas []string, winner store.Entry, traceID, spanID uint64) {
	req := protocol.Request{
		Opcode: protocol.OpReplicate, Key: []byte(key), Value: append([]byte(nil), winner.Value...),
		Version: winner.Version, WriterNodeHash: winner.WriterNodeHash, Tombstone: winner.Tombstone,
		TraceID: traceID, SpanID: spanID,
	}
	if !winner.Expiry.IsNone() {
		req.ExpiresAt = winner.Expiry
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

// stampOfResponse is the single canonical conversion from wire response
// to LWW comparison currency (see Entry.Stamp for why it exists).
func stampOfResponse(res protocol.Response) store.Stamp {
	return store.Stamp{Version: res.Version, Writer: res.WriterNodeHash, Tombstone: res.Tombstone}
}

func (d *reducer) noteRemote(res protocol.Response, err error) {
	if err != nil {
		return
	}
	if res.Status == protocol.StatusOK && res.Version != 0 {
		d.acks++
		vote := stampOfResponse(res)
		if !d.hasBest {
			d.best = voteToEntry(res)
			d.hasBest = true
			d.repair = true
			return
		}

		cur := d.best.Stamp()
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
		Expiry: res.ExpiresAt,
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
