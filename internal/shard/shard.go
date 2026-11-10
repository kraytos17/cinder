// Package shard migrates keys when cluster membership changes. After a
// ring rebuild, the manager snapshots local entries and reconciles each
// key with its current replica set (primary plus RF-1 successors). Nodes
// that remain in the set keep their copy and push updates to other members;
// nodes removed from the set delete local data only after every new holder
// acknowledges the migration.
//
// Two-phase design: enumerate first (LiveEntries is already a snapshot),
// send after — the enumeration never sends, so no re-entrant deadlock.
// Quarantined targets defer (caller retries after the window); dead or
// suspect targets are skipped at send time. Transport failures on migrate
// keep the local copy and fire the migration-failed callback so the owner
// can schedule a delayed re-rebalance; push failures are best-effort.
//
// The migration manager logs operational events and exposes metrics; it
// does not add a tracing subsystem.
package shard

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/obs"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/transport"
)

// Transport is the shared inter-node send contract (see
// internal/transport); *client.Pool satisfies it.
type Transport = transport.Sender

// nopLogger is the shared disabled logger for managers constructed
// without SetLogger.
var nopLogger = log.Nop()

// Manager reconciles the local store against the ring. Not safe for
// concurrent Rebalance calls; the server serializes them through its
// debounce gate.
type Manager struct {
	store      *store.Store
	ring       *hashring.Ring
	transport  Transport
	table      *cluster.MembershipTable
	self       string
	now        func() time.Time
	rf         int
	quarantine time.Duration

	onMigrationFailed func()
	met               atomic.Pointer[metrics.Collector]
	logger            atomic.Pointer[zerolog.Logger]
}

// NewManager returns a Manager. rf < 1 selects 1 (no replication).
// A nil now selects time.Now.
func NewManager(
	st *store.Store,
	ring *hashring.Ring,
	t Transport,
	table *cluster.MembershipTable,
	self string,
	rf int,
	quarantine time.Duration,
	now func() time.Time,
) *Manager {
	if rf < 1 {
		rf = 1
	}
	if now == nil {
		now = time.Now
	}
	return &Manager{
		store: st, ring: ring, transport: t, table: table,
		self: self, now: now, rf: rf, quarantine: quarantine,
	}
}

// SetOnMigrationFailed wires the cheap, non-blocking callback invoked on
// the rebalance goroutine each time a primary migration send fails. It
// schedules a debounced retry, never retries inline.
func (m *Manager) SetOnMigrationFailed(cb func()) {
	m.onMigrationFailed = cb
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

// log returns the installed logger (Nop when unset).
func (m *Manager) log() *zerolog.Logger {
	if l := m.logger.Load(); l != nil {
		return l
	}
	return &nopLogger
}

// Rebalance pushes keys this node no longer owns to their new ring owners.
// It returns true when at least one key was deferred because an owner is
// still inside the quarantine window (retry after it clears). Transport
// failures are not reflected: a failed migrate keeps the local copy and
// fires the migration-failed callback instead.
func (m *Manager) Rebalance() bool {
	type copyPush struct {
		key   string
		entry store.Entry
		owner string
	}
	type migratePush struct {
		key     string
		entry   store.Entry
		primary string
		peers   []string
	}

	var copies []copyPush
	var migrates []migratePush
	deferred := 0
	now := m.now()
	// LiveEntries is a point-in-time snapshot: collection sends nothing,
	// so a synchronous transport can never deadlock re-entrantly here.
	for _, p := range m.store.LiveEntries() {
		desired, err := m.ring.GetNodes(p.Key, m.rf)
		if err != nil || len(desired) == 0 {
			continue
		}
		if contains(desired, m.self) {
			// Keep the local copy; repair every other member.
			for _, owner := range desired {
				if owner == m.self {
					continue
				}
				if m.table.IsQuarantined(owner, now, m.quarantine) {
					deferred++
					continue
				}
				copies = append(copies, copyPush{key: p.Key, entry: p.Entry, owner: owner})
			}
			continue
		}
		// Leaving the set: migrate only once EVERY new owner has acked.
		if m.table.IsQuarantined(desired[0], now, m.quarantine) {
			deferred++
			continue
		}
		mp := migratePush{key: p.Key, entry: p.Entry, primary: desired[0]}
		for _, r := range desired[1:] {
			if m.table.IsQuarantined(r, now, m.quarantine) {
				deferred++
				continue
			}
			mp.peers = append(mp.peers, r)
		}
		migrates = append(migrates, mp)
	}

	for _, c := range copies {
		m.pushReplica(c.key, c.entry, c.owner)
	}
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.RebalanceCopies.Add(uint64(len(copies)))
		mt.RebalanceMigrations.Add(uint64(len(migrates)))
	})
	for _, mg := range migrates {
		m.migrateKey(mg.key, mg.entry, mg.primary, mg.peers)
	}
	return deferred > 0
}

// makeReplicateRequest builds a Replicate request containing the entry's
// LWW identity and absolute expiry so replicas use the same deadline.
func makeReplicateRequest(key string, e store.Entry) protocol.Request {
	req := protocol.Request{
		Opcode:         protocol.OpReplicate,
		Key:            []byte(key),
		Value:          append([]byte(nil), e.Value...),
		Version:        e.Version,
		WriterNodeHash: e.WriterNodeHash,
		Tombstone:      e.Tombstone,
	}
	if !e.Expiry.IsNone() {
		req.ExpiresAt = e.Expiry
	}
	return req
}

func (m *Manager) alive(id string) bool {
	info, ok := m.table.Get(id)
	return !ok || info.State == cluster.Alive
}

// migrateKey sends the entry to the whole new set and drops the local copy
// only once every member has acked. A primary failure keeps the local copy
// as a failover copy and fires the retry callback; a primary ack with
// unconfirmed replicas likewise retains the copy for the next round.
func (m *Manager) migrateKey(key string, e store.Entry, primary string, peers []string) {
	if !m.alive(primary) {
		return
	}
	expected := 1 + len(peers)
	var acks atomic.Int64
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			if !m.alive(node) {
				return
			}
			if _, err := m.transport.Send(node, makeReplicateRequest(key, e)); err == nil {
				acks.Add(1)
			}
		}(peer)
	}
	if e := m.log().Debug(); e != nil {
		e.Str("key", key).Str("primary", primary).Strs("peers", peers).Msg("migrating")
	}
	_, err := m.transport.Send(primary, makeReplicateRequest(key, e))
	wg.Wait()
	if err != nil {
		if ev := m.log().Warn(); ev != nil {
			ev.Str("key", key).Str("primary", primary).Msg("migration failed")
		}
		obs.IfPresent(&m.met, func(mt *metrics.Collector) {
			mt.RebalanceMigFailures.Add(1)
		})
		if m.onMigrationFailed != nil {
			m.onMigrationFailed()
		}
		return
	}
	if acks.Add(1) == int64(expected) {
		// Every new holder confirmed: the local copy is a redundant
		// orphan. Hard erase (tombstones migrate like any entry; only
		// this post-handoff cleanup erases).
		m.store.Remove(key)
		return
	}
	// Otherwise retained-underreplicated: still served locally, retried
	// next round; hinted handoff and anti-entropy cover it independently.
	obs.IfPresent(&m.met, func(mt *metrics.Collector) {
		mt.RebalanceDroppedUnder.Add(1)
	})
}

// pushReplica sends one idempotent copy. Failures are best-effort (the
// version-gated apply makes duplicates harmless and the next rebalance,
// hint replay, or anti-entropy round retries).
func (m *Manager) pushReplica(key string, e store.Entry, owner string) {
	if !m.alive(owner) {
		return
	}
	_, _ = m.transport.Send(owner, makeReplicateRequest(key, e))
}

func contains(ids []string, id string) bool {
	return slices.Contains(ids, id)
}
