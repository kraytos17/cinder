package sim

// This file implements the deterministic cluster: nodes, membership,
// the event-driven pump, and the coordinator facade used by tests.
//
// Design notes (see doc.go for the full contract):
//
//   - Managers are used directly; no Server, no background goroutines,
//     no real I/O. Every clock reads the shared ManualClock.
//   - Membership is test-driven: Join/Leave/MarkDead/MarkAlive mutate the
//     cluster alive set, mirror the rumor into every node's table, then
//     rebuild all rings (mirroring Server.rebuildRingFromTable, including
//     the "always append self" rule).
//   - The coordinator facade mirrors dispatch.go handleSet/handleDelete
//     and the replicated handleGet path, including the notReady redirect
//     shape ("moved to <owner>").

import (
	"bytes"
	"cmp"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/shard"
	"github.com/kraytos17/cinder/internal/store"
)

// Store and counter byte budgets for sim nodes.
const (
	simStoreBytes   = 1 << 20
	simCounterBytes = 1 << 20
)

// Options configures a Cluster.
type Options struct {
	RF   hashring.ReplicaCount
	Mode repl.Mode
	Seed uint64
}

// Node is one simulated process: the same managers a Server wires, driven
// by the shared virtual clock and network. Persist is nil until
// EnablePersistence wires WAL + snapshots + the hint journal.
type Node struct {
	ID        string
	Store     *store.Store
	Counters  *counter.Store
	Repl      *repl.Manager
	AE        *ae.Manager
	Shard     *shard.Manager
	Persist   *persist.Manager
	Ring      *hashring.Ring
	Table     *cluster.MembershipTable
	Transport *SimTransport
}

// Cluster is a deterministic in-process cluster. It is not safe for
// concurrent use outside the pump helpers; tests drive it from one
// goroutine while the pump runs background operation goroutines to
// completion.
type Cluster struct {
	t     testing.TB
	clk   *store.ManualClock
	base  time.Time
	net   *net
	nodes map[string]*Node
	order []string
	rf    hashring.ReplicaCount
	mode  repl.Mode
	alive map[string]bool
	// dirs holds the disk dir per persistence-enabled node; incar tracks
	// the latest membership incarnation so restarts rejoin newer.
	dirs  map[string]string
	incar map[string]uint64
}

// NewCluster builds a cluster with ids nodes, converged membership, and
// identical rings on every node. RF below 1 selects 1.
func NewCluster(t testing.TB, ids []string, opt Options) *Cluster {
	t.Helper()
	rf := max(opt.RF, 1)
	base := time.Unix(1_700_000_000, 0).UTC()
	clk := store.NewManualClock(base)
	c := &Cluster{
		t: t, clk: clk, base: base,
		nodes: make(map[string]*Node, len(ids)),
		order: slices.Clone(ids),
		rf:    rf, mode: opt.Mode,
		alive: make(map[string]bool, len(ids)),
		dirs:  make(map[string]string, len(ids)),
		incar: make(map[string]uint64, len(ids)),
	}
	for _, id := range ids {
		c.alive[id] = true
	}

	c.net = newNetwork(c.elapsed, opt.Seed, c.deliver)
	for _, id := range ids {
		tn, ring, table := c.newIdentity(id)
		st := store.NewLRUStore(simStoreBytes, clk)
		counters := counter.NewStore(simCounterBytes)
		c.nodes[id] = c.wireNode(id, st, counters, tn, ring, table)
	}
	for id := range c.nodes {
		c.net.known[id] = true
	}

	c.rebuildRings()
	return c
}

// newIdentity builds the restart-stable parts of a node: its network
// endpoint (address identity), ring, and seeded membership table.
func (c *Cluster) newIdentity(id string) (*SimTransport, *hashring.Ring, *cluster.MembershipTable) {
	c.t.Helper()
	tn := &SimTransport{net: c.net, from: id}
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		c.t.Fatalf("sim: new ring: %v", err)
	}

	table := cluster.NewMembershipTable(id, c.clk.Now)
	peers := make([]cluster.NodeInfo, 0, len(c.order)-1)
	for _, other := range slices.DeleteFunc(slices.Clone(c.order), func(s string) bool { return s == id }) {
		peers = append(peers, cluster.NodeInfo{ID: other})
	}

	table.Seed(peers)
	return tn, ring, table
}

// wireNode builds fresh managers around st and counters, preserving the
// node's network identity, ring, and membership table. NewCluster wires
// fresh identity; restarts reuse the crashed node's.
func (c *Cluster) wireNode(id string, st *store.Store, counters *counter.Store, tn *SimTransport, ring *hashring.Ring, table *cluster.MembershipTable) *Node {
	replMgr := repl.NewManager(repl.Config{Local: st, Self: id, Clock: c.clk, Transport: tn})
	aeMgr := ae.NewManager(ae.Config{
		Store: st, Ring: ring, Self: id, Transport: tn,
		Clock: c.clk.Now,
	})

	aeMgr.SetCounterStore(counters)
	shardMgr := shard.NewManager(shard.Config{
		Store: st, Ring: ring, Transport: tn, Membership: table,
		Self: id, ReplicaFactor: c.rf, Quarantine: 0, Clock: c.clk.Now,
	})
	return &Node{
		ID: id, Store: st, Counters: counters,
		Repl: replMgr, AE: aeMgr, Shard: shardMgr,
		Ring: ring, Table: table, Transport: tn,
	}
}

func (c *Cluster) elapsed() time.Duration { return c.clk.Now().Sub(c.base) }

// Node returns the named node or fails the test.
func (c *Cluster) Node(id string) *Node {
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("sim: unknown node %s", id)
	}
	return n
}

// IDs returns the cluster node ids in construction order.
func (c *Cluster) IDs() []string { return slices.Clone(c.order) }

// Transport exposes the shared-network endpoint of the named node for
// fault injection (Partition/SetDelay/SetLoss/SetJitter/SetReorder).
// Reachability faults use Suspend/Resume (transport-only) and
// MarkDead/MarkAlive (FD conviction with ring rebuild).
func (c *Cluster) Transport(id string) *SimTransport { return c.Node(id).Transport }

// Events returns a copy of the network event log in insertion order.
func (c *Cluster) Events() []event { return append([]event(nil), c.net.log...) }

// SortedEvents returns the log sorted by (time, from, to, opcode, key,
// version, outcome): the canonical form for the determinism check.
func (c *Cluster) SortedEvents() []event {
	out := c.Events()
	slices.SortFunc(out, func(a, b event) int {
		return cmp.Or(
			cmp.Compare(a.T, b.T),
			cmp.Compare(a.From, b.From),
			cmp.Compare(a.To, b.To),
			cmp.Compare(a.Op, b.Op),
			cmp.Compare(a.Key, b.Key),
			cmp.Compare(a.Ver, b.Ver),
			cmp.Compare(a.Out, b.Out),
		)
	})
	return out
}

// rebuildRings publishes the current alive set as every node's ring,
// mirroring Server.rebuildRingFromTable: every alive member is included
// and self is always appended (a lone node owns everything).
func (c *Cluster) rebuildRings() {
	c.t.Helper()
	ids := make([]string, 0, len(c.order))
	for _, id := range c.order {
		if c.alive[id] {
			ids = append(ids, id)
		}
	}
	for _, id := range c.order {
		n := c.nodes[id]
		present := slices.Contains(ids, id)
		ringIDs := ids
		if !present {
			ringIDs = append(slices.Clone(ids), id)
		}
		if err := n.Ring.SetNodes(ringIDs); err != nil {
			c.t.Fatalf("sim: set nodes: %v", err)
		}
	}
}

// Suspend cuts all traffic to and from id without touching membership
// or rings: the crashed-but-not-yet-convicted window where the peer is
// still a replica target, so failed sends become hints. Resume reverses
// it. MarkDead/MarkAlive instead drive FD conviction (ring removal).
func (c *Cluster) Suspend(id string) {
	c.Node(id)
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	c.net.down[id] = true
}

// Resume reverses Suspend.
func (c *Cluster) Resume(id string) {
	c.Node(id)
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	delete(c.net.down, id)
}

// MarkDead flags id dead on every node's table (converged gossip) and
// rebuilds rings, mirroring FD conviction + Server.rebuildRingFromTable.
// Table order follows construction order; the per-table mutations are
// disjoint, so the converged view is order-independent. In-flight traffic
// involving id drops at delivery.
func (c *Cluster) MarkDead(id string) {
	c.Node(id)
	for _, nid := range c.order {
		c.nodes[nid].Table.MarkDead(id)
	}

	c.alive[id] = false
	c.net.mu.Lock()
	c.net.down[id] = true
	c.net.mu.Unlock()
	c.rebuildRings()
}

// MarkAlive recovers id with a fresh incarnation on every node's table
// and rebuilds rings. Quarantine is zero in sim, so the node is
// immediately eligible.
func (c *Cluster) MarkAlive(id string, incarnation uint64) {
	c.Node(id)
	for _, nid := range c.order {
		c.nodes[nid].Table.MarkAlive(id, incarnation)
	}

	c.alive[id] = true
	c.net.mu.Lock()
	delete(c.net.down, id)
	c.net.mu.Unlock()
	c.rebuildRings()
}

// MarkDeadOn applies a one-sided FD conviction: only observerID's table
// marks target dead, and only its ring rebuilds. This models the
// pre-convergence window where each side of a partition believes itself
// the sole owner — the setup for split-brain divergence tests. Prefer
// MarkDead (converged) unless the test needs asymmetric views.
func (c *Cluster) MarkDeadOn(observerID, targetID string) {
	c.Node(observerID).Table.MarkDead(targetID)
	c.rebuildNodeRing(observerID)
}

// rebuildNodeRing rebuilds one node's ring from its own table view,
// always appending self like Server.rebuildRingFromTable does.
func (c *Cluster) rebuildNodeRing(id string) {
	c.t.Helper()
	n := c.nodes[id]
	snap := n.Table.Snapshot()
	ids := make([]string, 0, len(snap)+1)
	for _, m := range snap {
		if m.State == cluster.Alive {
			ids = append(ids, m.ID)
		}
	}
	if !slices.Contains(ids, id) {
		ids = append(ids, id)
	}
	if err := n.Ring.SetNodes(ids); err != nil {
		c.t.Fatalf("sim: set nodes: %v", err)
	}
}

// Partition cuts all traffic between sets a and b in both directions.
func (c *Cluster) Partition(a, b []string) {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	cut := cut{a: make(map[string]bool, len(a)), b: make(map[string]bool, len(b))}
	for _, id := range a {
		cut.a[id] = true
	}
	for _, id := range b {
		cut.b[id] = true
	}
	c.net.cuts = append(c.net.cuts, cut)
}

// HealAll removes every partition cut.
func (c *Cluster) HealAll() {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	c.net.cuts = nil
}

// Advance moves the virtual clock forward.
func (c *Cluster) Advance(d time.Duration) { c.clk.Advance(d) }

// Pump delivers queued events to quiescence. Each wave jumps virtual time
// to its deliver_at; when the queue is empty only parked senders remain,
// which means deadlock — fail loudly instead of hanging.
//
// Background fan-out (async replication, shard migration) spawns real
// goroutines that Send on the network, so Pump must observe both the
// queue and parked senders: loop while either is nonzero.
func (c *Cluster) Pump() {
	c.t.Helper()
	idleRuns := 0
	for range 1 << 20 {
		c.net.mu.Lock()
		idle := len(c.net.pending) == 0 && c.net.inflight.Load() == 0
		c.net.mu.Unlock()
		if !idle {
			idleRuns = 0
			c.pumpWave()
			continue
		}
		// Quorum fan-out returns after W acks while a straggler send
		// may not have queued yet; the idle check cannot see
		// not-yet-started goroutines. Yield and require quiescence to
		// persist across consecutive observations before accepting
		// it: a runnable straggler gets scheduled during a yield and
		// its send shows up as queue activity on the next pass.
		// Without this, a straggler that starts after the final drain
		// is silently missing from the event log.
		idleRuns++
		if idleRuns >= 3 {
			return
		}
		runtime.Gosched()
	}
	c.t.Fatal("sim: pump did not converge")
}

// do runs fn in a goroutine while pumping the network on the caller's
// goroutine, then drains stragglers. This is the only way to drive
// blocking operations (quorum writes, AE rounds): the operation parks on
// its Send channel and the pump delivers it. The caller must never block
// on done without pumping: fn can only complete via deliveries this loop
// performs, so an empty queue just yields until fn queues or finishes.
func (c *Cluster) do(fn func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		fn()
	})

	for range 1 << 20 {
		select {
		case <-done:
			c.Pump()
			wg.Wait()
			return
		default:
		}
		c.pumpWave()
	}
	c.t.Fatal("sim: operation did not complete")
}

// pumpWave delivers one wave: advance to the earliest due event, deliver
// everything due now. With an empty queue it yields so a starting
// operation goroutine can queue its first send.
func (c *Cluster) pumpWave() {
	c.net.mu.Lock()
	if len(c.net.pending) == 0 {
		c.net.mu.Unlock()
		runtime.Gosched()
		return
	}

	var at time.Duration
	first := true
	for _, ev := range c.net.pending {
		if first || ev.at < at {
			at = ev.at
			first = false
		}
	}

	now := c.elapsed()
	if at > now {
		c.net.mu.Unlock()
		c.clk.Advance(at - now)
		c.net.mu.Lock()
	}

	c.net.deliverLocked()
	c.net.mu.Unlock()
}

// deliver routes one message to its target node, mirroring tcp dispatch:
// replicate/hint apply versioned writes, versioned reads serve voters,
// AE frames hit the AE manager, counter frames hit the counter store.
func (c *Cluster) deliver(to string, req protocol.Request) (protocol.Response, bool) {
	n, ok := c.nodes[to]
	if !ok {
		return protocol.Response{}, false
	}
	switch req.Opcode {
	case protocol.OpReplicate, protocol.OpHint:
		return applyReplicated(n, req), true
	case protocol.OpGetVersioned:
		return serveVersioned(n, req), true
	case protocol.OpAntiEntropyDigest:
		if string(req.Key) == ae.RefinementKey {
			st, val := n.AE.OnBucketRefinement(req.Value)
			return protocol.Response{Status: st, Value: val, HasValue: val != nil}, true
		}

		st, val := n.AE.OnDigestRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, true
	case protocol.OpAntiEntropySync:
		st, val := n.AE.OnSyncRequest(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, true
	case protocol.OpCounterMerge:
		st, err := counter.DecodePNCounter(req.Value)
		if err != nil {
			return protocol.Response{Status: protocol.StatusInvalidArgument}, true
		}
		if err := n.Counters.Merge(string(req.Key), st); err != nil {
			return protocol.Response{Status: protocol.StatusInternalError}, true
		}
		return protocol.Response{Status: protocol.StatusOK}, true
	case protocol.OpCounterAntiEntropy:
		st, val := n.AE.OnCounterAntiEntropy(req.Value)
		return protocol.Response{Status: st, Value: val, HasValue: val != nil}, true
	case protocol.OpGossip:
		// Membership is test-driven converged gossip; nothing to do.
		return protocol.Response{Status: protocol.StatusOK}, true
	default:
		return protocol.Response{Status: protocol.StatusNotSupported}, true
	}
}

// storeStatusOf mirrors the store arm of dispatch statusOf: capacity is
// distinct, everything else is an internal error.
func storeStatusOf(err error) protocol.Status {
	if errors.Is(err, store.ErrCapacityExceeded) {
		return protocol.StatusCapacityExceeded
	}
	return protocol.StatusInternalError
}

// applyReplicated mirrors tcp Server.applyReplicated: tombstones without
// an absolute expiry take the versioned-remove path (local tombstone-GC
// deadline); everything else is a versioned put.
func applyReplicated(n *Node, req protocol.Request) protocol.Response {
	if req.Tombstone && req.ExpiresAt.IsNone() {
		if err := n.Store.RemoveVersioned(string(req.Key), req.Version, req.WriterNodeHash); err != nil {
			return protocol.Response{Status: storeStatusOf(err)}
		}
		return protocol.Response{Status: protocol.StatusOK}
	}

	e := store.Entry{
		Value:          bytes.Clone(req.Value),
		Version:        req.Version,
		WriterNodeHash: req.WriterNodeHash,
		Tombstone:      req.Tombstone,
	}
	if !req.ExpiresAt.IsNone() {
		e.Expiry = req.ExpiresAt
	}
	if err := n.Store.PutVersioned(string(req.Key), e); err != nil {
		return protocol.Response{Status: storeStatusOf(err)}
	}
	return protocol.Response{Status: protocol.StatusOK}
}

// serveVersioned mirrors handleGetVersioned without the redirect: the sim
// routes by node id directly, so a miss is NotFound.
func serveVersioned(n *Node, req protocol.Request) protocol.Response {
	e, ok := n.Store.GetVersioned(string(req.Key))
	if !ok {
		return protocol.Response{Status: protocol.StatusNotFound}
	}

	res := protocol.Response{
		Status:         protocol.StatusOK,
		Version:        e.Version,
		WriterNodeHash: e.WriterNodeHash,
		Tombstone:      e.Tombstone,
		ExpiresAt:      e.Expiry,
	}
	if !e.Tombstone {
		res.Value = e.Value
		res.HasValue = true
	}
	return res
}

// ownerOf mirrors tcp Server.ownerOf: ring owner, or self when the ring
// is empty (cannot happen after rebuildRings, kept for parity).
func (c *Cluster) ownerOf(node *Node, key string) string {
	if owner := node.Ring.GetNode(key); owner != "" {
		return owner
	}
	return node.ID
}

// replicaNodes mirrors tcp Server.replicaNodes: the ring replica set
// excluding the coordinator.
func (c *Cluster) replicaNodes(node *Node, key string) []string {
	nodes, err := node.Ring.GetNodes(key, c.rf)
	if err != nil || len(nodes) == 0 {
		return nil
	}

	out := nodes[:0:0]
	for _, n := range nodes {
		if n != node.ID {
			out = append(out, n)
		}
	}
	return out
}

// WriteResult is the coordinator outcome: either OK or a redirect/capacity
// signal carried as a status, mirroring dispatch writeResult/statusOf.
type WriteResult struct {
	Status   protocol.Status
	Redirect string
}

// Write routes a client write through nodeID: ownership-gated, then
// quorum/async fan-out when replicated, else a local versioned put.
// Mirrors handleSet.
func (c *Cluster) Write(nodeID, key string, value []byte, ttl time.Duration) WriteResult {
	var out WriteResult
	c.do(func() {
		n := c.nodes[nodeID]
		if owner := c.ownerOf(n, key); owner != nodeID {
			out = WriteResult{Status: protocol.StatusNotReady, Redirect: "moved to " + owner}
			return
		}
		if c.rf > 1 {
			err := n.Repl.Write(key, value, ttl, c.replicaNodes(n, key), c.mode, 0, 0)
			out = WriteResult{Status: writeStatus(err)}
			return
		}

		var err error
		if ttl > 0 {
			err = n.Store.PutWithTTL(key, value, ttl)
		} else {
			err = n.Store.Put(key, value)
		}
		out = WriteResult{Status: writeStatus(err)}
	})
	return out
}

// Delete routes a client delete through nodeID, mirroring handleDelete.
func (c *Cluster) Delete(nodeID, key string) WriteResult {
	var out WriteResult
	c.do(func() {
		n := c.nodes[nodeID]
		if owner := c.ownerOf(n, key); owner != nodeID {
			out = WriteResult{Status: protocol.StatusNotReady, Redirect: "moved to " + owner}
			return
		}
		if c.rf > 1 {
			err := n.Repl.Delete(key, c.replicaNodes(n, key), c.mode, 0, 0)
			out = WriteResult{Status: writeStatus(err)}
			return
		}

		err := n.Store.RemoveVersioned(key, n.Store.MintVersion(), 0)
		out = WriteResult{Status: writeStatus(err)}
	})
	return out
}

func writeStatus(err error) protocol.Status {
	switch {
	case err == nil:
		return protocol.StatusOK
	case errors.Is(err, store.ErrCapacityExceeded):
		// Parity with dispatch statusOf: capacity failures are not
		// quorum failures.
		return protocol.StatusCapacityExceeded
	default:
		return protocol.StatusNotReady
	}
}

// counterStatusOf mirrors counterStatusOf: capacity and actor failures
// keep their distinct statuses instead of collapsing to NotReady.
func counterStatusOf(err error) protocol.Status {
	switch {
	case err == nil:
		return protocol.StatusOK
	case errors.Is(err, counter.ErrCapacityExceeded):
		return protocol.StatusCapacityExceeded
	case errors.Is(err, counter.ErrInvalidActor):
		return protocol.StatusInvalidArgument
	default:
		return protocol.StatusInternalError
	}
}

// CounterAdjust applies a counter delta through nodeID, mirroring
// handleCounterAdjust: ownership-gated, zero-delta no-op, then full-state
// fan-out when replicated.
func (c *Cluster) CounterAdjust(nodeID, key string, delta uint64, decrement bool) protocol.Status {
	var st protocol.Status
	c.do(func() {
		n := c.nodes[nodeID]
		if owner := c.ownerOf(n, key); owner != nodeID {
			st = protocol.StatusNotReady
			return
		}
		if delta == 0 {
			st = protocol.StatusOK
			return
		}

		var err error
		if decrement {
			err = n.Counters.Decrement(key, nodeID, delta)
		} else {
			err = n.Counters.Increment(key, nodeID, delta)
		}

		if err != nil {
			st = counterStatusOf(err)
			return
		}

		ctr, ok := n.Counters.Get(key)
		if !ok {
			st = protocol.StatusInternalError
			return
		}
		if c.rf > 1 {
			n.Repl.ReplicateCounter(key, ctr.Encode(), c.replicaNodes(n, key), 0, 0)
		}
		st = protocol.StatusOK
	})
	return st
}

// CounterGet serves the local counter copy when present, mirroring
// handleCounterGet: redirect to the ring owner only on a miss.
func (c *Cluster) CounterGet(nodeID, key string) ([]byte, protocol.Status) {
	var (
		enc []byte
		st  protocol.Status
	)

	c.do(func() {
		n := c.nodes[nodeID]
		if ctr, ok := n.Counters.Get(key); ok {
			enc = ctr.Encode()
			if len(enc) == 0 {
				st = protocol.StatusInternalError
				return
			}

			st = protocol.StatusOK
			return
		}
		if owner := c.ownerOf(n, key); owner != nodeID {
			st = protocol.StatusNotReady
			return
		}
		st = protocol.StatusNotFound
	})
	return enc, st
}

// Get routes a client read through nodeID, mirroring the replicated
// handleGet path (quorum fan-out when rf > 1, else local copy with
// redirect-on-miss). Tombstones read as NotFound. Quorum reads repair
// via repl.Read, so this is also the read-repair path tests assert.
func (c *Cluster) Get(nodeID, key string) (store.Entry, protocol.Status) {
	var (
		entry store.Entry
		st    protocol.Status
	)

	c.do(func() {
		n := c.nodes[nodeID]
		if c.rf > 1 {
			e, err := n.Repl.Read(key, c.replicaNodes(n, key), c.rf, 0, 0)
			if err != nil {
				// Parity with handleGet: a quorum of misses is
				// NotFound, anything else is NotReady.
				if errors.Is(err, repl.ErrNotFound) {
					st = protocol.StatusNotFound
				} else {
					st = protocol.StatusNotReady
				}
				return
			}
			if e.Tombstone {
				st = protocol.StatusNotFound
				return
			}

			entry, st = e, protocol.StatusOK
			return
		}

		val, ok := n.Store.Get(key)
		if !ok {
			if owner := c.ownerOf(n, key); owner != nodeID {
				st = protocol.StatusNotReady
				return
			}

			st = protocol.StatusNotFound
			return
		}
		entry, st = store.Entry{Value: val}, protocol.StatusOK
	})
	return entry, st
}

// Rebalance runs one rebalance pass on every node, pumping migration
// traffic between passes so multi-hop moves converge.
func (c *Cluster) Rebalance() {
	for _, id := range c.order {
		c.do(func() { c.nodes[id].Shard.Rebalance() })
	}
	c.Pump()
}

// RunAE runs one anti-entropy round from nodeID against its picked
// partner, pumping the digest/sync exchange.
func (c *Cluster) RunAE(nodeID string) int {
	var n int
	c.do(func() { n = c.nodes[nodeID].AE.RunRound(c.rf) })
	return n
}

// RunCounterRound runs one counter anti-entropy exchange from nodeID
// against its picked partner, pumping the bundle exchange. Missed merges
// (best-effort, never hinted) converge here.
func (c *Cluster) RunCounterRound(nodeID string) {
	c.do(func() { c.nodes[nodeID].AE.RunCounterRound(c.rf) })
}

// ReplayHints runs hint replay on nodeID, pumping redelivery.
func (c *Cluster) ReplayHints(nodeID string) int {
	var n int
	c.do(func() { n = c.nodes[nodeID].Repl.ReplayHints() })
	return n
}

// WaitForHint spins until nodeID holds at least one queued hint. Async
// fan-out enqueues hints from a background goroutine after Write returns,
// so tests must rendezvous with it before replaying. Each iteration pumps
// one wave: a queued send is delivered (a fault completes as an error and
// the hint is queued), while an empty queue just yields so a starting
// goroutine can queue its first send. Spinning without pumping would miss
// hints whose sends are still parked, and parking without yielding would
// starve the goroutine that queues them.
func (c *Cluster) WaitForHint(nodeID string) {
	c.t.Helper()
	for range 1 << 20 {
		if c.nodes[nodeID].Repl.HintCount() > 0 {
			return
		}
		c.pumpWave()
	}
	c.t.Fatalf("sim: no hint queued on %s", nodeID)
}
