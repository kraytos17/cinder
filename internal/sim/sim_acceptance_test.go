package sim_test

import (
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/sim"
)

// peerOf returns the single non-owner id. All peer-based tests use
// two-node clusters, so exactly one such id exists.
func peerOf(t testing.TB, c *sim.Cluster, owner string) string {
	t.Helper()
	for _, id := range c.IDs() {
		if id != owner {
			return id
		}
	}
	t.Fatalf("no peer of %s", owner)
	return ""
}

func quorumCluster(t testing.TB, seed uint64) *sim.Cluster {
	t.Helper()
	return sim.NewCluster(t, []string{"n1", "n2", "n3"}, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
}

func ownerOf(t testing.TB, c *sim.Cluster, key string) string {
	t.Helper()
	for _, id := range c.IDs() {
		if r := c.Write(id, key, []byte("probe"), 0); r.Status == protocol.StatusOK {
			// Probe write succeeded; delete it so the script starts clean.
			c.Delete(id, key)
			return id
		}
	}
	t.Fatalf("no owner accepted key %q", key)
	return ""
}

// Same script on two fresh clusters with faults disabled must produce
// identical sorted event logs: the core determinism guarantee.
func TestSimDeterministicLogs(t *testing.T) {
	script := func(t testing.TB) *sim.Cluster {
		c := quorumCluster(t, 7)
		owner := ownerOf(t, c, "det-key")
		if r := c.Write(owner, "det-key", []byte("v1"), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write: %+v", r)
		}
		if _, st := c.Get("n1", "det-key"); st != protocol.StatusOK {
			t.Fatalf("get: %d", uint8(st))
		}
		c.RunAE("n1")
		c.RunAE("n2")
		c.Rebalance()
		if r := c.Delete(owner, "det-key"); r.Status != protocol.StatusOK {
			t.Fatalf("delete: %+v", r)
		}
		// The tombstone converges too: every node reads NotFound.
		for _, id := range c.IDs() {
			if _, st := c.Get(id, "det-key"); st != protocol.StatusNotFound {
				t.Fatalf("node %s after delete: status %d, want NotFound", id, uint8(st))
			}
		}
		return c
	}
	a, b := script(t), script(t)
	ea, eb := a.SortedEvents(), b.SortedEvents()
	if len(ea) == 0 {
		t.Fatal("empty event log")
	}
	if !slices.Equal(ea, eb) {
		t.Fatalf("logs differ: %d vs %d events", len(ea), len(eb))
	}
}

// Quorum write lands on every replica; every node reads it back.
func TestSimQuorumWriteConverges(t *testing.T) {
	c := quorumCluster(t, 1)
	owner := ownerOf(t, c, "k1")
	if r := c.Write(owner, "k1", []byte("v"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("write: %+v", r)
	}
	for _, id := range c.IDs() {
		e, st := c.Get(id, "k1")
		if st != protocol.StatusOK || string(e.Value) != "v" {
			t.Fatalf("node %s: st=%d val=%q", id, uint8(st), e.Value)
		}
	}
}

// A quorum read of a missing key is NotFound, not NotReady — parity
// with the replicated handleGet path.
func TestSimQuorumReadMissing(t *testing.T) {
	c := quorumCluster(t, 10)
	for _, id := range c.IDs() {
		if _, st := c.Get(id, "absent"); st != protocol.StatusNotFound {
			t.Fatalf("node %s: status %d, want NotFound", id, uint8(st))
		}
	}
}

// Counter merges fan out best-effort; a missed merge converges through
// counter anti-entropy. PN-counter merge is per-actor max, so both nodes
// agree on the total.
func TestSimCounterConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 11})
	owner := ownerOf(t, c, "cc")
	peer := peerOf(t, c, owner)
	if st := c.CounterAdjust(owner, "cc", 5, false); st != protocol.StatusOK {
		t.Fatalf("adjust: %d", uint8(st))
	}
	oe, ost := c.CounterGet(owner, "cc")
	pe, pst := c.CounterGet(peer, "cc")
	if ost != protocol.StatusOK || pst != protocol.StatusOK || !slices.Equal(oe, pe) {
		t.Fatalf("merge diverged: %d %d equal=%v", uint8(ost), uint8(pst), slices.Equal(oe, pe))
	}
	// Missed merge while suspended: no hints carry counter state.
	c.Suspend(peer)
	if st := c.CounterAdjust(owner, "cc", 3, false); st != protocol.StatusOK {
		t.Fatalf("adjust while suspended: %d", uint8(st))
	}
	// Decrement exercises the signed path; PN-counter merge keeps the
	// per-actor max, so the total stays exact.
	if st := c.CounterAdjust(owner, "cc", 2, true); st != protocol.StatusOK {
		t.Fatalf("decrement: %d", uint8(st))
	}
	c.Resume(peer)
	c.RunCounterRound(owner)
	oe2, ost2 := c.CounterGet(owner, "cc")
	pe2, pst2 := c.CounterGet(peer, "cc")
	if ost2 != protocol.StatusOK || pst2 != protocol.StatusOK || !slices.Equal(oe2, pe2) {
		t.Fatalf("after counter AE: %d %d equal=%v", uint8(ost2), uint8(pst2), slices.Equal(oe2, pe2))
	}
	oc, err := counter.DecodePNCounter(oe2)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if oc.Value() != 6 {
		t.Fatalf("counter value = %d, want 6", oc.Value())
	}
}

// Async write with one replica down queues a hint; replay after recovery
// converges the replica.
func TestSimHintReplayConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 2})
	owner := ownerOf(t, c, "hk")
	peer := peerOf(t, c, owner)
	c.Suspend(peer)
	if r := c.Write(owner, "hk", []byte("hv"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("async write with peer down: %+v", r)
	}
	c.WaitForHint(owner)
	if n := c.ReplayHints(owner); n != 0 {
		t.Fatalf("replay while peer down delivered %d", n)
	}
	c.Resume(peer)
	if n := c.ReplayHints(owner); n == 0 {
		t.Fatal("replay after recovery delivered nothing")
	}
	for _, id := range c.IDs() {
		e, st := c.Get(id, "hk")
		if st != protocol.StatusOK || string(e.Value) != "hv" {
			t.Fatalf("node %s: st=%d val=%q", id, uint8(st), e.Value)
		}
	}
}

// Partitioned quorum writes fail fast; reads stay available on the
// best-effort path, serving the pre-partition copy. After healing the
// retried write converges everywhere.
func TestSimPartitionHealConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Quorum, Seed: 3})
	owner := ownerOf(t, c, "pk")
	peer := peerOf(t, c, owner)
	if r := c.Write(owner, "pk", []byte("pv0"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("pre-partition write: %+v", r)
	}
	c.Partition([]string{owner}, []string{peer})
	if e, st := c.Get(owner, "pk"); st != protocol.StatusOK || string(e.Value) != "pv0" {
		t.Fatalf("quorum read across partition: st=%d val=%q, want pv0 available", uint8(st), e.Value)
	}
	if r := c.Write(owner, "pk", []byte("pv"), 0); r.Status == protocol.StatusOK {
		t.Fatal("quorum write across partition unexpectedly succeeded")
	}
	c.HealAll()
	if r := c.Write(owner, "pk", []byte("pv"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("write after heal: %+v", r)
	}
	for _, id := range c.IDs() {
		e, st := c.Get(id, "pk")
		if st != protocol.StatusOK || string(e.Value) != "pv" {
			t.Fatalf("node %s: st=%d val=%q", id, uint8(st), e.Value)
		}
	}
}

// Divergent async writes on both sides of a partition converge after
// healing via anti-entropy: each side convicts the other (asymmetric
// views, both self-own), accepts its own write, then gossip reconverges
// and LWW picks one winner on both nodes.
func TestSimAntiEntropyConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 4})
	owner := ownerOf(t, c, "ak")
	peer := peerOf(t, c, owner)
	c.Partition([]string{owner}, []string{peer})
	c.MarkDeadOn(owner, peer)
	c.MarkDeadOn(peer, owner)
	if r := c.Write(owner, "ak", []byte("v1"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("write v1 on %s: %+v", owner, r)
	}
	if r := c.Write(peer, "ak", []byte("v2"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("write v2 on %s: %+v", peer, r)
	}
	// Prove the split brain is real before healing: each side serves its
	// own value locally (quorum reads can't cross the partition).
	va, oka := c.Node(owner).Store.Get("ak")
	vb, okb := c.Node(peer).Store.Get("ak")
	if !oka || !okb || string(va) != "v1" || string(vb) != "v2" {
		t.Fatalf("no divergence: %q/%v vs %q/%v", va, oka, vb, okb)
	}
	c.HealAll()
	c.MarkAlive(owner, 1)
	c.MarkAlive(peer, 1)
	// NOTE: incarnations must differ per node for MarkAlive to take
	// effect where the table already records a newer stamp; both were
	// freshly convicted so 1 advances both.
	c.RunAE(owner)
	c.RunAE(peer)
	ea, sa := c.Get(owner, "ak")
	eb, sb := c.Get(peer, "ak")
	if sa != protocol.StatusOK || sb != protocol.StatusOK {
		t.Fatalf("statuses: %d %d", uint8(sa), uint8(sb))
	}
	if string(ea.Value) != string(eb.Value) {
		t.Fatalf("diverged after AE: %q vs %q", ea.Value, eb.Value)
	}
	if got := string(ea.Value); got != "v1" && got != "v2" {
		t.Fatalf("winner %q is neither write", got)
	}
}

// A revived node catches up through rebalance + AE and serves every key.
func TestSimRejoinConverges(t *testing.T) {
	c := quorumCluster(t, 5)
	c.MarkDead("n3")
	keys := []string{"r1", "r2", "r3", "r4", "r5", "r6"}
	owners := map[string]string{}
	for _, k := range keys {
		o := ownerOf(t, c, k)
		owners[k] = o
		if r := c.Write(o, k, []byte("val-"+k), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write %s: %+v", k, r)
		}
	}
	c.MarkAlive("n3", 1)
	c.Rebalance()
	for _, id := range c.IDs() {
		c.RunAE(id)
	}
	for _, id := range c.IDs() {
		c.ReplayHints(id)
	}
	for _, k := range keys {
		var want string
		for _, id := range c.IDs() {
			e, st := c.Get(id, k)
			if st != protocol.StatusOK {
				t.Fatalf("key %s node %s: status %d", k, id, uint8(st))
			}
			if want == "" {
				want = string(e.Value)
			} else if string(e.Value) != want {
				t.Fatalf("key %s diverged: %q vs %q", k, want, e.Value)
			}
		}
		if want != "val-"+k {
			t.Fatalf("key %s: got %q", k, want)
		}
	}
}

// Delivery loss under async replication surfaces as send errors, which
// queue hints; replay after the fault clears converges the replica. The
// seed is part of the fixture: loss draws come from the seeded RNG, so the
// drop pattern (and the Dropped accounting) is reproducible.
func TestSimLossConverges(t *testing.T) {
	// Seed 29 drops the first delivery draw (verified offline); the seed
	// is part of the fixture, so the drop pattern is reproducible.
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 29})
	owner := ownerOf(t, c, "lk")
	peer := peerOf(t, c, owner)
	c.Transport(owner).SetLoss(0.3)
	if r := c.Write(owner, "lk", []byte("lv"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("async write under loss: %+v", r)
	}
	// Rendezvous with the background fan-out, then clear the fault: the
	// replay below must carry the peer, not quorum reads papering over it.
	c.WaitForHint(owner)
	c.Transport(owner).SetLoss(0)
	if n := c.ReplayHints(owner); n == 0 {
		t.Fatal("hint queued but replay delivered nothing")
	}
	// The seed must keep exercising the drop path; fail loudly on rot.
	if got := c.Transport(peer).Dropped(); got == 0 {
		t.Fatal("loss enabled but nothing dropped; seed no longer exercises the path")
	}
	for _, id := range c.IDs() {
		e, st := c.Get(id, "lk")
		if st != protocol.StatusOK || string(e.Value) != "lv" {
			t.Fatalf("node %s: st=%d val=%q", id, uint8(st), e.Value)
		}
	}
}

// Reorder swaps adjacent due deliveries: the multiset is unchanged, so
// LWW still converges. Suspending one peer queues one hint per key (each
// key is written through its own ring owner, live or suspended — the
// suspended owner's sends drop mid-flight); the replay then packs the due
// list, exercising the swap on more than one pair.
func TestSimReorderConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 22})
	peer := peerOf(t, c, ownerOf(t, c, "rk"))
	c.Suspend(peer)
	keys := []string{"r1", "r2", "r3", "r4"}
	for _, k := range keys {
		o := ownerOf(t, c, k)
		if r := c.Write(o, k, []byte("v"+k), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write %s via %s: %+v", k, o, r)
		}
	}
	// Rendezvous: four writes queue four hints across the two nodes. Pump
	// between checks so queued sends complete to hints (a parked send
	// with no pump would never enqueue); yield so starting goroutines
	// can queue.
	hints := func() int {
		return c.Node("n1").Repl.HintCount() + c.Node("n2").Repl.HintCount()
	}
	queued := false
	for range 1 << 20 {
		if hints() >= len(keys) {
			queued = true
			break
		}
		c.Pump()
		runtime.Gosched()
	}
	if !queued {
		t.Fatalf("only %d/%d hints queued", hints(), len(keys))
	}
	c.Resume(peer)
	c.Transport("n1").SetReorder(true)
	c.ReplayHints("n1")
	c.ReplayHints("n2")
	c.Transport("n1").SetReorder(false)
	for _, k := range keys {
		for _, id := range c.IDs() {
			e, st := c.Get(id, k)
			if st != protocol.StatusOK || string(e.Value) != "v"+k {
				t.Fatalf("key %s node %s: st=%d val=%q", k, id, uint8(st), e.Value)
			}
		}
	}
}

// Jitter spreads deliver_at without dropping anything: the virtual clock
// advances past the send instant, and everything still converges.
func TestSimJitterConverges(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Quorum, Seed: 23})
	owner := ownerOf(t, c, "jk")
	c.Transport(owner).SetJitter(5 * time.Millisecond)
	if r := c.Write(owner, "jk", []byte("jv"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("write with jitter: %+v", r)
	}
	advanced := false
	for _, ev := range c.Events() {
		if ev.T > 0 {
			advanced = true
			break
		}
	}
	if !advanced {
		t.Fatal("jitter applied but virtual clock never advanced")
	}
	for _, id := range c.IDs() {
		e, st := c.Get(id, "jk")
		if st != protocol.StatusOK || string(e.Value) != "jv" {
			t.Fatalf("node %s: st=%d val=%q", id, uint8(st), e.Value)
		}
	}
}

// Writes to a non-owner are redirected with the owner named.
func TestSimRedirect(t *testing.T) {
	c := quorumCluster(t, 6)
	owner := ownerOf(t, c, "wk")
	for _, id := range c.IDs() {
		if id == owner {
			continue
		}
		r := c.Write(id, "wk", []byte("v"), 0)
		if r.Status != protocol.StatusNotReady {
			t.Fatalf("node %s: expected redirect, got %+v", id, r)
		}
		if r.Redirect != "moved to "+owner {
			t.Fatalf("redirect = %q, want owner %q", r.Redirect, owner)
		}
		return
	}
}

// TTL rides the write and expires on both sides under the virtual clock.
func TestSimTTLExpires(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Quorum, Seed: 8})
	owner := ownerOf(t, c, "tk")
	if r := c.Write(owner, "tk", []byte("tv"), 100*time.Millisecond); r.Status != protocol.StatusOK {
		t.Fatalf("write: %+v", r)
	}
	if _, st := c.Get(owner, "tk"); st != protocol.StatusOK {
		t.Fatalf("pre-expiry get: %d", uint8(st))
	}
	c.Advance(200 * time.Millisecond)
	for _, id := range c.IDs() {
		if _, st := c.Get(id, "tk"); st == protocol.StatusOK {
			t.Fatalf("node %s: expected expiry, key still present", id)
		}
	}
}
