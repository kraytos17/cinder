package sim_test

// Durable crash-recovery: nodes run WAL + snapshots + hint journals on
// disk (t.TempDir), crash via RestartNode (in-memory state dropped,
// recovered from disk), rejoin, and converge. Fault injection stays off;
// these tests prove the durability path, not the network.

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/sim"
)

// persistedCluster builds a quorum cluster with persistence enabled on
// every node under per-node temp dirs, returning the cluster and dirs.
func persistedCluster(t testing.TB, ids []string, opt sim.Options) (*sim.Cluster, map[string]string) {
	t.Helper()
	c := sim.NewCluster(t, ids, opt)
	dirs := make(map[string]string, len(ids))
	for _, id := range ids {
		dir := t.TempDir()
		c.EnablePersistence(id, dir)
		dirs[id] = dir
	}
	return c, dirs
}

// convergeAll runs rebalance to completion, then one anti-entropy and
// hint-replay round per node, and asserts every key holds the same value
// on every node.
func convergeAll(t testing.TB, c *sim.Cluster, keys []string, want func(string) string) {
	t.Helper()
	c.Rebalance()
	for _, id := range c.IDs() {
		c.RunAE(id)
	}
	for _, id := range c.IDs() {
		c.ReplayHints(id)
	}
	for _, k := range keys {
		var wantVal string
		for _, id := range c.IDs() {
			e, st := c.Get(id, k)
			if st != protocol.StatusOK {
				t.Fatalf("key %s node %s: status %d", k, id, uint8(st))
			}
			if wantVal == "" {
				wantVal = string(e.Value)
			} else if string(e.Value) != wantVal {
				t.Fatalf("key %s diverged: %q vs %q", k, wantVal, e.Value)
			}
		}
		if wantVal != want(k) {
			t.Fatalf("key %s: got %q, want %q", k, wantVal, want(k))
		}
	}
}

func writeKeys(t testing.TB, c *sim.Cluster, keys []string, val func(string) string) {
	t.Helper()
	for _, k := range keys {
		o := ownerOf(t, c, k)
		if r := c.Write(o, k, []byte(val(k)), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write %s: %+v", k, r)
		}
	}
}

// Snapshot recovery: the victim compacts before crashing, so recovery
// replays snapshot + WAL tail. Post-rejoin traffic proves the recovered
// node serves live writes too.
func TestSimCrashSnapshotRecovery(t *testing.T) {
	c, dirs := persistedCluster(t, []string{"n1", "n2", "n3"}, sim.Options{RF: 3, Mode: repl.Quorum, Seed: 41})
	keys := make([]string, 0, 20)
	for i := range 20 {
		keys = append(keys, fmt.Sprintf("s%d", i))
	}
	writeKeys(t, c, keys, func(k string) string { return "val-" + k })
	o := ownerOf(t, c, "sc")
	if st := c.CounterAdjust(o, "sc", 7, false); st != protocol.StatusOK {
		t.Fatalf("counter adjust: %d", uint8(st))
	}
	if err := c.Node("n3").Persist.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// Pin the snapshot path: recovery must load a snapshot, not just the
	// WAL (pure WAL replay is covered by TestSimCrashWALTailRecovery).
	snaps, err := filepath.Glob(filepath.Join(dirs["n3"], "snapshot_*.dat"))
	if err != nil || len(snaps) == 0 {
		t.Fatalf("no snapshot in n3 dir after compact: %v %v", snaps, err)
	}
	c.RestartNode("n3")
	// No writes happened between Compact and the crash, so the fresh
	// active WAL is empty: local presence before any repair proves the
	// snapshot itself loaded.
	for _, k := range keys {
		if _, ok := c.Node("n3").Store.GetVersioned(k); !ok {
			t.Fatalf("key %s missing right after restart: snapshot load failed", k)
		}
	}
	// Fresh keys land after the crash; the recovered node must serve them.
	extra := []string{"x1", "x2", "x3", "x4", "x5"}
	writeKeys(t, c, extra, func(k string) string { return "val-" + k })
	convergeAll(t, c, append(keys, extra...), func(k string) string { return "val-" + k })
	oe, ost := c.CounterGet("n3", "sc")
	if ost != protocol.StatusOK {
		t.Fatalf("counter after restart: %d", uint8(ost))
	}
	oc, err := counter.DecodePNCounter(oe)
	if err != nil || oc.Value() != 7 {
		t.Fatalf("counter = %v, err %v; want 7", oc, err)
	}
}

// WAL-tail recovery without any compact: every key must be present locally
// immediately after restart, before any anti-entropy runs — proving WAL
// replay, not repair.
func TestSimCrashWALTailRecovery(t *testing.T) {
	c, _ := persistedCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Quorum, Seed: 42})
	keys := []string{"w1", "w2", "w3"}
	writeKeys(t, c, keys, func(k string) string { return "val-" + k })
	c.RestartNode("n2")
	for _, k := range keys {
		if _, ok := c.Node("n2").Store.GetVersioned(k); !ok {
			t.Fatalf("key %s missing right after restart: WAL replay failed", k)
		}
	}
	convergeAll(t, c, keys, func(k string) string { return "val-" + k })
}

// Hint-journal survival: the owner's queued hint is journaled, survives
// the owner's crash, and redelivers after both recover. The peer assertion
// reads the local store directly so quorum read-repair cannot paper over a
// missed replay.
func TestSimHintJournalSurvivesCrash(t *testing.T) {
	c, _ := persistedCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 43})
	owner := ownerOf(t, c, "jc")
	var peer string
	for _, id := range c.IDs() {
		if id != owner {
			peer = id
		}
	}
	c.Suspend(peer)
	if r := c.Write(owner, "jc", []byte("jv"), 0); r.Status != protocol.StatusOK {
		t.Fatalf("async write with peer suspended: %+v", r)
	}
	c.WaitForHint(owner)
	// Crash the owner with the hint still queued; nothing is compacted.
	c.RestartNode(owner)
	if got := c.Node(owner).Repl.HintCount(); got == 0 {
		t.Fatal("hint journal did not survive the crash")
	}
	c.Resume(peer)
	if n := c.ReplayHints(owner); n == 0 {
		t.Fatal("replay after recovery delivered nothing")
	}
	if _, ok := c.Node(peer).Store.GetVersioned("jc"); !ok {
		t.Fatal("peer missing replayed key in local store")
	}
	e, st := c.Get(peer, "jc")
	if st != protocol.StatusOK || string(e.Value) != "jv" {
		t.Fatalf("peer: st=%d val=%q", uint8(st), e.Value)
	}
}
