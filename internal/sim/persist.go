package sim

// Durable crash-recovery: WAL + snapshots + hint journal per node,
// mirroring Server.ConfigurePersistence. The crash model is a full process
// restart — fresh in-memory state, same disk dir, same network identity —
// with FsyncAlways so every acknowledged write is durable at the crash
// instant. Close-before-wipe is then semantically a no-op (Close always
// syncs; FsyncAlways already synced everything) and only releases file
// descriptors held by the test process.

import (
	"path/filepath"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/counter"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/store"
)

// hintsFile is the journal name inside a node's disk dir.
const hintsFile = "hints.log"

// EnablePersistence turns on WAL + snapshots + counter persistence and the
// durable hint journal for id under dir, mirroring ConfigurePersistence:
// recover first (replay never logs itself), then wire the store hooks.
// FsyncAlways models crash-safety; auto-compact time triggers are
// disabled so tests compact manually for determinism.
func (c *Cluster) EnablePersistence(id, dir string) {
	c.t.Helper()
	n := c.Node(id)
	pm := persist.NewManager(dir, n.Store, c.clk.Now)
	pm.SetCounterStore(n.Counters)
	if err := pm.Recover(); err != nil {
		c.t.Fatalf("sim: recover %s: %v", id, err)
	}

	n.Store.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
		return pm.OnWrite(key, e, hardDelete)
	})
	n.Counters.SetPersistence(func(key string, state counter.PNCounter, remove bool) error {
		return pm.OnCounterWrite(key, state, remove)
	})
	if err := pm.SetFsyncPolicy(persist.FsyncAlways, 0); err != nil {
		c.t.Fatalf("sim: fsync policy %s: %v", id, err)
	}

	pm.SetSnapshotInterval(0)
	n.Persist = pm
	c.dirs[id] = dir
	c.openHints(id, dir)
}

// openHints (re)opens the durable hint journal against the node's live
// queue and wires it into the replication manager, replaying any hints
// that survived the crash.
func (c *Cluster) openHints(id, dir string) {
	c.t.Helper()
	n := c.Node(id)
	j, _, err := ae.OpenJournal(filepath.Join(dir, hintsFile), n.Repl.HintQueue())
	if err != nil {
		c.t.Fatalf("sim: open hints %s: %v", id, err)
	}
	n.Repl.SetHintJournal(j)
}

// RestartNode simulates a process crash and restart: all in-memory state
// is dropped, persistence is recovered from the node's disk dir, managers
// are rewired around the recovered stores, and the node rejoins with a
// fresh incarnation. Network identity (transport endpoint), ring, and
// membership table are preserved, exactly as a restarted process keeps its
// address. Requires EnablePersistence.
func (c *Cluster) RestartNode(id string) {
	c.t.Helper()
	n := c.Node(id)
	dir, ok := c.dirs[id]
	if !ok {
		c.t.Fatalf("sim: no persistence enabled for %s", id)
	}

	c.MarkDead(id)
	if n.Persist != nil {
		if err := n.Persist.Close(); err != nil {
			c.t.Fatalf("sim: close persist %s: %v", id, err)
		}
	}
	if err := n.Repl.CloseHintJournal(); err != nil {
		c.t.Fatalf("sim: close hints %s: %v", id, err)
	}

	st := store.NewLRUStore(simStoreBytes, c.clk)
	counters := counter.NewStore(simCounterBytes)
	pm := persist.NewManager(dir, st, c.clk.Now)
	pm.SetCounterStore(counters)
	if err := pm.Recover(); err != nil {
		c.t.Fatalf("sim: recover %s: %v", id, err)
	}

	st.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
		return pm.OnWrite(key, e, hardDelete)
	})
	counters.SetPersistence(func(key string, state counter.PNCounter, remove bool) error {
		return pm.OnCounterWrite(key, state, remove)
	})
	if err := pm.SetFsyncPolicy(persist.FsyncAlways, 0); err != nil {
		c.t.Fatalf("sim: fsync policy %s: %v", id, err)
	}

	pm.SetSnapshotInterval(0)
	c.nodes[id] = c.wireNode(id, st, counters, n.Transport, n.Ring, n.Table)
	c.nodes[id].Persist = pm
	c.openHints(id, dir)
	c.incar[id]++
	if c.incar[id] == 0 {
		c.incar[id] = 1
	}
	c.MarkAlive(id, c.incar[id])
}
