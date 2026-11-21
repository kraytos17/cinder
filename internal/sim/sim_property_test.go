package sim_test

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/sim"
)

// TestSimSeededMixedOperationsConverge exercises a deterministic family of
// clusters rather than one hand-picked schedule. It mixes LWW sets/deletes
// and PN-counter increments/decrements under seeded message loss. After
// loss stops, hinted handoff and repeated AE rounds must make every replica
// agree locally; the assertions deliberately avoid quorum reads, which
// could repair divergence and hide a missing background repair path.
func TestSimSeededMixedOperationsConverge(t *testing.T) {
	const (
		seedCount         = 50
		operationsPerSeed = 24
		keyCount          = 8
	)
	ids := []string{"n1", "n2", "n3"}

	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0xD1B54A32D192ED03))
			c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Async, Seed: seed})
			c.Transport(ids[0]).SetLoss(0.2)
			keys := make([]string, keyCount)
			counterKeys := make([]string, keyCount)
			for i := range keyCount {
				keys[i] = fmt.Sprintf("k%d", i)
				counterKeys[i] = fmt.Sprintf("c%d", i)
			}
			partitionAt := rng.IntN(operationsPerSeed)

			for step := range operationsPerSeed {
				if step == partitionAt {
					c.Partition([]string{ids[0]}, ids[1:])
				}
				key := keys[rng.IntN(keyCount)]
				owner := c.Node(ids[0]).Ring.GetNode(key)
				switch rng.IntN(3) {
				case 0, 1:
					value := fmt.Appendf(nil, "seed-%d-step-%d", seed, step)
					if r := c.Write(owner, key, value, 0); r.Status != protocol.StatusOK {
						t.Fatalf("write seed=%d step=%d key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
				case 2:
					if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
						t.Fatalf("delete seed=%d step=%d key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
				}

				counterKey := counterKeys[rng.IntN(keyCount)]
				counterOwner := c.Node(ids[0]).Ring.GetNode(counterKey)
				delta, decrement := uint64(rng.IntN(5)+1), rng.IntN(2) == 0
				if st := c.CounterAdjust(counterOwner, counterKey, delta, decrement); st != protocol.StatusOK {
					t.Fatalf("counter seed=%d step=%d key=%s: status=%d", seed, step, counterKey, uint8(st))
				}
			}

			// Heal the seeded partition and end the loss window before retrying. Replay every node's
			// hints; rounds repeat because each AE manager round-robins one
			// partner and repairs are pairwise.
			c.HealAll()
			c.Transport(ids[0]).SetLoss(0)
			for _, id := range ids {
				c.ReplayHints(id)
			}
			for range 8 {
				for _, id := range ids {
					c.RunAE(id)
					c.RunCounterRound(id)
				}
				for _, id := range ids {
					c.ReplayHints(id)
				}
			}

			for _, key := range keys {
				first, present := c.Node(ids[0]).Store.GetVersioned(key)
				for _, id := range ids[1:] {
					got, ok := c.Node(id).Store.GetVersioned(key)
					if ok != present {
						t.Fatalf("seed=%d key=%s presence differs: %s=%v %s=%v", seed, key, ids[0], present, id, ok)
					}
					if present && (got.Stamp() != first.Stamp() || !bytes.Equal(got.Value, first.Value)) {
						t.Fatalf("seed=%d key=%s diverged: %s=%+v %s=%+v", seed, key, ids[0], first, id, got)
					}
				}
			}

			for _, key := range counterKeys {
				first, present := c.Node(ids[0]).Counters.Get(key)
				for _, id := range ids[1:] {
					got, ok := c.Node(id).Counters.Get(key)
					if ok != present || (present && !got.Equal(first)) {
						t.Fatalf("seed=%d counter=%s diverged: %s present=%v value=%d; %s present=%v value=%d", seed, key, ids[0], present, first.Value(), id, ok, got.Value())
					}
				}
			}
		})
	}
}

// A bounded hint queue never exceeds its configured live-byte budget even
// when every replica send fails. Oversized hints may be dropped, which must
// be reflected by the drop counter rather than hidden from operators.
func TestSimHintQueueStaysWithinBudget(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 51})
	const key = "bounded-hint-key"
	owner := c.Node("n1").Ring.GetNode(key)
	peer := peerOf(t, c, owner)
	c.Suspend(peer)
	c.Node(owner).Repl.SetMaxHintBytes(1)

	for i := range 8 {
		if r := c.Write(owner, key, fmt.Appendf(nil, "payload-%d-that-does-not-fit", i), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write %s: %+v", key, r)
		}
	}
	c.Pump()
	if got := c.Node(owner).Repl.HintBytes(); got > 1 {
		t.Fatalf("hint bytes = %d, exceeds configured budget 1", got)
	}
	if got := c.Node(owner).Repl.HintsDropped(); got == 0 {
		t.Fatal("all sends failed and the tiny queue budget was not reflected in dropped-hint accounting")
	}
}

// A successful RF=3 quorum write with one target unreachable must exist on
// the owner and another replica (W=2). Once the peer returns, AE must carry
// the acknowledged winner to the missing copy. Direct-store checks make the
// W/R property observable without best-effort reads repairing it for us.
func TestSimQuorumAckSurvivesOneReplicaOutage(t *testing.T) {
	const seedCount = 20
	ids := []string{"n1", "n2", "n3"}
	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
			const key = "quorum-ack-key"
			owner := c.Node(ids[0]).Ring.GetNode(key)
			replicas, err := c.Node(owner).Ring.GetNodes(key, 3)
			if err != nil || len(replicas) != 3 {
				t.Fatalf("replicas: %v, err=%v", replicas, err)
			}
			failed := ""
			for _, id := range replicas {
				if id != owner {
					failed = id
					break
				}
			}
			if failed == "" {
				t.Fatal("replica set did not include a non-owner")
			}
			c.Suspend(failed)
			const value = "acknowledged"
			if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
				t.Fatalf("quorum write with one replica down: %+v", r)
			}

			available := 0
			for _, id := range replicas {
				if id == failed {
					continue
				}
				e, ok := c.Node(id).Store.GetVersioned(key)
				if ok && string(e.Value) == value {
					available++
				}
			}
			if available < 2 {
				t.Fatalf("successful write had %d durable replica copies, want W=2", available)
			}

			c.Resume(failed)
			for range 4 {
				for _, id := range ids {
					c.RunAE(id)
				}
			}
			for _, id := range replicas {
				e, ok := c.Node(id).Store.GetVersioned(key)
				if !ok || string(e.Value) != value {
					t.Fatalf("acked value missing after repair on %s: %+v, present=%v", id, e, ok)
				}
			}
		})
	}
}

// Randomized membership churn must not erase the latest quorum-acknowledged
// value. Each transition removes or rejoins one node, settles shard
// migrations, then checks the current replica set directly (without a read
// path that could repair a missing copy). The seed and operation index make
// any failing schedule reproducible.
func TestSimRebalanceChurnPreservesAckedValues(t *testing.T) {
	const (
		seedCount = 50
		steps     = 32
		keyCount  = 12
	)
	ids := []string{"n1", "n2", "n3", "n4"}

	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15))
			c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
			keys := make([]string, keyCount)
			latest := make(map[string]string, keyCount)
			for i := range keyCount {
				key := fmt.Sprintf("rebalance-%d", i)
				keys[i] = key
				value := fmt.Sprintf("seed-%d-initial-%d", seed, i)
				owner := c.Node(ids[0]).Ring.GetNode(key)
				if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d initial write key=%s owner=%s: %+v", seed, key, owner, r)
				}
				latest[key] = value
			}

			var offline string
			var leaves, rejoins int
			incarnations := make(map[string]uint64, len(ids))
			activeRing := func() *sim.Node {
				for _, id := range ids {
					if id != offline {
						return c.Node(id)
					}
				}
				t.Fatal("all nodes are offline")
				return nil
			}
			checkReplicas := func(step int, phase string) {
				t.Helper()
				ring := activeRing().Ring
				for _, key := range keys {
					replicas, err := ring.GetNodes(key, 3)
					if err != nil || len(replicas) != 3 {
						t.Fatalf("seed=%d step=%d %s key=%s replicas=%v err=%v", seed, step, phase, key, replicas, err)
					}
					for _, id := range replicas {
						entry, ok := c.Node(id).Store.GetVersioned(key)
						if !ok || string(entry.Value) != latest[key] {
							t.Fatalf("seed=%d step=%d %s lost acked key=%s value=%q on replica=%s; present=%v got=%q", seed, step, phase, key, latest[key], id, ok, entry.Value)
						}
					}
				}
			}

			for step := range steps {
				// A single node is out at a time, leaving exactly three live
				// members when churned. This keeps RF=3 quorum writes possible.
				action := rng.IntN(4)
				if offline == "" && action == 0 {
					key := keys[rng.IntN(len(keys))]
					owner := activeRing().Ring.GetNode(key)
					replicas, err := c.Node(owner).Ring.GetNodes(key, 3)
					if err != nil || len(replicas) != 3 {
						t.Fatalf("seed=%d step=%d replicas for crash-window write: %v err=%v", seed, step, replicas, err)
					}
					targets := make([]string, 0, 2)
					for _, id := range replicas {
						if id != owner {
							targets = append(targets, id)
						}
					}
					offline = targets[rng.IntN(len(targets))]
					c.Suspend(offline)
					value := fmt.Sprintf("seed-%d-crash-window-%d", seed, step)
					if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d quorum write with %s suspended: %+v", seed, step, offline, r)
					}
					latest[key] = value
					c.MarkDead(offline)
					leaves++
					c.Rebalance()
					checkReplicas(step, "after leave/rebalance")
					continue
				}
				if offline != "" && action <= 1 {
					id := offline
					incarnations[id]++
					c.MarkAlive(id, incarnations[id]+1)
					offline = ""
					rejoins++
					c.Rebalance()
					checkReplicas(step, "after rejoin/rebalance")
					continue
				}

				key := keys[rng.IntN(len(keys))]
				owner := activeRing().Ring.GetNode(key)
				value := fmt.Sprintf("seed-%d-step-%d", seed, step)
				if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d step=%d write key=%s owner=%s: %+v", seed, step, key, owner, r)
				}
				latest[key] = value
			}

			if offline != "" {
				incarnations[offline]++
				c.MarkAlive(offline, incarnations[offline]+1)
				rejoins++
			}
			c.Rebalance()
			checkReplicas(steps, "final rebalance")
			if leaves == 0 || rejoins == 0 {
				t.Fatalf("seed=%d did not exercise both leave and rejoin: leaves=%d rejoins=%d", seed, leaves, rejoins)
			}
		})
	}
}

// Quorum deletes must survive the same churn: a tombstone that wins LWW must
// not lose to a stale live copy carried by a rejoining replica. The test
// mixes writes and deletes under leave/rejoin, then checks replica stores
// directly so read repair cannot hide a resurrection. Tombstone GC stays at
// its 24h default and the virtual clock never advances past it, so a missing
// marker is a real loss, not collection.
func TestSimChurnDeletesStayDeleted(t *testing.T) {
	const (
		seedCount = 50
		steps     = 32
		keyCount  = 10
	)
	ids := []string{"n1", "n2", "n3", "n4"}

	type expectation struct {
		value   string
		deleted bool
	}

	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x51AB3F2D4C7E9A01))
			c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
			keys := make([]string, keyCount)
			latest := make(map[string]expectation, keyCount)
			for i := range keyCount {
				key := fmt.Sprintf("churn-del-%d", i)
				keys[i] = key
				value := fmt.Sprintf("seed-%d-initial-%d", seed, i)
				owner := c.Node(ids[0]).Ring.GetNode(key)
				if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d initial write key=%s owner=%s: %+v", seed, key, owner, r)
				}
				latest[key] = expectation{value: value}
			}

			var offline string
			var leaves, rejoins, deleteOps int
			incarnations := make(map[string]uint64, len(ids))
			activeRing := func() *sim.Node {
				for _, id := range ids {
					if id != offline {
						return c.Node(id)
					}
				}
				t.Fatal("all nodes are offline")
				return nil
			}
			checkReplicas := func(step int, phase string) {
				t.Helper()
				ring := activeRing().Ring
				for _, key := range keys {
					exp := latest[key]
					replicas, err := ring.GetNodes(key, 3)
					if err != nil || len(replicas) != 3 {
						t.Fatalf("seed=%d step=%d %s key=%s replicas=%v err=%v", seed, step, phase, key, replicas, err)
					}
					for _, id := range replicas {
						entry, ok := c.Node(id).Store.GetVersioned(key)
						if exp.deleted {
							if !ok {
								t.Fatalf("seed=%d step=%d %s key=%s replica=%s lost tombstone", seed, step, phase, key, id)
							}
							if !entry.Tombstone {
								t.Fatalf("seed=%d step=%d %s resurrected key=%s value=%q on replica=%s", seed, step, phase, key, entry.Value, id)
							}
							if _, live := c.Node(id).Store.Get(key); live {
								t.Fatalf("seed=%d step=%d %s deleted key=%s still live on replica=%s", seed, step, phase, key, id)
							}
						} else if !ok || entry.Tombstone || string(entry.Value) != exp.value {
							t.Fatalf("seed=%d step=%d %s lost acked key=%s value=%q on replica=%s; present=%v tombstone=%v got=%q", seed, step, phase, key, exp.value, id, ok, entry.Tombstone, entry.Value)
						}
					}
				}
			}

			for step := range steps {
				action := rng.IntN(4)
				if offline == "" && action == 0 {
					key := keys[rng.IntN(len(keys))]
					owner := activeRing().Ring.GetNode(key)
					replicas, err := c.Node(owner).Ring.GetNodes(key, 3)
					if err != nil || len(replicas) != 3 {
						t.Fatalf("seed=%d step=%d replicas for crash-window op: %v err=%v", seed, step, replicas, err)
					}
					targets := make([]string, 0, 2)
					for _, id := range replicas {
						if id != owner {
							targets = append(targets, id)
						}
					}
					offline = targets[rng.IntN(len(targets))]
					c.Suspend(offline)
					if rng.IntN(2) == 0 {
						if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
							t.Fatalf("seed=%d step=%d quorum delete with %s suspended: %+v", seed, step, offline, r)
						}
						latest[key] = expectation{deleted: true}
						deleteOps++
					} else {
						value := fmt.Sprintf("seed-%d-crash-window-%d", seed, step)
						if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
							t.Fatalf("seed=%d step=%d quorum write with %s suspended: %+v", seed, step, offline, r)
						}
						latest[key] = expectation{value: value}
					}
					c.MarkDead(offline)
					leaves++
					c.Rebalance()
					checkReplicas(step, "after leave/rebalance")
					continue
				}
				if offline != "" && action <= 1 {
					id := offline
					incarnations[id]++
					c.MarkAlive(id, incarnations[id]+1)
					offline = ""
					rejoins++
					c.Rebalance()
					for _, nid := range ids {
						c.ReplayHints(nid)
					}
					for range 4 {
						for _, nid := range ids {
							c.RunAE(nid)
						}
					}
					checkReplicas(step, "after rejoin/rebalance")
					continue
				}

				key := keys[rng.IntN(len(keys))]
				owner := activeRing().Ring.GetNode(key)
				if rng.IntN(3) == 2 {
					if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d delete key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{deleted: true}
					deleteOps++
				} else {
					value := fmt.Sprintf("seed-%d-step-%d", seed, step)
					if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d write key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{value: value}
				}
			}

			if offline != "" {
				incarnations[offline]++
				c.MarkAlive(offline, incarnations[offline]+1)
				rejoins++
			}
			c.Rebalance()
			for _, nid := range ids {
				c.ReplayHints(nid)
			}
			for range 4 {
				for _, nid := range ids {
					c.RunAE(nid)
				}
			}
			checkReplicas(steps, "final rebalance")
			if leaves == 0 || rejoins == 0 {
				t.Fatalf("seed=%d did not exercise both leave and rejoin: leaves=%d rejoins=%d", seed, leaves, rejoins)
			}
			if deleteOps == 0 {
				t.Fatalf("seed=%d exercised no deletes", seed)
			}
		})
	}
}

// A brand-new node joining via AddNode must observe every acknowledged value
// (or tombstone) after migration settles. Unlike leave/rejoin, the joiner
// starts empty: Rebalance must push current state to it, and hint replay plus
// AE must close any race between in-flight writes and the ring change. Checks
// read replica stores directly.
func TestSimJoinPreservesAckedValues(t *testing.T) {
	const (
		seedCount = 30
		steps     = 16
		keyCount  = 10
	)
	base := []string{"n1", "n2", "n3"}

	type expectation struct {
		value   string
		deleted bool
	}

	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x6A09E667F3BCC909))
			c := sim.NewCluster(t, base, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
			keys := make([]string, keyCount)
			latest := make(map[string]expectation, keyCount)
			for i := range keyCount {
				key := fmt.Sprintf("join-%d", i)
				keys[i] = key
				value := fmt.Sprintf("seed-%d-initial-%d", seed, i)
				owner := c.Node(c.IDs()[0]).Ring.GetNode(key)
				if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d initial write key=%s owner=%s: %+v", seed, key, owner, r)
				}
				latest[key] = expectation{value: value}
			}

			checkReplicas := func(step int, phase string) {
				t.Helper()
				ring := c.Node(c.IDs()[0]).Ring
				for _, key := range keys {
					exp := latest[key]
					replicas, err := ring.GetNodes(key, 3)
					if err != nil || len(replicas) != 3 {
						t.Fatalf("seed=%d step=%d %s key=%s replicas=%v err=%v", seed, step, phase, key, replicas, err)
					}
					for _, id := range replicas {
						entry, ok := c.Node(id).Store.GetVersioned(key)
						if exp.deleted {
							if !ok || !entry.Tombstone {
								t.Fatalf("seed=%d step=%d %s key=%s replica=%s lost tombstone; present=%v", seed, step, phase, key, id, ok)
							}
						} else if !ok || entry.Tombstone || string(entry.Value) != exp.value {
							t.Fatalf("seed=%d step=%d %s lost acked key=%s value=%q on replica=%s", seed, step, phase, key, exp.value, id)
						}
					}
				}
			}

			settle := func() {
				t.Helper()
				c.Rebalance()
				for _, nid := range c.IDs() {
					c.ReplayHints(nid)
				}
				for range 4 {
					for _, nid := range c.IDs() {
						c.RunAE(nid)
					}
				}
			}

			c.AddNode("n4")
			settle()
			checkReplicas(0, "after n4 join")

			for step := range steps {
				key := keys[rng.IntN(len(keys))]
				owner := c.Node(c.IDs()[0]).Ring.GetNode(key)
				if step == 0 || rng.IntN(3) == 2 {
					if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d delete key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{deleted: true}
				} else {
					value := fmt.Sprintf("seed-%d-step-%d", seed, step)
					if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d write key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{value: value}
				}
			}

			c.AddNode("n5")
			settle()
			checkReplicas(steps, "after n5 join")
		})
	}
}

// Clock jumps must not resurrect expired keys through any path: hints
// queued before the jump, anti-entropy afterwards, or read repair. Each seed
// suspends one replica, writes TTL keys against the outage, jumps past every
// deadline, then heals with hint replay and AE; a randomized phase mixes
// live writes, TTL writes, deletes, and further jumps. Checks read replica
// stores directly (plus NotFound on the coordinator path) so repair cannot
// hide a resurrection. No delay/jitter/loss is enabled, so pump waves never
// advance the clock behind the test's back; elapsed tracks every Advance.
func TestSimTTLClockJumpNeverResurrects(t *testing.T) {
	const (
		seedCount = 30
		steps     = 16
		keyCount  = 8
	)
	ids := []string{"n1", "n2", "n3"}

	type expectation struct {
		value    string
		deadline time.Duration
		hasTTL   bool
		deleted  bool
	}

	for seed := range uint64(seedCount) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0x165667B19E3779F9))
			c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Quorum, Seed: seed})
			var elapsed time.Duration
			advance := func(d time.Duration) {
				c.Advance(d)
				elapsed += d
			}
			keys := make([]string, keyCount)
			latest := make(map[string]expectation, keyCount)
			for i := range keyCount {
				key := fmt.Sprintf("ttl-jump-%d", i)
				keys[i] = key
				value := fmt.Sprintf("seed-%d-initial-%d", seed, i)
				owner := c.Node(ids[0]).Ring.GetNode(key)
				if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d initial write key=%s owner=%s: %+v", seed, key, owner, r)
				}
				latest[key] = expectation{value: value}
			}

			checkAll := func(step int, phase string) {
				t.Helper()
				for _, key := range keys {
					exp := latest[key]
					expired := exp.hasTTL && !exp.deleted && elapsed >= exp.deadline
					for _, id := range c.IDs() {
						entry, ok := c.Node(id).Store.GetVersioned(key)
						if exp.deleted {
							if !ok || !entry.Tombstone {
								t.Fatalf("seed=%d step=%d %s key=%s replica=%s lost tombstone; present=%v", seed, step, phase, key, id, ok)
							}
							continue
						}
						if expired {
							if ok {
								t.Fatalf("seed=%d step=%d %s resurrected expired key=%s value=%q on replica=%s", seed, step, phase, key, entry.Value, id)
							}
							continue
						}
						if !ok || entry.Tombstone || string(entry.Value) != exp.value {
							t.Fatalf("seed=%d step=%d %s lost live key=%s value=%q on replica=%s", seed, step, phase, key, exp.value, id)
						}
					}
					if exp.deleted || expired {
						if _, st := c.Get(c.IDs()[0], key); st != protocol.StatusNotFound {
							t.Fatalf("seed=%d step=%d %s key=%s coordinator read status=%d, want NotFound", seed, step, phase, key, uint8(st))
						}
					}
				}
			}

			// Targeted outage: TTL writes while one replica is suspended,
			// then a jump past every deadline before healing.
			suspended := ids[rng.IntN(len(ids))]
			c.Suspend(suspended)
			alive0 := ids[0]
			if alive0 == suspended {
				alive0 = ids[1]
			}
			var ttlKeys []string
			var maxTTL time.Duration
			ownersWritten := map[string]bool{}
			for _, key := range keys {
				if len(ttlKeys) >= 4 {
					break
				}
				owner := c.Node(alive0).Ring.GetNode(key)
				if owner == suspended {
					continue
				}
				ttl := time.Duration(300+rng.IntN(2700)) * time.Millisecond
				if ttl > maxTTL {
					maxTTL = ttl
				}
				value := fmt.Sprintf("seed-%d-ttl-%s", seed, key)
				if r := c.Write(owner, key, []byte(value), ttl); r.Status != protocol.StatusOK {
					t.Fatalf("seed=%d TTL write key=%s owner=%s ttl=%v: %+v", seed, key, owner, ttl, r)
				}
				latest[key] = expectation{value: value, deadline: elapsed + ttl, hasTTL: true}
				ttlKeys = append(ttlKeys, key)
				ownersWritten[owner] = true
			}
			if len(ttlKeys) == 0 {
				t.Fatalf("seed=%d: no key owned outside suspended %s", seed, suspended)
			}
			for owner := range ownersWritten {
				c.WaitForHint(owner)
			}
			advance(maxTTL + 100*time.Millisecond)
			for _, key := range ttlKeys {
				owner := c.Node(alive0).Ring.GetNode(key)
				if _, ok := c.Node(owner).Store.GetVersioned(key); ok {
					t.Fatalf("seed=%d owner retained expired key=%s", seed, key)
				}
			}
			c.Resume(suspended)
			for _, nid := range c.IDs() {
				c.ReplayHints(nid)
			}
			for range 4 {
				for _, nid := range c.IDs() {
					c.RunAE(nid)
				}
			}
			checkAll(0, "after outage jump and heal")

			for step := range steps {
				key := keys[rng.IntN(len(keys))]
				owner := c.Node(c.IDs()[0]).Ring.GetNode(key)
				switch {
				case step == 0:
					if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d delete key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{deleted: true}
				case step == 1 || rng.IntN(10) < 3:
					ttl := time.Duration(300+rng.IntN(2700)) * time.Millisecond
					value := fmt.Sprintf("seed-%d-ttl-step-%d", seed, step)
					if r := c.Write(owner, key, []byte(value), ttl); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d TTL write key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{value: value, deadline: elapsed + ttl, hasTTL: true}
				case rng.IntN(4) == 3:
					if r := c.Delete(owner, key); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d delete key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{deleted: true}
				default:
					value := fmt.Sprintf("seed-%d-step-%d", seed, step)
					if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
						t.Fatalf("seed=%d step=%d write key=%s owner=%s: %+v", seed, step, key, owner, r)
					}
					latest[key] = expectation{value: value}
				}
				if rng.IntN(4) == 0 {
					advance(time.Duration(100+rng.IntN(700)) * time.Millisecond)
				}
			}

			for _, nid := range c.IDs() {
				c.ReplayHints(nid)
			}
			for range 4 {
				for _, nid := range c.IDs() {
					c.RunAE(nid)
				}
			}
			checkAll(steps, "final")
		})
	}
}

// Draining a member migrates everything away: after MarkLeaving plus
// rebalance/hint/AE convergence the drainer holds nothing, every key is
// present on its new replica set, and new writes route away from the
// drainer while reads still serve from it until it empties.
func TestSimDrainEmptiesAndPreserves(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4"}
	c := sim.NewCluster(t, ids, sim.Options{RF: 3, Mode: repl.Quorum, Seed: 9})
	keys := make([]string, 10)
	latest := make(map[string]string, 10)
	for i := range keys {
		key := fmt.Sprintf("drain-%d", i)
		keys[i] = key
		value := fmt.Sprintf("v-%d", i)
		owner := c.Node(ids[0]).Ring.GetNode(key)
		if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
			t.Fatalf("initial write key=%s owner=%s: %+v", key, owner, r)
		}
		latest[key] = value
	}

	var held []string
	for _, key := range keys {
		if _, ok := c.Node("n4").Store.Get(key); ok {
			held = append(held, key)
		}
	}
	if len(held) == 0 {
		t.Fatal("n4 holds nothing to drain")
	}

	c.MarkLeaving("n4")

	// Writes addressed to the drainer redirect; the same keys succeed
	// through their new owners, so the cluster stays available mid-drain.
	for _, key := range keys[:3] {
		if r := c.Write("n4", key, []byte("x"), 0); r.Status != protocol.StatusNotReady {
			t.Fatalf("write via drainer: %+v, want NotReady redirect", r)
		}
		owner := c.Node(ids[0]).Ring.GetNode(key)
		value := "during-drain-" + key
		if r := c.Write(owner, key, []byte(value), 0); r.Status != protocol.StatusOK {
			t.Fatalf("write during drain key=%s owner=%s: %+v", key, owner, r)
		}
		latest[key] = value
	}
	// Reads still serve from the drainer's copy until it migrates away.
	if _, ok := c.Node("n4").Store.Get(held[0]); !ok {
		t.Fatal("drainer stopped serving reads")
	}

	settled := false
	for range 10 {
		c.Rebalance()
		for _, id := range ids {
			c.ReplayHints(id)
		}
		for range 4 {
			for _, id := range ids {
				c.RunAE(id)
			}
		}
		if c.Node("n4").Store.Size() == 0 {
			settled = true
			break
		}
	}
	if !settled {
		t.Fatal("drainer never emptied")
	}
	for _, key := range keys {
		replicas, err := c.Node(ids[0]).Ring.GetNodes(key, 3)
		if err != nil || len(replicas) != 3 {
			t.Fatalf("replicas for %q: %v %v", key, replicas, err)
		}
		for _, id := range replicas {
			if id == "n4" {
				t.Fatalf("drained node still in replica set for %q", key)
			}
			entry, ok := c.Node(id).Store.GetVersioned(key)
			if !ok || string(entry.Value) != latest[key] {
				t.Fatalf("key %q missing/wrong on %s after drain", key, id)
			}
		}
	}
}

// A TTL-bearing write queued as a hint must not resurrect after it expires
// during an outage. Replay and AE both run after recovery; direct local-store
// checks ensure read repair cannot conceal a resurrection.
func TestSimExpiredHintDoesNotResurrect(t *testing.T) {
	c := sim.NewCluster(t, []string{"n1", "n2"}, sim.Options{RF: 2, Mode: repl.Async, Seed: 52})
	owner := c.Node("n1").Ring.GetNode("expiring-hint")
	peer := peerOf(t, c, owner)
	c.Suspend(peer)
	if r := c.Write(owner, "expiring-hint", []byte("short-lived"), 100*time.Millisecond); r.Status != protocol.StatusOK {
		t.Fatalf("write: %+v", r)
	}
	c.WaitForHint(owner)
	c.Advance(time.Second)
	if _, ok := c.Node(owner).Store.GetVersioned("expiring-hint"); ok {
		t.Fatal("owner retained an expired value")
	}
	c.Resume(peer)
	c.ReplayHints(owner)
	for range 4 {
		for _, id := range c.IDs() {
			c.RunAE(id)
		}
	}
	for _, id := range c.IDs() {
		if _, ok := c.Node(id).Store.GetVersioned("expiring-hint"); ok {
			t.Fatalf("expired value resurrected on %s", id)
		}
	}
}
