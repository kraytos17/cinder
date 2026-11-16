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
