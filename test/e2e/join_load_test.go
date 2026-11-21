package e2e_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// TestE2EJoinUnderLoadPreservesAckedWrites starts a 3-node RF=3 quorum
// cluster under a continuous write load, joins an empty 4th node, refreshes
// the writer's routing view the way a topology-aware client would, and
// requires every acknowledged write — from before and after the join — to
// land on its current replica set after migration settles.
func TestE2EJoinUnderLoadPreservesAckedWrites(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	bin := buildCinderd(t)
	ids := []string{"n1", "n2", "n3"}
	ports := map[string]int{}
	for _, id := range ids {
		ports[id] = freePort(t)
	}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", ports[id])
	}
	peersOf := func(self string, members []string) []string {
		var peers []string
		for _, other := range members {
			if other != self {
				peers = append(peers, fmt.Sprintf("%s@%s", other, addrs[other]))
			}
		}
		return peers
	}

	procs := map[string]*proc{}
	for _, id := range ids {
		procs[id] = startNode(t, bin, id, ports[id], peersOf(id, ids), 3)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	ring3, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring3.SetNodes(ids); err != nil {
		t.Fatal(err)
	}

	w := startWriter(addrs, ring3, "e2e-jl")
	time.Sleep(1500 * time.Millisecond) // pre-join writes on the 3-node ring

	// Join an empty 4th node with the full member list as seeds.
	ports["n4"] = freePort(t)
	addrs["n4"] = fmt.Sprintf("127.0.0.1:%d", ports["n4"])
	four := []string{"n1", "n2", "n3", "n4"}
	procs["n4"] = startNode(t, bin, "n4", ports["n4"], peersOf("n4", four), 3)
	waitReady(t, addrs["n4"], 20*time.Second)

	ring4, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring4.SetNodes(four); err != nil {
		t.Fatal(err)
	}
	w.setRing(ring4)                    // topology refresh: route by the 4-node ring
	time.Sleep(2500 * time.Millisecond) // post-join writes under migration
	w.finish()
	if w.acked.Load() == 0 {
		t.Fatal("no write acknowledged: cluster stalled")
	}
	t.Logf("acked=%d failed=%d", w.acked.Load(), w.failed.Load())

	// Settle migration, then require every acked value on its current
	// replica set (poll: the joiner backfills asynchronously).
	time.Sleep(4 * time.Second)
	acked := w.log.snapshot()
	deadline := time.Now().Add(60 * time.Second)
	for key, want := range acked {
		replicas, err := ring4.GetNodes(key, 3)
		if err != nil || len(replicas) != 3 {
			t.Fatalf("replicas for %q: %v, %v", key, replicas, err)
		}
		for _, id := range replicas {
			for {
				res := doGetVersioned(t, addrs[id], key)
				if res.Status == protocol.StatusOK && string(res.Value) == want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acked %q missing/wrong on %s (want %q, got %+v)", key, id, want, res)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
