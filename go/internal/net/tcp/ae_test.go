package tcp_test

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/go/internal/client"
	"github.com/kraytos17/cinder/go/internal/net/protocol"
	"github.com/kraytos17/cinder/go/internal/repl"
	"github.com/kraytos17/cinder/go/internal/store"
)

// AE convergence without hints or read repair: async fan-outs are drained
// to hints first, the replica restarts empty, and only then do manual
// anti-entropy rounds run. Direct store reads (never TCP Gets) verify
// state, since a quorum Get would heal on read and hide AE's work.
func TestAEConvergesAfterRestart(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	n1 := c.nodes["node1"]
	d1, err := client.Dial(n1.addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer d1.Close()
	put := func(key, val string) {
		t.Helper()
		res, err := d1.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(val)})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
	}
	put("k1", "v1")
	// Both nodes converge via replication here; AE must be a no-op.
	if n := n1.srv.AntiEntropy().RunRound(2); n != 0 {
		t.Fatalf("converged round repaired %d", n)
	}
	// Crash node2, write node1-owned keys (async fan-outs fail into hints).
	p2port := portOf(t, c.nodes["node2"].addr)
	c.kill("node2")
	var divergent []string
	for i := 0; len(divergent) < 10; i++ {
		if k := keyFor(i); ringOwner(t, k) == "node1" {
			put(k, "v")
			divergent = append(divergent, k)
		}
	}
	// Quiesce: wait until every fan-out finished (hinted). Without this,
	// in-flight async sends could land after the restart and masquerade
	// as anti-entropy repair.
	waitForHints(t, n1.mgr, len(divergent))
	c.restart("node2", p2port, []string{"node1", "node2"})
	n2 := c.nodes["node2"]
	// The fresh store must be empty and stay empty until AE runs.
	if got := countKeys(t, n2); got != 0 {
		t.Fatalf("fresh store has %d keys", got)
	}
	for _, k := range divergent {
		if _, ok := n2.mgr.Local().GetVersioned(k); ok {
			t.Fatalf("fresh store already has %q", k)
		}
	}
	// Converge purely through anti-entropy rounds.
	deadline := time.Now().Add(15 * time.Second)
	for {
		n1.srv.AntiEntropy().RunRound(2)
		n2.srv.AntiEntropy().RunRound(2)
		done := true
		for _, k := range divergent {
			if _, ok := n2.mgr.Local().GetVersioned(k); !ok {
				done = false
				break
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("AE did not converge node2")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Tombstones converge too: kill node2 again so the delete hint piles
	// up (the delete coalesces over the earlier put hint for the same
	// key, so the count stays flat — wait for the tombstone bit instead).
	c.kill("node2")
	if _, err := d1.Do(protocol.Request{Opcode: protocol.OpDel, Key: []byte(divergent[0])}); err != nil {
		t.Fatal(err)
	}
	waitForTombHint(t, n1.mgr, "node2", divergent[0])
	c.restart("node2", p2port, []string{"node1", "node2"})
	n2 = c.nodes["node2"]
	deadline = time.Now().Add(15 * time.Second)
	for {
		n1.srv.AntiEntropy().RunRound(2)
		n2.srv.AntiEntropy().RunRound(2)
		if e, ok := n2.mgr.Local().GetVersioned(divergent[0]); ok && e.Tombstone {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("tombstone did not converge")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func keyFor(i int) string {
	return "ae-key-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for n := i; n > 0; n /= 10 {
		pos--
		b[pos] = byte('0' + n%10)
	}
	return string(b[pos:])
}

func countKeys(t *testing.T, n *testNode) int {
	t.Helper()
	count := 0
	n.mgr.Local().ForEach(func(string, store.Entry) { count++ })
	return count
}

func waitForTombHint(t *testing.T, m *repl.Manager, target, key string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, h := range m.HintQueue().Snapshot(time.Now(), 2048) {
			if h.Target == target && string(h.Req.Key) == key && h.Req.Tombstone {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no tombstone hint for %q -> %q", target, key)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForHints(t *testing.T, m interface {
	HintCount() int
}, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for m.HintCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("only %d hints, want %d", m.HintCount(), n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
