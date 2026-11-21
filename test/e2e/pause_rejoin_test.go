package e2e_test

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

func (p *proc) stop(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}
}

func (p *proc) cont(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT: %v", err)
	}
}

// TestE2EPausePastSuspectRejoins freezes a node with SIGSTOP past the
// failure-detector suspect timeout while writes continue, then thaws it
// with SIGCONT and requires every acknowledged write to converge on all
// three nodes. Unlike SIGKILL, the paused process keeps its sockets: hung
// replication sends exercise the RPC-timeout path rather than fast refusal.
func TestE2EPausePastSuspectRejoins(t *testing.T) {
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
	peersOf := func(id string) []string {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, fmt.Sprintf("%s@%s", other, addrs[other]))
			}
		}
		return peers
	}

	procs := map[string]*proc{}
	for _, id := range ids {
		procs[id] = startNode(t, bin, id, ports[id], peersOf(id), 3)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}

	w := startWriter(addrs, ring, "e2e-ps")
	time.Sleep(1500 * time.Millisecond)

	procs["n3"].stop(t)         // freeze: no gossip, no replies, sockets stay open
	time.Sleep(3 * time.Second) // past the 1s suspect timeout: convicted
	ackedFrozen, failedFrozen := w.acked.Load(), w.failed.Load()
	procs["n3"].cont(t) // thaw
	waitReady(t, addrs["n3"], 20*time.Second)
	time.Sleep(3 * time.Second) // rejoin, rebalance, anti-entropy
	w.finish()

	t.Logf("acked=%d failed=%d (frozen window acked=%d failed=%d)",
		w.acked.Load(), w.failed.Load(), ackedFrozen, failedFrozen)
	if ackedFrozen == 0 {
		t.Fatal("no write acknowledged before the thaw: cluster stalled")
	}

	acked := w.log.snapshot()
	if len(acked) == 0 {
		t.Fatal("verifier log is empty")
	}
	deadline := time.Now().Add(60 * time.Second)
	for key, want := range acked {
		for _, id := range ids {
			for {
				if res := doGetVersioned(t, addrs[id], key); res.Status == protocol.StatusOK && string(res.Value) == want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acked %q missing/wrong on %s", key, id)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
