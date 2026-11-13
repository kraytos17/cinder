package tcp_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

// Background hint replay: hints queued while a peer is down drain
// automatically on the replay tick once it returns, without a manual
// ReplayHints call. The test shortens the interval for faster execution.
func TestHintReplayTickHealsRestartedPeer(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	for _, n := range c.nodes {
		n.srv.SetReplayInterval(100 * time.Millisecond)
	}
	n1 := c.nodes["node1"]
	writeToOwner(t, c, "rk-sanity", "v")
	if _, ok := c.nodes["node2"].mgr.Local().GetVersioned("rk-sanity"); !ok {
		t.Fatal("replication path broken before kill")
	}
	// Crash node2; writes pile up as hints (async fan-outs fail).
	p2port := portOf(t, c.nodes["node2"].addr)
	c.kill("node2")
	var divergent []string
	for i := 0; len(divergent) < 10; i++ {
		if k := fmt.Sprintf("rk-key-%d", i); ringOwner(t, k) == "node1" {
			writeToOwner(t, c, k, "v")
			divergent = append(divergent, k)
		}
	}
	// Quiesce: every fan-out must have finished (hinted) before restart,
	// so only the replay tick can deliver what follows.
	waitForHint(t, n1.mgr, len(divergent))
	c.restart("node2", p2port, []string{"node1", "node2"})
	// No manual ReplayHints: the tick must converge node2 and drain hints.
	deadline := time.Now().Add(15 * time.Second)
	for {
		n2 := c.nodes["node2"]
		done := n1.mgr.HintCount() == 0
		for _, k := range divergent {
			if _, ok := n2.mgr.Local().GetVersioned(k); !ok {
				done = false
				break
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay tick did not heal node2 (hints=%d)", n1.mgr.HintCount())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestHintJournalSurvivesHintHolderRestart(t *testing.T) {
	dir := t.TempDir()
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	portA := listenerA.Addr().(*net.TCPAddr).Port
	listenerA.Close()
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	portB := listenerB.Addr().(*net.TCPAddr).Port
	listenerB.Close()
	key := findKeyOwnedBy(t, "nodeA")
	a, mgrA := startHintHolder(t, portA, portB, dir)
	writeToHintNode(t, fmt.Sprintf("127.0.0.1:%d", portA), key, "durable-hint")
	waitForHint(t, mgrA, 1)
	if err := a.ShutdownContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Restart the hint holder from the same data directory: the counter in
	// hints.log must populate the live queue before the server starts.
	lA2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", portA))
	if err != nil {
		t.Fatal(err)
	}
	a2, mgrA2 := startHintHolderWithListener(t, lA2, portA, portB, dir)
	_ = a2
	if got := mgrA2.HintCount(); got != 1 {
		t.Fatalf("recovered hints=%d, want 1", got)
	}
	b2, bAddr2, bStop2 := startHintPeer(t, portB, portA)
	_ = b2
	defer bStop2()
	deadline := time.Now().Add(10 * time.Second)
	for mgrA2.HintCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("recovered hint did not replay; remaining=%d", mgrA2.HintCount())
		}
		time.Sleep(20 * time.Millisecond)
	}
	waitForPeerValue(t, bAddr2, key, "durable-hint")
}

func findKeyOwnedBy(t *testing.T, id string) string {
	t.Helper()
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes([]string{"nodeA", "nodeB"}); err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		key := fmt.Sprintf("journal-key-%d", i)
		if ring.GetNode(key) == id {
			return key
		}
	}
	t.Fatal("could not find a key owned by nodeA")
	return ""
}

func startHintHolder(t *testing.T, portA, portB int, dir string) (*tcp.Server, *repl.Manager) {
	t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", portA))
	if err != nil {
		t.Fatal(err)
	}
	return startHintHolderWithListener(t, l, portA, portB, dir)
}

func startHintHolderWithListener(t *testing.T, l net.Listener, portA, portB int, dir string) (*tcp.Server, *repl.Manager) {
	t.Helper()
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "nodeA", Port: netport.Port(portA), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureCluster([]tcp.Peer{{ID: "nodeB", Host: "127.0.0.1", Port: netport.Port(portB)}}); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigurePersistence(dir); err != nil {
		t.Fatal(err)
	}
	pool := client.NewPool([]client.NodeAddr{{ID: "nodeB", Host: "127.0.0.1", Port: netport.Port(portB)}}, time.Second)
	mgr := repl.NewManager(repl.Config{Local: srv.Store(), Self: "nodeA", Transport: pool})
	if err := srv.ConfigureReplication(2, repl.Async, mgr); err != nil {
		t.Fatal(err)
	}
	journal, _, err := ae.OpenJournal(filepath.Join(dir, "hints.log"), mgr.HintQueue())
	if err != nil {
		t.Fatal(err)
	}
	mgr.SetHintJournal(journal)
	srv.SetReplayInterval(50 * time.Millisecond)
	srv.SetSuspectTimeout(time.Hour)
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return srv, mgr
}

func startHintPeer(t *testing.T, portB, portA int) (*tcp.Server, string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", portB))
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "nodeB", Port: netport.Port(portB), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureCluster([]tcp.Peer{{ID: "nodeA", Host: "127.0.0.1", Port: netport.Port(portA)}}); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	stop := func() { srv.Kill() }
	return srv, l.Addr().String(), stop
}

func writeToHintNode(t *testing.T, addr, key, value string) {
	t.Helper()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(value)})
	if err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("SET %s: %+v %v", key, res, err)
	}
}

func waitForPeerValue(t *testing.T, addr, key, value string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := client.Dial(addr, 500*time.Millisecond)
		if err == nil {
			res, callErr := c.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)})
			c.Close()
			if callErr == nil && res.Status == protocol.StatusOK && string(res.Value) == value {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("key %q did not reach %s with value %q", key, addr, value)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
