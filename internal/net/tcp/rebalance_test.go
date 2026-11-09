package tcp_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
)

// addNode joins a fresh empty node to a running test cluster. Gossip
// carries the membership both ways; quarantine stays 0 (wired in
// startNode) so migration lands immediately.
func addNode(t *testing.T, c *testCluster, id string, ids []string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ports := map[string]int{id: l.Addr().(*net.TCPAddr).Port}
	for pid, n := range c.nodes {
		_, p, err := net.SplitHostPort(n.addr)
		if err != nil {
			t.Fatal(err)
		}
		var pi int
		fmt.Sscanf(p, "%d", &pi)
		ports[pid] = pi
	}
	c.startNode(id, l, ports, ids)
	c.nodes[id].srv.SetGossipInterval(200 * time.Millisecond)
	c.nodes[id].srv.SetProbeInterval(200 * time.Millisecond)
}

func fastGossip(c *testCluster) {
	for _, n := range c.nodes {
		n.srv.SetGossipInterval(200 * time.Millisecond)
		n.srv.SetProbeInterval(200 * time.Millisecond)
	}
}

func writeToOwner(t *testing.T, c *testCluster, key, val string) {
	t.Helper()
	owner := ringOwner(t, key)
	n, ok := c.nodes[owner]
	if !ok {
		t.Fatalf("owner %s not in cluster", owner)
	}
	d, err := client.Dial(n.addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	res, err := d.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(val)})
	if err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("set %q on %s: %+v %v", key, owner, res, err)
	}
}

func waitForAlive(t *testing.T, srv *tcp.Server, id string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		if info, ok := srv.Table().Get(id); ok && info.State == cluster.Alive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never saw %s alive", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RF=2 join: every key must end up on BOTH members of its 3-node replica
// set after node3 joins — replicas rebalance just like primaries.
func TestRebalanceJoinRF2(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	fastGossip(c)

	keys := make([]string, 0, 50)
	for i := range 50 {
		k := fmt.Sprintf("rb2-key-%d", i)
		keys = append(keys, k)
		writeToOwner(t, c, k, "v-"+k)
	}
	addNode(t, c, "node3", []string{"node1", "node2", "node3"})
	waitForAlive(t, c.nodes["node1"].srv, "node3")
	waitForAlive(t, c.nodes["node3"].srv, "node1")

	deadline := time.Now().Add(20 * time.Second)
	for {
		done := true
		for _, k := range keys {
			nodes, err := testRing3.GetNodes(k, 2)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range nodes {
				if _, ok := c.nodes[id].mgr.Local().GetVersioned(k); !ok {
					done = false
					break
				}
			}
			if !done {
				break
			}
		}
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica sets never converged after join")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Migrated-away copies are reclaimed once every ring has converged: a
	// not-yet-converged peer can re-push a key the leaver just dropped
	// (harmless — redundant, never split-brain), so scan repeatedly until
	// no key sits outside its set. Table convergence implies ring
	// convergence (rebuild runs synchronously in the change callback).
	for _, n := range c.nodes {
		waitForAlive(t, n.srv, "node3")
	}
	lingering := func() (string, string) {
		for _, k := range keys {
			nodes, _ := testRing3.GetNodes(k, 2)
			want := map[string]bool{}
			for _, id := range nodes {
				want[id] = true
			}
			for id, n := range c.nodes {
				if !want[id] {
					if _, ok := n.mgr.Local().GetVersioned(k); ok {
						return k, id
					}
				}
			}
		}
		return "", ""
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		for _, n := range c.nodes {
			n.srv.RebalanceNow()
		}
		if k, id := lingering(); k == "" {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("%q still lingers on %s", k, id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RF=1 join: keys whose new owner is the joiner migrate there and leave
// their old owner.
func TestRebalanceJoinRF1(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 1, 0)
	defer c.shutdown()
	fastGossip(c)

	keys := make([]string, 0, 30)
	for i := range 30 {
		k := fmt.Sprintf("rb1-key-%d", i)
		keys = append(keys, k)
		writeToOwner(t, c, k, "v-"+k)
	}
	addNode(t, c, "node3", []string{"node1", "node2", "node3"})
	waitForAlive(t, c.nodes["node1"].srv, "node3")

	var moved int
	deadline := time.Now().Add(20 * time.Second)
	for {
		moved = 0
		complete := true
		for _, k := range keys {
			nodes, err := testRing3.GetNodes(k, 1)
			if err != nil {
				t.Fatal(err)
			}
			owner := nodes[0]
			if _, ok := c.nodes[owner].mgr.Local().GetVersioned(k); !ok {
				complete = false
				break
			}
			if owner == "node3" {
				moved++
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("primaries never converged after join")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if moved == 0 {
		t.Fatal("no key migrated to the joining node")
	}
	// Old owners release migrated keys: same poll-until-clean as RF=2
	// (rings may still be converging right after primaries land).
	for _, n := range c.nodes {
		waitForAlive(t, n.srv, "node3")
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		for _, n := range c.nodes {
			n.srv.RebalanceNow()
		}
		var lingering string
		for _, k := range keys {
			nodes, _ := testRing3.GetNodes(k, 1)
			for id, n := range c.nodes {
				if id != nodes[0] {
					if _, ok := n.mgr.Local().GetVersioned(k); ok {
						lingering = k
					}
				}
			}
		}
		if lingering == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q still lingers on an old owner", lingering)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
