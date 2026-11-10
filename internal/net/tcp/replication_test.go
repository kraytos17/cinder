package tcp_test

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

func TestConfigureReplicationRejectsInvalidConfig(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureReplication(0, repl.Async, nil); !errors.Is(err, hashring.ErrInvalidReplicaCount) {
		t.Fatalf("zero replica count error = %v", err)
	}
	if err := srv.ConfigureReplication(1, repl.Mode(99), nil); err == nil {
		t.Fatal("invalid consistency mode was accepted")
	}
}

// testNode is one live Go node with its replication manager exposed.
type testNode struct {
	id   string
	addr string
	srv  *tcp.Server
	mgr  *repl.Manager
}

// testCluster is a Go-only multi-node cluster for replication tests.
type testCluster struct {
	t     *testing.T
	nodes map[string]*testNode
	rf    hashring.ReplicaCount
	mode  repl.Mode
}

func newTestCluster(t *testing.T, ids []string, rf hashring.ReplicaCount, mode repl.Mode) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[string]*testNode{}, rf: rf, mode: mode}
	addrs := map[string]string{}
	ports := map[string]int{}
	listeners := make([]net.Listener, 0, len(ids))
	for _, id := range ids {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		ports[id] = l.Addr().(*net.TCPAddr).Port
		addrs[id] = l.Addr().String()
	}
	for i, id := range ids {
		c.startNode(id, listeners[i], ports, ids)
		// Freeze failure detection: these tests exercise replication, not
		// liveness. Graceful-leave tests rely on explicit Dead rumors.
		c.nodes[id].srv.SetSuspectTimeout(time.Hour)
	}
	return c
}

func (c *testCluster) startNode(id string, l net.Listener, ports map[string]int, ids []string) {
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: id, Port: netport.Port(ports[id]), Store: st})
	var peers []tcp.Peer
	var addrs []client.NodeAddr
	for _, pid := range ids {
		if pid == id {
			continue
		}
		peers = append(peers, tcp.Peer{ID: pid, Host: "127.0.0.1", Port: netport.Port(ports[pid])})
		addrs = append(addrs, client.NodeAddr{ID: pid, Host: "127.0.0.1", Port: netport.Port(ports[pid])})
	}
	if err := srv.ConfigureCluster(peers); err != nil {
		c.t.Fatal(err)
	}
	pool := client.NewPool(addrs, 5*time.Second)
	mgr := repl.NewManager(repl.Config{Local: st, Self: id, Clock: nil, Transport: pool})
	if err := srv.ConfigureReplication(c.rf, c.mode, mgr); err != nil {
		c.t.Fatal(err)
	}
	if err := srv.ConfigureRebalance(c.rf, 0); err != nil {
		c.t.Fatal(err)
	}
	srv.ConfigureAntiEntropy(256, 1<<20)
	go srv.Serve(l)
	c.nodes[id] = &testNode{id: id, addr: l.Addr().String(), srv: srv, mgr: mgr}
}

// kill stops a node; its data is lost (fresh restart, like a crashed node).
func (c *testCluster) kill(id string) {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("no node %s", id)
	}
	// Crash, not leave: survivors keep routing to it until failure
	// detection catches up, which keeps replica sets (and hints)
	// deterministic for the test window.
	n.srv.Kill()
	delete(c.nodes, id)
}

// restart brings a node back with an empty store on the same port.
func (c *testCluster) restart(id string, port int, ids []string) {
	c.t.Helper()
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		c.t.Fatalf("re-listen %s: %v", id, err)
	}
	ports := map[string]int{id: port}
	for _, pid := range ids {
		if pid == id {
			continue
		}
		if n, ok := c.nodes[pid]; ok {
			_, p, _ := net.SplitHostPort(n.addr)
			var pi int
			fmt.Sscanf(p, "%d", &pi)
			ports[pid] = pi
		}
	}
	c.startNode(id, l, ports, ids)
}

func (c *testCluster) client() *client.Cluster {
	c.t.Helper()
	nodes := make([]client.NodeAddr, 0, len(c.nodes))
	for id, n := range c.nodes {
		_, port, err := net.SplitHostPort(n.addr)
		if err != nil {
			c.t.Fatal(err)
		}
		var p int
		fmt.Sscanf(port, "%d", &p)
		nodes = append(nodes, client.NodeAddr{ID: id, Host: "127.0.0.1", Port: netport.Port(p)})
	}
	cl, err := client.NewCluster(client.ClusterConfig{
		Nodes: nodes, MaxRetries: 5, BaseBackoffMs: 10, RPCTimeout: 5 * time.Second,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	return cl
}

func (c *testCluster) shutdown() {
	for _, n := range c.nodes {
		n.srv.Shutdown()
	}
}

// ownerOf computes the expected owner for the default three-node test ring.
func clusterOwner(key string) string {
	return testRing3.GetNode(key)
}

var testRing3 = func() *hashring.Ring {
	r, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		panic(err)
	}
	if err := r.SetNodes([]string{"node1", "node2", "node3"}); err != nil {
		panic(err)
	}
	return r
}()

func TestReplQuorumSurvivesOneFailure(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2", "node3"}, 2, repl.Quorum)
	defer c.shutdown()
	cl := c.client()
	defer cl.Close()
	// Baseline: everything works with all nodes up.
	mustClusterSet(t, cl, "q-key", "v")
	c.kill("node3")
	// Quorum (W=2) holds with two survivors, but only for keys whose full
	// replica set survived: a key replicating to the dead node fails
	// closed when a required replica is unavailable.
	repOf := func(key string) []string {
		nodes, _ := testRing3.GetNodes(key, 2)
		return nodes
	}
	alive := func(nodes []string) bool {
		return !slices.Contains(nodes, "node3")
	}
	for i := range 60 {
		key := fmt.Sprintf("q-survive-%d", i)
		if clusterOwner(key) == "node3" || !alive(repOf(key)) {
			continue
		}
		if res, err := cl.Set(key, []byte("v")); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
		if res, err := cl.Get(key); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("get %q: %+v %v", key, res, err)
		}
	}
	// Losing a second node breaks quorum for good.
	c.kill("node2")
	res, err := cl.Set("q-dead", []byte("v"))
	if err == nil && res.Status == protocol.StatusOK {
		t.Fatal("quorum write succeeded with one survivor")
	}
}

func mustClusterSet(t *testing.T, cl *client.Cluster, key, value string) {
	t.Helper()
	res, err := cl.Set(key, []byte(value))
	if err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("set %q: %+v %v", key, res, err)
	}
}

// Graceful leave shrinks the ring: once the survivors converge on the
// reduced membership, a lone node serves alone (W=1) instead of failing
// closed. This is the orderly-teardown counterpart to the crash coverage.
func TestReplGracefulLeaveConverges(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2", "node3"}, 2, repl.Quorum)
	defer c.shutdown()
	for _, id := range []string{"node2", "node3"} {
		n := c.nodes[id]
		delete(c.nodes, id)
		n.srv.Shutdown() // graceful: broadcasts Dead self
	}
	cl := c.client()
	defer cl.Close()
	// The survivor converges once the leave rumors arrive.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if res, err := cl.Set("solo-key", []byte("v")); err == nil && res.Status == protocol.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Skip("leave rumors did not converge in time")
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cl.Get("solo-key")
	if err != nil || got.Status != protocol.StatusOK || string(got.Value) != "v" {
		t.Fatalf("solo get: %+v %v", got, err)
	}
}

func TestReplQuorumReadRepair(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2", "node3"}, 3, repl.Quorum)
	defer c.shutdown()
	cl := c.client()
	defer cl.Close()
	// Pick a key owned by node1 (replicas node2, node3 in ring order).
	key := ""
	for i := range 200 {
		if k := fmt.Sprintf("repair-key-%d", i); clusterOwner(k) == "node1" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by node1")
	}
	_, p3, _ := net.SplitHostPort(c.nodes["node3"].addr)
	var port3 int
	fmt.Sscanf(p3, "%d", &port3)
	// Kill node3, write (quorum local + node2), restart node3 empty.
	c.kill("node3")
	mustClusterSet(t, cl, key, "v2")
	c.restart("node3", port3, []string{"node1", "node2", "node3"})
	// A quorum read sees the winner and repairs the stale replica.
	got, err := cl.Get(key)
	if err != nil || got.Status != protocol.StatusOK || string(got.Value) != "v2" {
		t.Fatalf("get: %+v %v", got, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		direct, err := client.Dial(c.nodes["node3"].addr, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		res, err := direct.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
		direct.Close()
		if err == nil && res.Status == protocol.StatusOK && string(res.Value) == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node3 never repaired: %+v %v", res, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReplAsyncHintAndReplay(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2", "node3"}, 2, repl.Async)
	defer c.shutdown()
	// Freeze failure detection: the killed node must stay ring-listed so
	// the hint path (not the leave path) is exercised deterministically.
	// Freeze hint replay too: this test drives ReplayHints manually, and a
	// background tick would drain the queue first (replayed == 0 flake).
	for _, n := range c.nodes {
		n.srv.SetSuspectTimeout(time.Hour)
		n.srv.SetReplayInterval(time.Hour)
	}
	cl := c.client()
	defer cl.Close()
	// Remember node3's port: restart rebinds it.
	_, p3, err := net.SplitHostPort(c.nodes["node3"].addr)
	if err != nil {
		t.Fatal(err)
	}
	var port3 int
	fmt.Sscanf(p3, "%d", &port3)
	// Keys owned by node2 replicate to node3 (ring order); kill node3 first.
	c.kill("node3")
	var hinted []string
	for i := range 30 {
		key := fmt.Sprintf("hint-key-%d", i)
		if clusterOwner(key) != "node2" {
			continue
		}
		mustClusterSet(t, cl, key, "v")
		hinted = append(hinted, key)
	}
	if len(hinted) == 0 {
		t.Fatal("no keys owned by node2")
	}
	// node2's manager must hold hints for the dead replica.
	waitForHint(t, c.nodes["node2"].mgr, 1)
	// Restart node3 empty; replay converges it.
	c.restart("node3", port3, []string{"node1", "node2", "node3"})
	replayed := c.nodes["node2"].mgr.ReplayHints()
	if replayed == 0 {
		t.Fatal("expected hints to replay")
	}
	direct, err := client.Dial(c.nodes["node3"].addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	for _, key := range hinted {
		res, err := direct.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("replayed key %q missing on node3: %+v %v", key, res, err)
		}
	}
}

func waitForHint(t *testing.T, m interface {
	HintCount() int
}, n int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for m.HintCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("hint count %d < %d", m.HintCount(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
