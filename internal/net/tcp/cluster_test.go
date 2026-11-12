package tcp_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

func TestAdminClusterNodeStateUsesWireLabels(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	defer srv.Shutdown()
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	table := srv.Table()
	table.Seed([]cluster.NodeInfo{{ID: "peer", Host: "127.0.0.1", Port: 7001}})
	for _, tc := range []struct {
		state cluster.NodeState
		want  string
	}{{cluster.Alive, "alive"}, {cluster.Suspect, "suspect"}, {cluster.Dead, "dead"}} {
		table.ApplyRumor(cluster.NodeInfo{ID: "peer", Host: "127.0.0.1", Port: 7001, State: tc.state, Incarnation: uint64(tc.state + 1)})
		got := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
		if got.Status != protocol.StatusOK {
			t.Fatalf("cluster info status = %v", got.Status)
		}
		var nodes []struct {
			ID    string `json:"id"`
			State string `json:"state"`
		}
		if err := json.Unmarshal(got.Value, &nodes); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, node := range nodes {
			if node.ID == "peer" {
				found = true
				if node.State != tc.want {
					t.Errorf("admin state = %q, want %q", node.State, tc.want)
				}
			}
		}
		if !found {
			t.Errorf("admin cluster omitted peer for state %q", tc.want)
		}
	}
}

func TestAdminRingReportsLiveRingStats(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node-a", Store: store.NewLRUStore(1<<20, nil)})
	defer srv.Shutdown()
	if err := srv.ConfigureCluster([]tcp.Peer{{ID: "node-b", Host: "b", Port: 7001}}); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureRebalance(2, 0); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureReplication(2, repl.Async, nil); err != nil {
		t.Fatal(err)
	}
	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminRing})
	if res.Status != protocol.StatusOK {
		t.Fatalf("status = %v", res.Status)
	}
	var got struct {
		Self          string `json:"self"`
		VNodesPerNode int    `json:"vnodes_per_node"`
		RF            int    `json:"rf"`
		RingVersion   uint64 `json:"ring_version"`
		Nodes         []struct {
			ID         string `json:"id"`
			TokenCount int    `json:"token_count"`
		} `json:"nodes"`
		Note string `json:"note"`
	}
	if err := json.Unmarshal(res.Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.Self != "node-a" || got.VNodesPerNode != hashring.DefaultVNodes || got.RF != 2 {
		t.Fatalf("ring header = %+v", got)
	}
	if len(got.Nodes) != 2 {
		t.Fatalf("nodes = %+v", got.Nodes)
	}
	for _, n := range got.Nodes {
		if n.TokenCount != hashring.DefaultVNodes {
			t.Errorf("node %q token count = %d", n.ID, n.TokenCount)
		}
	}
	if strings.Contains(got.Note, "150") {
		t.Errorf("ring note still hard-codes vnode count: %q", got.Note)
	}
	// The ring response is live placement membership: a known-dead peer must
	// remain in admin cluster but be absent from the ring node list.
	srv.Table().MarkDead("node-b")
	clusterRes := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
	var members []adminClusterNodeForTest
	if err := json.Unmarshal(clusterRes.Value, &members); err != nil {
		t.Fatal(err)
	}
	deadSeen := false
	for _, m := range members {
		if m.ID == "node-b" && m.State == "dead" {
			deadSeen = true
		}
	}
	if !deadSeen {
		t.Fatalf("cluster view did not retain dead peer: %+v", members)
	}
	res = srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminRing})
	if err := json.Unmarshal(res.Value, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "node-a" {
		t.Fatalf("ring still contains dead node: %+v", got.Nodes)
	}
}

func TestAdminRingReportsActualNonDefaultVNodes(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node", VNodes: 9, Store: store.NewLRUStore(1<<20, nil)})
	defer srv.Shutdown()
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminRing})
	if res.Status != protocol.StatusOK {
		t.Fatalf("status = %v", res.Status)
	}
	var got struct {
		VNodesPerNode int `json:"vnodes_per_node"`
	}
	if err := json.Unmarshal(res.Value, &got); err != nil {
		t.Fatal(err)
	}
	if got.VNodesPerNode != 9 {
		t.Fatalf("vnode count = %d, want configured ring count 9", got.VNodesPerNode)
	}
}

type adminClusterNodeForTest struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func TestAdminClusterStandaloneUsesConfiguredAdvertiseAddress(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "alone", AdvertiseHost: "cache.example.test", Port: 7123, Store: store.NewLRUStore(1<<20, nil)})
	defer srv.Shutdown()
	res := srv.Dispatch(protocol.Request{Opcode: protocol.OpAdminCluster})
	var nodes []struct {
		ID          string       `json:"id"`
		Host        string       `json:"host"`
		Port        netport.Port `json:"port"`
		Incarnation uint64       `json:"incarnation"`
	}
	if err := json.Unmarshal(res.Value, &nodes); err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Host != "cache.example.test" || nodes[0].Port != 7123 || nodes[0].Incarnation != 1 {
		t.Fatalf("standalone cluster = %+v", nodes)
	}
}

func TestConfigureRebalanceRejectsInvalidConfig(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureRebalance(0, 0); !errors.Is(err, hashring.ErrInvalidReplicaCount) {
		t.Fatalf("zero replica count error = %v", err)
	}
	if err := srv.ConfigureRebalance(hashring.ReplicaCount(hashring.MaxReplicas+1), 0); !errors.Is(err, hashring.ErrInvalidReplicaCount) {
		t.Fatalf("oversized replica count error = %v", err)
	}
	if err := srv.ConfigureRebalance(1, -time.Second); err == nil {
		t.Fatal("negative quarantine interval was accepted")
	}
}

// goCluster spins two Go nodes that know each other.
func goCluster(t *testing.T) (addrs map[string]string, stop func()) {
	t.Helper()
	// Pre-listen so ports are known before either ring is configured.
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p1, p2 := l1.Addr().(*net.TCPAddr).Port, l2.Addr().(*net.TCPAddr).Port
	s1 := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(p1), Store: store.NewLRUStore(1<<20, nil)})
	s2 := tcp.NewServer(tcp.ServerConfig{NodeID: "node2", Port: netport.Port(p2), Store: store.NewLRUStore(1<<20, nil)})
	if err := s1.ConfigureCluster([]tcp.Peer{{ID: "node2", Host: "127.0.0.1", Port: netport.Port(p2)}}); err != nil {
		t.Fatal(err)
	}
	if err := s2.ConfigureCluster([]tcp.Peer{{ID: "node1", Host: "127.0.0.1", Port: netport.Port(p1)}}); err != nil {
		t.Fatal(err)
	}
	go s1.Serve(l1)
	go s2.Serve(l2)
	return map[string]string{"node1": l1.Addr().String(), "node2": l2.Addr().String()},
		func() { s1.Shutdown(); s2.Shutdown() }
}

func TestClusterSetGetAcrossNodes(t *testing.T) {
	addrs, stop := goCluster(t)
	defer stop()
	cl, err := client.NewCluster(client.ClusterConfig{
		Nodes: []client.NodeAddr{
			{ID: "node1", Host: "127.0.0.1", Port: netport.Port(portOf(t, addrs["node1"]))},
			{ID: "node2", Host: "127.0.0.1", Port: netport.Port(portOf(t, addrs["node2"]))},
		},
		MaxRetries:  3,
		BaseBackoff: 5 * time.Millisecond,
		RPCTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// Every key must round-trip through its owner, wherever it lives.
	owners := map[string]int{}
	for i := range 50 {
		key := fmt.Sprintf("cluster-key-%d", i)
		res, err := cl.Set(key, fmt.Appendf(nil, "v%d", i))
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %v %v", key, res.Status, err)
		}
		got, err := cl.Get(key)
		if err != nil || got.Status != protocol.StatusOK || string(got.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %q: %+v %v", key, got, err)
		}
		ver, err := cl.GetVersioned(key)
		if err != nil || ver.Status != protocol.StatusOK || ver.Version == 0 {
			t.Fatalf("versioned %q: %+v %v", key, ver, err)
		}
		if del, err := cl.Del(key); err != nil || del.Status != protocol.StatusOK {
			t.Fatalf("del %q: %+v %v", key, del, err)
		}
		if del, err := cl.Get(key); err != nil || del.Status != protocol.StatusNotFound {
			t.Fatalf("deleted %q still visible: %+v %v", key, del, err)
		}
		owners[ringOwner(t, key)]++
	}
	if len(owners) < 2 {
		t.Fatalf("keys did not spread across owners: %v", owners)
	}
}

// ringOwner mirrors the servers' ring to assert key placement.
var testRing = func() *hashring.Ring {
	r, _ := hashring.New(hashring.DefaultVNodes, 0)
	_ = r.SetNodes([]string{"node1", "node2"})
	return r
}()

func ringOwner(t *testing.T, key string) string {
	t.Helper()
	return testRing.GetNode(key)
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var p int
	fmt.Sscanf(port, "%d", &p)
	return p
}
