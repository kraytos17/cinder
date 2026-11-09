package tcp_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// clusterServer starts one Go node with a full two-node ring view.
func clusterServer(t *testing.T, id string, peers []tcp.Peer) (addr string, stop func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer(id, port, st, nil)
	if err := srv.ConfigureCluster(peers); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	return l.Addr().String(), srv.Shutdown
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
	s1 := tcp.NewServer("node1", p1, store.NewLRUStore(1<<20, nil), nil)
	s2 := tcp.NewServer("node2", p2, store.NewLRUStore(1<<20, nil), nil)
	if err := s1.ConfigureCluster([]tcp.Peer{{ID: "node2", Host: "127.0.0.1", Port: p2}}); err != nil {
		t.Fatal(err)
	}
	if err := s2.ConfigureCluster([]tcp.Peer{{ID: "node1", Host: "127.0.0.1", Port: p1}}); err != nil {
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
			{ID: "node1", Host: "127.0.0.1", Port: portOf(t, addrs["node1"])},
			{ID: "node2", Host: "127.0.0.1", Port: portOf(t, addrs["node2"])},
		},
		MaxRetries:    3,
		BaseBackoffMs: 5,
		RPCTimeout:    5 * time.Second,
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
