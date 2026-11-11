package load_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/load"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

// spinNode starts one loopback server and returns its address.
func spinNode(t *testing.T, id string) (client.NodeAddr, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{
		NodeID: id,
		Port:   netport.Port(l.Addr().(*net.TCPAddr).Port),
		Store:  store.NewLRUStore(1<<20, nil),
	})
	go srv.Serve(l)
	return client.NodeAddr{ID: id, Host: "127.0.0.1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port)},
		func() { srv.Shutdown() }
}

func TestRunSmokeExact(t *testing.T) {
	addr, stop := spinNode(t, "node1")
	defer stop()
	res, err := load.Run(context.Background(), load.Config{
		Nodes:     []client.NodeAddr{addr},
		Workers:   1,
		TotalOps:  200,
		KeySpace:  50,
		ValueSize: 16,
		Mix:       load.Mix{Get: 1, Set: 1, Del: 1},
		Seed:      7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops != 200 {
		t.Fatalf("ops = %d, want 200", res.Ops)
	}
	if got := res.Gets.Count() + res.Sets.Count() + res.Dels.Count(); got != 200 {
		t.Fatalf("histogram total = %d, want 200", got)
	}
	if got := res.Errors.Load(); got != 0 {
		t.Fatalf("errors = %d, first %v", got, res.FirstErr.Load())
	}
	if res.Throughput() <= 0 {
		t.Fatal("no throughput measured")
	}
	if res.Sets.Quantile(0.99) <= 0 {
		t.Fatal("empty set p99")
	}
}

func TestRunValidation(t *testing.T) {
	for name, cfg := range map[string]load.Config{
		"no nodes":   {Workers: 1, TotalOps: 10},
		"bad hot":    {Nodes: []client.NodeAddr{{ID: "n1"}}, Workers: 1, TotalOps: 1, HotFraction: 2},
		"bad values": {Nodes: []client.NodeAddr{{ID: "n1"}}, Workers: 1, TotalOps: 1, ValueSize: -1},
	} {
		if _, err := load.Run(context.Background(), cfg); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
}

func TestRunCancel(t *testing.T) {
	addr, stop := spinNode(t, "node1")
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	res, err := load.Run(ctx, load.Config{
		Nodes:     []client.NodeAddr{addr},
		Workers:   4,
		KeySpace:  100,
		ValueSize: 16,
		Seed:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops == 0 {
		t.Fatal("no ops before cancel")
	}
}

// TestRunThreeNodeRF2 runs the default mix against a 3-node RF=2 async
// cluster: routing, quorum-less async fan-out, and read-repair all sit
// on the measured path. Multi-worker over-claim is bounded by the worker
// count, so the total lands in [TotalOps, TotalOps+Workers].
func TestRunThreeNodeRF2(t *testing.T) {
	ids := []string{"node1", "node2", "node3"}
	type node struct {
		srv  *tcp.Server
		addr client.NodeAddr
	}
	nodes := make([]node, 0, len(ids))
	ports := map[string]int{}
	listeners := make([]net.Listener, 0, len(ids))
	for _, id := range ids {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		ports[id] = l.Addr().(*net.TCPAddr).Port
	}
	for i, id := range ids {
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
			t.Fatal(err)
		}
		pool := client.NewPool(addrs, 5*time.Second)
		mgr := repl.NewManager(repl.Config{Local: st, Self: id, Clock: nil, Transport: pool})
		if err := srv.ConfigureReplication(2, repl.Async, mgr); err != nil {
			t.Fatal(err)
		}
		go srv.Serve(listeners[i])
		nodes = append(nodes, node{srv: srv, addr: client.NodeAddr{ID: id, Host: "127.0.0.1", Port: netport.Port(ports[id])}})
	}
	defer func() {
		for _, n := range nodes {
			n.srv.Shutdown()
		}
	}()
	addrs := []client.NodeAddr{nodes[0].addr, nodes[1].addr, nodes[2].addr}
	res, err := load.Run(context.Background(), load.Config{
		Nodes: addrs, Workers: 4, TotalOps: 1000,
		KeySpace: 200, HotKeys: 10, HotFraction: 0.5, ValueSize: 32, Seed: 3,
		MaxRetries: 5, RPCTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Ops < 1000 || res.Ops > 1004 {
		t.Fatalf("ops = %d, want [1000,1004]", res.Ops)
	}
	if got := res.Errors.Load(); got != 0 {
		t.Fatalf("errors = %d, first %v", got, res.FirstErr.Load())
	}
	if res.Throughput() <= 0 || res.Sets.Quantile(0.99) <= 0 {
		t.Fatalf("no signal: %+v", res.Throughput())
	}
}
