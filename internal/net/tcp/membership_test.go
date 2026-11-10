package tcp_test

import (
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

func waitAlive(t *testing.T, srv *tcp.Server, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if info, ok := srv.Table().Get(id); ok && info.State == cluster.Alive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never saw %s alive", srvName(srv), id)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func srvName(srv *tcp.Server) string {
	if info, ok := srv.Table().Self(); ok {
		return info.ID
	}
	return "?"
}

// A hard-killed node (no leave broadcast) must be suspected then marked
// dead by failure detection, dropping out of the survivor's ring.
func TestFailureDetectorMarksDeadNode(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p1 := l1.Addr().(*net.TCPAddr).Port
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p2 := l2.Addr().(*net.TCPAddr).Port

	mk := func(id string, l net.Listener, port int, peers []tcp.Peer) *tcp.Server {
		srv := tcp.NewServer(tcp.ServerConfig{NodeID: id, Port: netport.Port(port), Store: store.NewLRUStore(1<<20, nil)})
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		srv.SetGossipInterval(100 * time.Millisecond)
		srv.SetProbeInterval(100 * time.Millisecond)
		srv.SetSuspectTimeout(300 * time.Millisecond)
		go srv.Serve(l)
		return srv
	}
	s1 := mk("node1", l1, p1, []tcp.Peer{{ID: "node2", Host: "127.0.0.1", Port: netport.Port(p2)}})
	s2 := mk("node2", l2, p2, []tcp.Peer{{ID: "node1", Host: "127.0.0.1", Port: netport.Port(p1)}})
	defer s1.Shutdown()
	defer s2.Shutdown()

	waitAlive(t, s1, "node2")
	waitAlive(t, s2, "node1")

	// Crash node2: no leave, so node1 must detect it.
	s2.Kill()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if info, ok := s1.Table().Get("node2"); ok && info.State == cluster.Dead {
			break
		}
		if time.Now().After(deadline) {
			n, _ := s1.Table().Get("node2")
			t.Fatalf("node2 never marked dead: %+v", n)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The dead node leaves the ring: all keys route to the survivor.
	for _, key := range []string{"a", "b", "c", "quorum-key-42"} {
		if got := s1.OwnerOf(key); got != "node1" {
			t.Fatalf("key %q routes to %q after death", key, got)
		}
	}
}
