package tcp_test

import (
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// waitServerReady polls until the server reports ready, the way an
// orchestrator would before sending traffic or moving to the next node.
func waitServerReady(t *testing.T, srv *tcp.Server) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !srv.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("server never became ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A standalone server with no cluster serves immediately.
func TestReadyStandalone(t *testing.T) {
	c := newTestCluster(t, []string{"node1"}, 1, 0)
	defer c.shutdown()
	waitServerReady(t, c.nodes["node1"].srv)
}

// A configured cluster reports not-ready until Serve starts gossip and
// failure detection.
func TestReadyUnstartedCluster(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	if srv.Ready() {
		t.Fatal("unstarted cluster must not be ready")
	}
}

// A freshly rejoined member is not ready inside its quarantine window;
// clearing the window (or draining it) reports ready without waiting.
func TestReadyQuarantineWindow(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	srv := c.nodes["node1"].srv
	waitServerReady(t, srv)
	// A fresh join falls inside a newly configured warming window...
	srv.SetQuarantineInterval(time.Hour)
	if srv.Ready() {
		t.Fatal("freshly joined member must not be ready inside quarantine")
	}
	// ...and reports ready once the window is disabled.
	srv.SetQuarantineInterval(0)
	if !srv.Ready() {
		t.Fatal("member must be ready once quarantine is disabled")
	}
}

// A draining node is not ready even though it still serves reads.
func TestReadyDrainingIsNotReady(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, 0)
	defer c.shutdown()
	srv := c.nodes["node1"].srv
	fastGossip(c)
	waitServerReady(t, srv)
	if _, err := srv.StartDrain(30 * time.Second); err != nil {
		t.Fatalf("StartDrain: %v", err)
	}
	if srv.Ready() {
		t.Fatal("draining node must not report ready")
	}
}
