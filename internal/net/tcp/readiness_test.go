package tcp_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/repl"
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

// A warming member answers versioned reads for keys it has not (yet)
// replicated with NotReady instead of NotFound: the copy may exist on the
// other replicas, and a miss vote must not masquerade as deletion. Once
// warmed, the same miss is a genuine NotFound, and quorum reads converge
// through the available voters either way.
func TestWarmingNodeDeflectsVersionedReads(t *testing.T) {
	c := newTestCluster(t, []string{"node1", "node2"}, 2, repl.Quorum)
	defer c.shutdown()
	warming, steady := c.nodes["node1"].srv, c.nodes["node2"].srv
	waitServerReady(t, warming)
	waitServerReady(t, steady)

	warming.SetQuarantineInterval(time.Hour)

	// Absent key owned by the warming node: NotReady while warming.
	var missing string
	for i := range 100 {
		k := fmt.Sprintf("warming-miss-%d", i)
		if ringOwner(t, k) == "node1" {
			missing = k
			break
		}
	}
	if missing == "" {
		t.Fatal("no key owned by node1")
	}
	d := dial(t, c.nodes["node1"].addr)
	res, err := d.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(missing)})
	d.Close()
	if err != nil || res.Status != protocol.StatusNotReady {
		t.Fatalf("warming miss = %+v %v, want NotReady", res, err)
	}

	// Warmed up, the same miss is a genuine NotFound.
	warming.SetQuarantineInterval(0)
	d2 := dial(t, c.nodes["node1"].addr)
	res, err = d2.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(missing)})
	d2.Close()
	if err != nil || res.Status != protocol.StatusNotFound {
		t.Fatalf("warmed miss = %+v %v, want NotFound", res, err)
	}

	// Quorum reads converge through the available voter either way.
	var present string
	for i := range 100 {
		k := fmt.Sprintf("warming-present-%d", i)
		if ringOwner(t, k) == "node2" {
			present = k
			break
		}
	}
	writeToOwner(t, c, present, "v")
	warming.SetQuarantineInterval(time.Hour)
	d3 := dial(t, c.nodes["node2"].addr)
	res, err = d3.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(present)})
	d3.Close()
	if err != nil || res.Status != protocol.StatusOK || string(res.Value) != "v" {
		t.Fatalf("quorum read with warming voter: %+v %v", res, err)
	}
	d4 := dial(t, c.nodes["node2"].addr)
	res, err = d4.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("warming-absent-everywhere")})
	d4.Close()
	if err != nil || res.Status != protocol.StatusNotFound {
		t.Fatalf("quorum miss with warming voter: %+v %v", res, err)
	}
}
