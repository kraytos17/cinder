package tcp_test

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

// startPersistedServer builds a single node with WAL + snapshots in dir.
func startPersistedServer(t *testing.T, id string, dir string) (*tcp.Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: id, Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigurePersistence(dir); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return srv, l.Addr().String()
}

func TestServerRestartRecovers(t *testing.T) {
	dir := t.TempDir()
	srv, addr := startPersistedServer(t, "node1", dir)
	_ = srv
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	set := func(key, val string) {
		t.Helper()
		res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(val)})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
	}
	set("a", "1")
	set("b", "2")
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("t"), Value: []byte("x"), TTL: time.Hour}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("set ttl: %+v %v", res, err)
	}
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpDel, Key: []byte("a")}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("del: %+v %v", res, err)
	}
	c.Close()
	srv.Shutdown()

	// Restart empty from the same directory: live keys return, deletes hold.
	_, addr2 := startPersistedServer(t, "node1", dir)
	c2, err := client.Dial(addr2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	res, err := c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("b")})
	if err != nil || res.Status != protocol.StatusOK || string(res.Value) != "2" {
		t.Fatalf("b after restart: %+v %v", res, err)
	}
	res, _ = c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("t")})
	if res.Status != protocol.StatusOK {
		t.Fatalf("ttl key after restart: %+v", res)
	}
	res, _ = c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("a")})
	if res.Status != protocol.StatusNotFound {
		t.Fatalf("deleted key resurrected: %+v", res)
	}
	ver, err := c2.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte("a")})
	if err != nil || ver.Status != protocol.StatusOK || !ver.Tombstone {
		t.Fatalf("tombstone lost across restart: %+v %v", ver, err)
	}
}

func TestServerCompactAndRecover(t *testing.T) {
	dir := t.TempDir()
	srv, addr := startPersistedServer(t, "node1", dir)
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26))
		res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte("v")})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
	}
	res, err := c.Do(protocol.Request{Opcode: protocol.OpAdminCompact})
	if err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("compact: %+v %v", res, err)
	}
	c.Close()
	srv.Shutdown()

	_, addr2 := startPersistedServer(t, "node1", dir)
	c2, err := client.Dial(addr2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := range 20 {
		key := string(rune('a'+i%26)) + string(rune('0'+i/26))
		res, err := c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("key %q lost across compact+restart: %+v %v", key, res, err)
		}
	}
}

// TestServerAutoCompacts snapshotting on a timer: with a short snapshot
// interval the server writes snapshots without any admin call, and a
// restart recovers from them.
func TestServerAutoCompacts(t *testing.T) {
	dir := t.TempDir()
	srv, addr := startPersistedServer(t, "node1", dir)
	srv.SetSnapshotInterval(200 * time.Millisecond)
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		key := fmt.Sprintf("auto-key-%d", i)
		if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte("v")}); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
	}
	c.Close()
	// Wait for the background tick to snapshot (no admin call).
	deadline := time.Now().Add(10 * time.Second)
	for {
		matches, _ := filepath.Glob(filepath.Join(dir, "snapshot_*.dat"))
		if len(matches) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("auto-compact never snapshotted")
		}
		time.Sleep(50 * time.Millisecond)
	}
	srv.Shutdown()

	_, addr2 := startPersistedServer(t, "node1", dir)
	c2, err := client.Dial(addr2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := range 10 {
		key := fmt.Sprintf("auto-key-%d", i)
		if res, err := c2.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)}); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("key %q lost across auto-compact + restart: %+v %v", key, res, err)
		}
	}
}

func TestClusterDoesNotAdvertiseNodeBeforePersistenceRecovery(t *testing.T) {
	peerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peerPort := netport.Port(peerListener.Addr().(*net.TCPAddr).Port)
	peer := tcp.NewServer(tcp.ServerConfig{
		NodeID: "peer", Port: peerPort, Store: store.NewLRUStore(1<<20, nil),
	})
	if err := peer.ConfigureCluster(nil); err != nil {
		t.Fatal(err)
	}
	peerDone := make(chan error, 1)
	go func() { peerDone <- peer.Serve(peerListener) }()
	t.Cleanup(func() {
		peer.Shutdown()
		select {
		case err := <-peerDone:
			if err != nil {
				t.Errorf("peer Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("peer Serve did not stop")
		}
	})

	nodeListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	nodePort := netport.Port(nodeListener.Addr().(*net.TCPAddr).Port)
	node := tcp.NewServer(tcp.ServerConfig{
		NodeID: "recovering", Port: nodePort, Store: store.NewLRUStore(1<<20, nil),
	})
	if err := node.ConfigureCluster([]tcp.Peer{{ID: "peer", Host: "127.0.0.1", Port: peerPort}}); err != nil {
		t.Fatal(err)
	}

	assertNotAdvertised := func(stage string) {
		t.Helper()
		if _, ok := peer.Table().Get("recovering"); ok {
			t.Fatalf("node was advertised before recovery completed (%s)", stage)
		}
	}
	assertNotAdvertised("cluster configured")
	if err := node.ConfigurePersistence(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	assertNotAdvertised("persistence recovered")

	nodeDone := make(chan error, 1)
	go func() { nodeDone <- node.Serve(nodeListener) }()
	t.Cleanup(func() {
		node.Shutdown()
		select {
		case err := <-nodeDone:
			if err != nil {
				t.Errorf("node Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("node Serve did not stop")
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, ok := peer.Table().Get("recovering"); ok && got.State == cluster.Alive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recovered node did not become visible after StartCluster")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
