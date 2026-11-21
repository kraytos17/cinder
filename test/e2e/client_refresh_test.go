package e2e_test

import (
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// TestE2EClientRefreshesAfterOwnerDeparture proves that a key whose learned
// owner drains and exits is re-routed after a transport failure. The client
// has only the original static addresses; surviving n1/n2 membership views
// must prune n3, and the retry must reach the new ring owner.
func TestE2EClientRefreshesAfterOwnerDeparture(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	bin := buildCinderd(t)
	adminBin := buildCinderAdmin(t)
	ids := []string{"n1", "n2", "n3"}
	ports := map[string]int{}
	for _, id := range ids {
		ports[id] = freePort(t)
	}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", ports[id])
	}
	peersOf := func(id string) []string {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, fmt.Sprintf("%s@%s", other, addrs[other]))
			}
		}
		return peers
	}
	procs := map[string]*proc{}
	for _, id := range ids {
		procs[id] = startNode(t, bin, id, ports[id], peersOf(id), 3)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	initialRing, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := initialRing.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	// Pick a key owned by n3 in the initial ring, with two known survivors
	// so post-departure quorum writes are still possible.
	var key string
	for i := range 1000 {
		k := fmt.Sprintf("refresh-owner-%d", i)
		if initialRing.GetNode(k) == "n3" {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("no key owned by n3 in the initial ring")
	}

	nodes := make([]client.NodeAddr, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, client.NodeAddr{ID: id, Host: "127.0.0.1", Port: mustPort(t, addrs[id])})
	}
	cl, err := client.NewCluster(client.ClusterConfig{Nodes: nodes, MaxRetries: 5, RPCTimeout: 2 * time.Second, BaseBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Route through n1. The initial ring points at n3, so n1 redirects and
	// teaches the client n3 as this key's owner.
	if res, err := cl.Set(key, []byte("before-drain")); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("initial write: %+v %v", res, err)
	}
	if res, err := cl.Get(key); err != nil || res.Status != protocol.StatusOK || string(res.Value) != "before-drain" {
		t.Fatalf("initial get: %+v %v", res, err)
	}

	// Drain and stop n3. Use explicit flags with the default quarantine so
	// the test exercises the production exit path rather than test-only
	// shortened settings.
	if out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "drain", "--timeout-ms", "60000"); code != 0 {
		t.Fatalf("drain exit=%d: %s", code, out)
	}
	if out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "shutdown"); code != 0 {
		t.Fatalf("shutdown exit=%d: %s", code, out)
	}
	waitUnserving(t, addrs["n3"])
	if err := procs["n3"].cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM n3: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- procs["n3"].cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("n3 exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("n3 did not exit")
	}

	// A subsequent call first hits the stale learned n3, gets a transport
	// failure, refreshes membership from n1/n2, and retries via the new ring.
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := cl.Set(key, []byte("after-drain"))
		if err == nil && res.Status == protocol.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client failed to refresh after owner departure: %+v %v", res, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, id := range []string{"n1", "n2"} {
		res := doGetVersioned(t, addrs[id], key)
		if res.Status != protocol.StatusOK || string(res.Value) != "after-drain" {
			t.Fatalf("refreshed owner write missing on %s: %+v", id, res)
		}
	}
}
