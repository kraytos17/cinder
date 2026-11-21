package e2e_test

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// memberState extracts one member's state from admin cluster JSON.
// Missing or unparsable members report "" (treated as gone).
func memberState(t *testing.T, clusterJSON, id string) string {
	t.Helper()
	var members []struct {
		ID    string `json:"id"`
		State string `json:"state"`
	}
	if err := json.Unmarshal([]byte(clusterJSON), &members); err != nil {
		return "unparsable"
	}
	for _, m := range members {
		if m.ID == id {
			return m.State
		}
	}
	return ""
}

// TestE2ERemoveNodeLeavesHealthyCluster drains n3, shuts it down and reaps
// the process, then requires the remaining 2-node cluster to stay fully
// functional: the departed member converges to non-alive in membership,
// fresh writes still quorum-ack, and every acked key reads back everywhere.
func TestE2ERemoveNodeLeavesHealthyCluster(t *testing.T) {
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

	// Pre-removal data: drain must hand every key to the survivors.
	ring3, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring3.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := fmt.Sprintf("e2e-remove-pre-%d", i)
		doSet(t, addrs[ring3.GetNode(key)], key, "v-"+key)
	}

	if out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "drain", "--timeout-ms", "60000"); code != 0 {
		t.Fatalf("drain exit=%d: %s", code, out)
	}
	if out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "shutdown"); code != 0 {
		t.Fatalf("shutdown exit=%d: %s", code, out)
	}
	waitUnserving(t, addrs["n3"])
	sigTerm(t, procs["n3"])

	// The survivors must converge on n3 being gone (dead, never alive
	// again). Dead entries may linger in the view; what matters is that
	// n3 is not alive anywhere new writes could route to it alone.
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n1"]), "cluster")
		if code == 0 && memberState(t, out, "n3") != "alive" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("n3 still alive after removal: %s", out)
		}
		time.Sleep(time.Second)
	}

	// Fresh writes must still quorum-ack with two members left.
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes([]string{"n1", "n2"}); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		key := fmt.Sprintf("e2e-remove-%d", i)
		doSet(t, addrs[ring.GetNode(key)], key, "v-"+key)
	}
	for i := range 10 {
		key := fmt.Sprintf("e2e-remove-%d", i)
		for _, id := range []string{"n1", "n2"} {
			if res := doGetVersioned(t, addrs[id], key); res.Status != protocol.StatusOK || string(res.Value) != "v-"+key {
				t.Fatalf("key %q missing on survivor %s: %+v", key, id, res)
			}
		}
	}
	// And the pre-removal data must have ridden the drain over too.
	for i := range 20 {
		key := fmt.Sprintf("e2e-remove-pre-%d", i)
		for _, id := range []string{"n1", "n2"} {
			if res := doGetVersioned(t, addrs[id], key); res.Status != protocol.StatusOK || string(res.Value) != "v-"+key {
				t.Fatalf("pre-removal key %q missing on survivor %s: %+v", key, id, res)
			}
		}
	}
}
