package e2e_test

import (
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// TestE2ERestartReportsReady exercises the rolling-restart procedure from
// OPERATIONS.md against live processes: restart a node under continuous
// load, poll admin info until it reports ready, and require every canary to
// converge on all members afterwards.
func TestE2ERestartReportsReady(t *testing.T) {
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
	start := func(id string) {
		procs[id] = startNode(t, bin, id, ports[id], peersOf(id), 3)
	}
	for _, id := range ids {
		start(id)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := fmt.Sprintf("e2e-restart-canary-%d", i)
		doSet(t, addrs[ring.GetNode(key)], key, "canary-"+key)
	}

	w := startWriter(addrs, ring, "e2e-rs")
	time.Sleep(1500 * time.Millisecond)

	// Graceful restart of n3: SIGTERM, then same port rejoin with the
	// default 10s quarantine so the ready gate exercises the real
	// warming window (the shared helper disables it for speed).
	if err := procs["n3"].cmd.Process.Signal(syscall.SIGTERM); err != nil {
		w.finish()
		t.Fatalf("SIGTERM: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- procs["n3"].cmd.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			w.finish()
			t.Fatalf("n3 exit: %v", err)
		}
	case <-time.After(15 * time.Second):
		w.finish()
		t.Fatal("n3 did not exit after SIGTERM")
	}
	procs["n3"] = startRaw(t, bin, []string{
		"--port", fmt.Sprint(ports["n3"]),
		"--node-id", "n3",
		"--peers", strings.Join(peersOf("n3"), ","),
		"--replication-factor", "3",
		"--consistency", "quorum",
		"--gossip-interval-ms", "100",
		"--ping-interval-ms", "100",
		"--suspect-timeout-ms", "1000",
		"--anti-entropy-interval-ms", "500",
		"--log-level", "warn",
	})
	waitReady(t, addrs["n3"], 20*time.Second)
	// The restarted node must report ready once recovered, rejoined, and
	// past its warming window — gate taken straight from the runbook.
	deadline := time.Now().Add(60 * time.Second)
	for {
		out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "info")
		if code == 0 && strings.Contains(out, `"ready":true`) {
			break
		}
		if time.Now().After(deadline) {
			w.finish()
			t.Fatalf("restarted n3 never reported ready (last: %q code=%d)", out, code)
		}
		time.Sleep(time.Second)
	}
	w.finish()
	if w.acked.Load() == 0 {
		t.Fatal("no write acknowledged during restart")
	}

	// Every canary must have converged back onto all three members.
	cdeadline := time.Now().Add(60 * time.Second)
	for i := range 20 {
		key := fmt.Sprintf("e2e-restart-canary-%d", i)
		for _, id := range ids {
			for {
				if res := doGetVersioned(t, addrs[id], key); res.Status == protocol.StatusOK && string(res.Value) == "canary-"+key {
					break
				}
				if time.Now().After(cdeadline) {
					t.Fatalf("canary %q missing on %s after restart", key, id)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}
