package e2e_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

func buildCinderAdmin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "cinder-admin")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/cinder-admin")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build cinder-admin: %v\n%s", err, out)
	}
	return bin
}

func runAdmin(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := errors.AsType[*exec.ExitError](err); ok {
			code = ee.ExitCode()
		} else {
			t.Fatalf("run %v: %v", args, err)
		}
	}
	return string(out), code
}

// TestE2EDrainUnderLoad starts a 3-node RF=3 quorum cluster under a
// continuous write load, drains n3 through the real cinder-admin CLI,
// and requires: drain exits 0, every acknowledged write lands on the
// survivors, and a follow-up shutdown exits 0 with the cluster still
// serving.
func TestE2EDrainUnderLoad(t *testing.T) {
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

	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		key := fmt.Sprintf("e2e-drain-canary-%d", i)
		doSet(t, addrs[ring.GetNode(key)], key, "v-"+key)
	}

	w := startWriter(addrs, ring, "e2e-drain")
	time.Sleep(1500 * time.Millisecond)

	out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "drain", "--timeout-ms", "60000")
	if code != 0 {
		t.Fatalf("drain exit=%d: %s", code, out)
	}
	t.Logf("drain output:\n%s", out)
	w.finish()
	if w.acked.Load() == 0 {
		t.Fatal("no write acknowledged during drain")
	}

	// Every acknowledged write must be present on both survivors: with
	// only two members left, each holds every key.
	acked := w.log.snapshot()
	deadline := time.Now().Add(60 * time.Second)
	for key, want := range acked {
		for _, id := range []string{"n1", "n2"} {
			for {
				res := doGetVersioned(t, addrs[id], key)
				if res.Status == protocol.StatusOK && string(res.Value) == want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("acked %q missing/wrong on survivor %s: %+v", key, id, res)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}

	// Finish via the regular shutdown path: admin shutdown stops the
	// node serving, then SIGTERM reaps the process. Confirm the
	// survivors still serve afterwards.
	if out, code := runAdmin(t, adminBin, "--port", fmt.Sprint(ports["n3"]), "shutdown"); code != 0 {
		t.Fatalf("shutdown exit=%d: %s", code, out)
	}
	unserving := time.Now().Add(10 * time.Second)
	for {
		c, err := client.Dial(addrs["n3"], 500*time.Millisecond)
		if err != nil {
			break
		}
		res, err := c.Ping()
		c.Close()
		if err != nil || res.Status != protocol.StatusOK {
			break
		}
		if time.Now().After(unserving) {
			t.Fatal("drained node still serving after admin shutdown")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := procs["n3"].cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- procs["n3"].cmd.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("drained node exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("drained node did not exit after SIGTERM")
	}
	if res := doGet(t, addrs["n1"], "e2e-drain-canary-0"); res.Status != protocol.StatusOK {
		t.Fatalf("survivor not serving after drain+shutdown: %+v", res)
	}
}
