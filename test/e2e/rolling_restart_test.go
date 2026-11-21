// Package e2e launches real cinderd processes and drives chaos scenarios
// against them. Scenarios run only when CINDER_E2E=1 is set; otherwise they
// skip instantly so the default unit run stays process-free:
//
//	CINDER_E2E=1 go test ./test/e2e/ -count=1 -timeout 10m
//
// or via `just test-e2e`. Gating at runtime (rather than a build tag) keeps
// these files in every default build, so editors, `go vet ./...`, and lint
// always see them and bitrot is caught by the normal gates. Each test builds
// the daemon from the working tree, so scenarios always exercise current code.
package e2e_test

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// buildCinderd compiles the working-tree daemon once per test.
func buildCinderd(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "cinderd")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/cinderd")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build cinderd: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// proc is one supervised daemon process.
type proc struct {
	cmd *exec.Cmd
}

func startNode(t *testing.T, bin, id string, port int, peers []string, rf int) *proc {
	t.Helper()
	args := []string{
		"--port", fmt.Sprint(port),
		"--node-id", id,
		"--peers", strings.Join(peers, ","),
		"--replication-factor", fmt.Sprint(rf),
		"--consistency", "quorum",
		"--gossip-interval-ms", "100",
		"--ping-interval-ms", "100",
		"--suspect-timeout-ms", "1000",
		"--anti-entropy-interval-ms", "500",
		"--quarantine-interval-ms", "0",
		"--log-level", "warn",
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	p := &proc{cmd: cmd}
	t.Cleanup(func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			cmd.Process.Kill()
		}
		cmd.Wait()
	})
	return p
}

func (p *proc) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	if _, err := p.cmd.Process.Wait(); err != nil {
		t.Fatalf("wait after kill: %v", err)
	}
}

func waitReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if c, err := client.Dial(addr, 500*time.Millisecond); err == nil {
			res, err := c.Ping()
			c.Close()
			if err == nil && res.Status == protocol.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node at %s never became ready", addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func doSet(t *testing.T, addr, key, value string) {
	t.Helper()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(value)})
	if err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("set %q via %s: res=%+v err=%v", key, addr, res, err)
	}
}

func doGetVersioned(t *testing.T, addr, key string) protocol.Response {
	t.Helper()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)})
	if err != nil {
		t.Fatalf("get %q via %s: %v", key, addr, err)
	}
	return res
}

func doGet(t *testing.T, addr, key string) protocol.Response {
	t.Helper()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	res, err := c.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(key)})
	if err != nil {
		t.Fatalf("get %q via %s: %v", key, addr, err)
	}
	return res
}

// TestE2ERollingRestartPreservesData kills one node of a 3-node RF=3
// quorum cluster with SIGKILL, serves reads from the survivors, restarts
// the victim empty on its old port, and requires rebalance plus
// anti-entropy to repopulate it with every acknowledged value.
func TestE2ERollingRestartPreservesData(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	bin := buildCinderd(t)
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
	const keyCount = 30
	keys := make([]string, keyCount)
	for i := range keyCount {
		key := fmt.Sprintf("e2e-rr-%d", i)
		keys[i] = key
		owner := ring.GetNode(key)
		doSet(t, addrs[owner], key, "v-"+key)
	}

	// Baseline: every replica holds every key locally.
	for _, key := range keys {
		for _, id := range ids {
			if res := doGetVersioned(t, addrs[id], key); res.Status != protocol.StatusOK || string(res.Value) != "v-"+key {
				t.Fatalf("baseline key %q on %s: %+v", key, id, res)
			}
		}
	}

	// SIGKILL one node; quorum reads from the survivors must stay green.
	// n2 is killed rather than picked at random so the schedule is stable.
	procs["n2"].kill(t)
	time.Sleep(2 * time.Second) // past the 1s suspect timeout: convicted
	for _, key := range keys {
		for _, id := range []string{"n1", "n3"} {
			if res := doGet(t, addrs[id], key); res.Status != protocol.StatusOK || string(res.Value) != "v-"+key {
				t.Fatalf("key %q on survivor %s during outage: %+v", key, id, res)
			}
		}
	}

	// Restart empty on the old port; migration must repopulate it.
	start("n2")
	waitReady(t, addrs["n2"], 20*time.Second)
	deadline := time.Now().Add(45 * time.Second)
	for _, key := range keys {
		for {
			if res := doGetVersioned(t, addrs["n2"], key); res.Status == protocol.StatusOK && string(res.Value) == "v-"+key {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("restarted n2 never recovered %q", key)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Final: all keys correct on all nodes.
	for _, key := range keys {
		for _, id := range ids {
			if res := doGetVersioned(t, addrs[id], key); res.Status != protocol.StatusOK || string(res.Value) != "v-"+key {
				t.Fatalf("final key %q on %s: %+v", key, id, res)
			}
		}
	}
}
