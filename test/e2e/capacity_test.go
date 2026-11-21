package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
)

// startRaw launches a daemon with a caller-built flag list.
func startRaw(t *testing.T, bin string, args []string) *proc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
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

// TestE2ECapacityExceededIsExplicit verifies that an oversize write
// surfaces as an explicit StatusCapacityExceeded — not a crash, a hang, or
// a silent drop — with the server still responsive and previously admitted
// keys intact afterwards. Oversize is defined per shard (capacity is striped
// across shards), so one value larger than a single shard's slice is
// rejected deterministically; filling to the brim would race LRU eviction.
func TestE2ECapacityExceededIsExplicit(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	bin := buildCinderd(t)
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	startRaw(t, bin, []string{
		"--port", fmt.Sprint(port),
		"--node-id", "solo",
		"--replication-factor", "1",
		"--capacity", fmt.Sprint(1 << 20), // 16 shards x 64 KiB each
		"--log-level", "warn",
	})
	waitReady(t, addr, 20*time.Second)

	// Small keys land fine.
	for i := range 10 {
		key := fmt.Sprintf("e2e-small-%d", i)
		c, err := client.Dial(addr, 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte("v")})
		c.Close()
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("small set %q: %+v %v", key, res, err)
		}
	}

	// One value larger than a shard slice is rejected explicitly.
	big := strings.Repeat("v", 256<<10)
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("e2e-big"), Value: []byte(big)})
	c.Close()
	if err != nil {
		t.Fatalf("oversize set: %v", err)
	}
	if res.Status != protocol.StatusCapacityExceeded {
		t.Fatalf("oversize status = %v, want StatusCapacityExceeded", res.Status)
	}

	// Still alive, and the admitted prefix reads back intact.
	waitReady(t, addr, 10*time.Second)
	for i := range 10 {
		key := fmt.Sprintf("e2e-small-%d", i)
		if res := doGet(t, addr, key); res.Status != protocol.StatusOK || string(res.Value) != "v" {
			t.Fatalf("admitted key %q unreadable after overflow: %+v", key, res.Status)
		}
	}
}
