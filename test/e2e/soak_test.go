package e2e_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/load"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/netport"
)

// soakCfg bounds the run. CINDER_SOAK_SECONDS scales every phase;
// the default is a short in-tree smoke, nightly CI raises it.
type soakCfg struct {
	steady time.Duration
	chaos  time.Duration
	settle time.Duration
}

func soakConfig() soakCfg {
	total := 150 * time.Second
	if v := os.Getenv("CINDER_SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 60 {
			total = time.Duration(n) * time.Second
		}
	}
	return soakCfg{
		steady: total * 30 / 100,
		chaos:  total * 50 / 100,
		settle: total - total*30/100 - total*50/100,
	}
}

// procStats samples RSS (KiB), open FDs, and thread count via /proc.
// ok is false off Linux or when the process is gone.
func procStats(pid int) (rssKB uint64, fds, tasks int, ok bool) {
	if runtime.GOOS != "linux" {
		return 0, 0, 0, false
	}
	status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, 0, false
	}
	for line := range strings.SplitSeq(string(status), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				if v, err := strconv.ParseUint(f[1], 10, 64); err == nil {
					rssKB = v
				}
			}
		}
	}
	fdEntries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return 0, 0, 0, false
	}
	taskEntries, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", pid))
	if err != nil {
		return 0, 0, 0, false
	}
	return rssKB, len(fdEntries), len(taskEntries), true
}

func scrapeCounter(t *testing.T, addr, metric string) float64 {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape %s: %v", addr, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	for line := range strings.SplitSeq(string(body), "\n") {
		if strings.HasPrefix(line, metric+" ") {
			f := strings.Fields(line)
			if len(f) == 2 {
				v, _ := strconv.ParseFloat(f[1], 64)
				return v
			}
		}
	}
	return 0
}

// TestE2ESoakShort runs steady load, then load under periodic chaos
// (kill/restart with disk recovery, freeze/thaw past the suspect timeout),
// then settles and checks canary keys, hint drainage, and resource bounds.
// Short by default; set CINDER_SOAK_SECONDS for longer runs.
func TestE2ESoakShort(t *testing.T) {
	if os.Getenv("CINDER_E2E") == "" {
		t.Skip("set CINDER_E2E=1 to run real-process e2e (builds cinderd, binds loopback ports)")
	}
	cfg := soakConfig()
	t.Logf("soak phases: steady=%v chaos=%v settle=%v", cfg.steady, cfg.chaos, cfg.settle)

	bin := buildCinderd(t)
	ids := []string{"n1", "n2", "n3"}
	ports, mports := map[string]int{}, map[string]int{}
	dirs := map[string]string{}
	for _, id := range ids {
		ports[id] = freePort(t)
		mports[id] = freePort(t)
		dirs[id] = t.TempDir()
	}
	addrs := map[string]string{}
	maddrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = fmt.Sprintf("127.0.0.1:%d", ports[id])
		maddrs[id] = fmt.Sprintf("127.0.0.1:%d", mports[id])
	}
	peersOf := func(self string, members []string) []string {
		var peers []string
		for _, other := range members {
			if other != self {
				peers = append(peers, fmt.Sprintf("%s@%s", other, addrs[other]))
			}
		}
		return peers
	}
	procs := map[string]*proc{}
	start := func(id string) {
		procs[id] = startRaw(t, bin, []string{
			"--port", fmt.Sprint(ports[id]),
			"--node-id", id,
			"--peers", strings.Join(peersOf(id, ids), ","),
			"--replication-factor", "3",
			"--consistency", "quorum",
			"--gossip-interval-ms", "100",
			"--ping-interval-ms", "100",
			"--suspect-timeout-ms", "1000",
			"--anti-entropy-interval-ms", "500",
			"--quarantine-interval-ms", "0",
			"--metrics-port", fmt.Sprint(mports[id]),
			"--enable-persistence",
			"--data-dir", dirs[id],
			"--fsync", "always",
			"--log-level", "warn",
		})
	}
	for _, id := range ids {
		start(id)
	}
	for _, id := range ids {
		waitReady(t, addrs[id], 20*time.Second)
	}

	addrsList := func() []client.NodeAddr {
		out := make([]client.NodeAddr, 0, len(ids))
		for _, id := range ids {
			out = append(out, client.NodeAddr{ID: id, Host: "127.0.0.1", Port: mustPort(t, addrs[id])})
		}
		return out
	}()

	// Canaries are written once, snapshotted to disk, then must survive
	// everything below.
	ring, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := ring.SetNodes(ids); err != nil {
		t.Fatal(err)
	}
	const canaryCount = 20
	for i := range canaryCount {
		key := fmt.Sprintf("e2e-soak-canary-%d", i)
		doSet(t, addrs[ring.GetNode(key)], key, "canary-"+key)
	}
	for _, id := range ids {
		c, err := client.Dial(addrs[id], 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if res, err := c.Admin(protocol.OpAdminCompact); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("compact %s: %+v %v", id, res, err)
		}
		c.Close()
	}

	sample := func(phase string) (rss map[string]uint64, fds map[string]int) {
		rss, fds = map[string]uint64{}, map[string]int{}
		for _, id := range ids {
			pid := procs[id].cmd.Process.Pid
			r, f, _, ok := procStats(pid)
			if !ok {
				continue
			}
			rss[id], fds[id] = r, f
		}
		t.Logf("sample[%s] rssKiB=%v fds=%v", phase, rss, fds)
		return rss, fds
	}
	startRSS, startFDs := sample("start")

	loadCfg := load.Config{
		Nodes: addrsList, MaxRetries: 5,
		Workers: 4, KeySpace: 2000, HotKeys: 20, HotFraction: 0.3,
		ValueSize: 256, Mix: load.Mix{Get: 90, Set: 9, Del: 1}, Seed: 7,
	}

	// Phase 1: steady load, nearly error-free.
	ctx1, cancel1 := context.WithTimeout(context.Background(), cfg.steady)
	res1, err := load.Run(ctx1, loadCfg)
	cancel1()
	if err != nil {
		t.Fatalf("steady load: %v", err)
	}
	t.Logf("steady: ops=%d throughput=%.0f/s p50get=%dns p99get=%dns p99set=%dns errors=%d",
		res1.Ops, res1.Throughput(),
		res1.Gets.Quantile(0.5), res1.Gets.Quantile(0.99),
		res1.Sets.Quantile(0.99), res1.Errors.Load())
	if res1.Ops == 0 {
		t.Fatal("steady load drove no ops")
	}
	if errRate := float64(res1.Errors.Load()) / float64(res1.Ops); errRate > 0.01 {
		t.Fatalf("steady error rate %.3f exceeds 1%%", errRate)
	}

	// Phase 2: load under chaos.
	ctx2, cancel2 := context.WithTimeout(context.Background(), cfg.chaos)
	done := make(chan *load.Result, 1)
	go func() {
		res, err := load.Run(ctx2, loadCfg)
		if err != nil {
			t.Errorf("chaos load: %v", err)
		}
		done <- res
	}()
	time.Sleep(cfg.chaos / 4)
	procs["n2"].kill(t) // SIGKILL with disk recovery on restart
	time.Sleep(cfg.chaos / 4)
	start("n2")
	waitReady(t, addrs["n2"], 20*time.Second)
	procs["n3"].stop(t) // freeze past the suspect timeout
	time.Sleep(3 * time.Second)
	procs["n3"].cont(t)
	waitReady(t, addrs["n3"], 20*time.Second)
	res2 := <-done
	cancel2()
	t.Logf("chaos: ops=%d throughput=%.0f/s p99get=%dns p99set=%dns errors=%d firstErr=%v",
		res2.Ops, res2.Throughput(),
		res2.Gets.Quantile(0.99), res2.Sets.Quantile(0.99),
		res2.Errors.Load(), res2.FirstErr.Load())
	if res2.Ops == 0 {
		t.Fatal("chaos load drove no ops")
	}

	// Phase 3: settle, then verify canaries, hint drainage, resources.
	time.Sleep(cfg.settle)
	for i := range canaryCount {
		key := fmt.Sprintf("e2e-soak-canary-%d", i)
		for _, id := range ids {
			deadline := time.Now().Add(30 * time.Second)
			for {
				if res := doGetVersioned(t, addrs[id], key); res.Status == protocol.StatusOK && string(res.Value) == "canary-"+key {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("canary %q missing on %s", key, id)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
	snapHints := func() (enq, rep, drp float64) {
		for _, id := range ids {
			enq += scrapeCounter(t, maddrs[id], "cinder_replication_hints_enqueued_total")
			rep += scrapeCounter(t, maddrs[id], "cinder_replication_hints_replayed_total")
			drp += scrapeCounter(t, maddrs[id], "cinder_replication_hints_dropped_total")
		}
		return enq, rep, drp
	}
	enq0, rep0, drp0 := snapHints()
	// No client traffic has run since settle began, so a healthy cluster
	// queues nothing new: every send succeeds. (Cumulative counters also
	// retain hints that aged out via TTL, so compare deltas, not totals.)
	time.Sleep(3 * time.Second)
	enq1, rep1, drp1 := snapHints()
	t.Logf("hints enq/rep/drp first=%.0f/%.0f/%.0f then=%.0f/%.0f/%.0f", enq0, rep0, drp0, enq1, rep1, drp1)
	if enq1 != enq0 {
		t.Fatalf("hints still accruing with no client traffic: %.0f -> %.0f", enq0, enq1)
	}
	endRSS, endFDs := sample("end")
	for _, id := range ids {
		if s, ok := startRSS[id]; ok {
			if r := endRSS[id]; r > uint64(float64(s)*1.5)+50*1024 {
				t.Fatalf("node %s RSS grew %d -> %d KiB", id, s, r)
			}
		}
		if s, ok := startFDs[id]; ok {
			// Restarted and thawed nodes legitimately hold a few extra
			// connections while pools redial; bound the growth, don't
			// require exact flatness.
			if d := endFDs[id]; d > s+30 {
				t.Fatalf("node %s FDs grew %d -> %d", id, s, d)
			}
		}
	}
}

func mustPort(t *testing.T, addr string) netport.Port {
	t.Helper()
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(p, "%d", &port); err != nil {
		t.Fatal(err)
	}
	return netport.Port(port)
}
