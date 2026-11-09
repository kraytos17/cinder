package tcp_test

import (
	"encoding/json"
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
	"github.com/kraytos17/cinder/internal/cluster"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

// This suite runs every case twice: once against the Go server and once
// against the real C++ cinderd binary (skipped when the binary is absent).
// Any behavioral divergence between the trees fails here first.
// Set CINDER_TEST_CINDERD_PATH to override the binary location.

func goServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(l)
	return l.Addr().String(), srv.Shutdown
}

func cppBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("CINDER_TEST_CINDERD_PATH"); p != "" {
		return p
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	return filepath.Join(root, "build", "debug", "bin", "cinderd")
}

func cppServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s (set CINDER_TEST_CINDERD_PATH)", bin)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--node-id", "node1")
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cmd.Process.Kill()
		<-done
	})
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := client.Dial(addr, 500*time.Millisecond)
		if err == nil {
			if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
				c.Close()
				return addr, func() {}
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("cinderd did not become ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// cppCluster spawns two C++ nodes with mutual static peers. The ring is
// static from config, so ownership works without waiting for gossip;
// redirect addresses may take a gossip round, and tests accept both
// redirect forms.
func cppCluster(t *testing.T) (addrs map[string]string, stop func()) {
	t.Helper()
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s (set CINDER_TEST_CINDERD_PATH)", bin)
	}
	ports := map[string]int{}
	var procs []*os.Process
	for _, id := range []string{"node1", "node2"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[id] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	start := func(id, peer string) {
		cmd := exec.Command(bin, "--port", fmt.Sprint(ports[id]), "--node-id", id, "--peers", peer)
		if err := cmd.Start(); err != nil {
			t.Skipf("cannot start cinderd: %v", err)
		}
		procs = append(procs, cmd.Process)
	}
	start("node1", fmt.Sprintf("node2@127.0.0.1:%d", ports["node2"]))
	start("node2", fmt.Sprintf("node1@127.0.0.1:%d", ports["node1"]))
	done := make(chan struct{})
	go func() {
		for _, p := range procs {
			p.Wait()
		}
		close(done)
	}()
	t.Cleanup(func() {
		for _, p := range procs {
			p.Kill()
		}
		<-done
	})
	addrs = map[string]string{}
	deadline := time.Now().Add(15 * time.Second)
	for id, port := range ports {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		for {
			c, err := client.Dial(addr, 500*time.Millisecond)
			if err == nil {
				if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
					c.Close()
					break
				}
				c.Close()
			}
			if time.Now().After(deadline) {
				t.Fatalf("cinderd %s did not become ready", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
		addrs[id] = addr
	}
	return addrs, func() {}
}

func runBoth(t *testing.T, suite func(t *testing.T, name, addr string)) {
	t.Helper()
	t.Run("go", func(t *testing.T) {
		addr, stop := goServer(t)
		defer stop()
		suite(t, "go", addr)
	})
	t.Run("cpp", func(t *testing.T) {
		addr, stop := cppServer(t)
		defer stop()
		suite(t, "cpp", addr)
	})
}

func mustStatus(t *testing.T, want protocol.Status, call func() (protocol.Response, error)) protocol.Response {
	t.Helper()
	res, err := call()
	if err != nil {
		t.Fatalf("rpc: %v", err)
	}
	if res.Status != want {
		t.Fatalf("status = %v, want %v (value %q)", res.Status, want, res.Value)
	}
	return res
}

func TestInteropPing(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Ping() })
		_ = name
	})
}

func TestInteropSetGet(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-basic-" + name
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Set(key, []byte("hello")) })
		res := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Get(key) })
		if string(res.Value) != "hello" {
			t.Fatalf("[%s] value = %q", name, res.Value)
		}
		res = mustStatus(t, protocol.StatusNotFound, func() (protocol.Response, error) { return c.Get("missing-" + name) })
		if res.HasValue {
			t.Fatalf("[%s] NotFound must carry no value", name)
		}
	})
}

func TestInteropOverwriteAndDelete(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-ow-" + name
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Set(key, []byte("v1")) })
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Set(key, []byte("v2")) })
		res := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Get(key) })
		if string(res.Value) != "v2" {
			t.Fatalf("[%s] value = %q", name, res.Value)
		}
		// DEL plants a versioned tombstone: always OK, hides the value,
		// exposes the marker on versioned reads.
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Del(key) })
		mustStatus(t, protocol.StatusNotFound, func() (protocol.Response, error) { return c.Get(key) })
		ver := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.GetVersioned(key) })
		if !ver.Tombstone {
			t.Fatalf("[%s] expected tombstone", name)
		}
	})
}

func TestInteropGetVersionedMeta(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-meta-" + name
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Set(key, []byte("v")) })
		res := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.GetVersioned(key) })
		if string(res.Value) != "v" {
			t.Fatalf("[%s] value = %q", name, res.Value)
		}
		if res.Version == 0 {
			t.Fatalf("[%s] version metadata missing: %+v", name, res)
		}
	})
}

func TestInteropTTL(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-ttl-" + name
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.SetWithTTL(key, []byte("ephemeral"), 150*time.Millisecond) })
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Get(key) })
		deadline := time.Now().Add(5 * time.Second)
		for {
			res, err := c.Get(key)
			if err != nil {
				t.Fatalf("[%s] get: %v", name, err)
			}
			if res.Status == protocol.StatusNotFound {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("[%s] key never expired", name)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

func TestInteropReplicate(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-repl-" + name
		// A primary-style write with absolute expiry lands in the store.
		_, err := c.Do(protocol.Request{
			Opcode: protocol.OpReplicate, Key: []byte(key), Value: []byte("r"),
			Version: 42, WriterNodeHash: 7,
			HasExpiresAt: true, ExpiresAt: time.Now().Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("[%s] replicate: %v", name, err)
		}
		res := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.GetVersioned(key) })
		if string(res.Value) != "r" || res.Version != 42 || res.WriterNodeHash != 7 {
			t.Fatalf("[%s] replicated entry = %+v", name, res)
		}
		// Tombstone via replicate hides the value but keeps the marker.
		_, err = c.Do(protocol.Request{
			Opcode: protocol.OpHint, Key: []byte(key),
			Version: 43, WriterNodeHash: 7, Tombstone: true,
			HasExpiresAt: true, ExpiresAt: time.Now().Add(time.Hour),
		})
		if err != nil {
			t.Fatalf("[%s] hint tombstone: %v", name, err)
		}
		mustStatus(t, protocol.StatusNotFound, func() (protocol.Response, error) { return c.Get(key) })
		tomb := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.GetVersioned(key) })
		if !tomb.Tombstone {
			t.Fatalf("[%s] expected tombstone", name)
		}
	})
}

func TestInteropAdmin(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		info := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Admin(protocol.OpAdminInfo) })
		var doc map[string]any
		if err := json.Unmarshal(info.Value, &doc); err != nil {
			t.Fatalf("[%s] admin info not JSON: %v", name, err)
		}
		if doc["node_id"] != "node1" {
			t.Fatalf("[%s] node_id = %v", name, doc["node_id"])
		}
		cluster := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Admin(protocol.OpAdminCluster) })
		var nodes []map[string]any
		if err := json.Unmarshal(cluster.Value, &nodes); err != nil || len(nodes) == 0 {
			t.Fatalf("[%s] admin cluster = %q, err %v", name, cluster.Value, err)
		}
		ring := mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Admin(protocol.OpAdminRing) })
		var ringDoc map[string]any
		if err := json.Unmarshal(ring.Value, &ringDoc); err != nil {
			t.Fatalf("[%s] admin ring not JSON: %v", name, err)
		}
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Admin(protocol.OpAdminCompact) })
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) { return c.Admin(protocol.OpAdminConfigReload) })
	})
}

func TestInteropTraceEcho(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		key := "interop-trace-" + name
		res, err := c.Do(protocol.Request{
			Opcode: protocol.OpSet, Key: []byte(key), Value: []byte("v"),
			TraceID: 777, SpanID: 888,
		})
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if res.TraceID != 777 || res.SpanID != 888 {
			t.Fatalf("[%s] trace not echoed: %+v", name, res)
		}
		// Zero trace is minted server-side and echoed back non-zero.
		res2, err := c.Do(protocol.Request{Opcode: protocol.OpPing})
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if res2.TraceID == 0 {
			t.Fatalf("[%s] server must mint trace ids", name)
		}
	})
}

func TestInteropTwoNodeCppCluster(t *testing.T) {
	addrs, _ := cppCluster(t)
	cl, err := client.NewCluster(client.ClusterConfig{
		Nodes: []client.NodeAddr{
			{ID: "node1", Host: "127.0.0.1", Port: portOf(t, addrs["node1"])},
			{ID: "node2", Host: "127.0.0.1", Port: portOf(t, addrs["node2"])},
		},
		MaxRetries:    5,
		BaseBackoffMs: 10,
		RPCTimeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	owners := map[string]int{}
	for i := range 50 {
		key := fmt.Sprintf("xnode-key-%d", i)
		if res, err := cl.Set(key, fmt.Appendf(nil, "v%d", i)); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %v %v", key, res.Status, err)
		}
		got, err := cl.Get(key)
		if err != nil || got.Status != protocol.StatusOK || string(got.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get %q: %+v %v", key, got, err)
		}
		ver, err := cl.GetVersioned(key)
		if err != nil || ver.Status != protocol.StatusOK || ver.Version == 0 {
			t.Fatalf("versioned %q: %+v %v", key, ver, err)
		}
		owners[ringOwner(t, key)]++
	}
	if len(owners) < 2 {
		t.Fatalf("keys did not spread: %v", owners)
	}

	// Direct write to the non-owner must redirect with a parseable target.
	direct := dial(t, addrs["node1"])
	foreign := ""
	for i := range 200 {
		if k := fmt.Sprintf("redirect-probe-%d", i); ringOwner(t, k) == "node2" {
			foreign = k
			break
		}
	}
	if foreign == "" {
		t.Fatal("no key owned by node2")
	}
	res, err := direct.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(foreign), Value: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != protocol.StatusNotReady {
		t.Fatalf("write to non-owner = %v, want NotReady", res.Status)
	}
	target, ok := client.ParseRedirectTarget(string(res.Value))
	if !ok || target.ID != "node2" {
		t.Fatalf("redirect = %q", res.Value)
	}
	// A missing key on the non-owner redirects the same way.
	res, err = direct.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte(foreign)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != protocol.StatusNotReady {
		t.Fatalf("read miss on non-owner = %v, want NotReady", res.Status)
	}
}

func TestInteropCppQuorumSurvivesOneFailure(t *testing.T) {
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s", bin)
	}
	ids := []string{"node1", "node2", "node3"}
	ports := map[string]int{}
	var procs []*os.Process
	for _, id := range ids {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports[id] = l.Addr().(*net.TCPAddr).Port
		l.Close()
	}
	peersFor := func(self string) string {
		var parts []string
		for _, id := range ids {
			if id != self {
				parts = append(parts, fmt.Sprintf("%s@127.0.0.1:%d", id, ports[id]))
			}
		}
		return strings.Join(parts, ",")
	}
	start := func(id string) {
		cmd := exec.Command(bin, "--port", fmt.Sprint(ports[id]), "--node-id", id,
			"--peers", peersFor(id), "--replication-factor", "2", "--consistency", "quorum")
		if err := cmd.Start(); err != nil {
			t.Skipf("cannot start cinderd: %v", err)
		}
		procs = append(procs, cmd.Process)
	}
	for _, id := range ids {
		start(id)
	}
	done := make(chan struct{})
	go func() {
		for _, p := range procs {
			p.Wait()
		}
		close(done)
	}()
	t.Cleanup(func() {
		for _, p := range procs {
			p.Kill()
		}
		<-done
	})
	addrs := map[string]string{}
	deadline := time.Now().Add(15 * time.Second)
	for _, id := range ids {
		addr := fmt.Sprintf("127.0.0.1:%d", ports[id])
		for {
			c, err := client.Dial(addr, 500*time.Millisecond)
			if err == nil {
				if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
					c.Close()
					break
				}
				c.Close()
			}
			if time.Now().After(deadline) {
				t.Fatalf("cinderd %s did not become ready", id)
			}
			time.Sleep(50 * time.Millisecond)
		}
		addrs[id] = addr
	}
	cl, err := client.NewCluster(client.ClusterConfig{
		Nodes: []client.NodeAddr{
			{ID: "node1", Host: "127.0.0.1", Port: ports["node1"]},
			{ID: "node2", Host: "127.0.0.1", Port: ports["node2"]},
			{ID: "node3", Host: "127.0.0.1", Port: ports["node3"]},
		},
		MaxRetries: 5, BaseBackoffMs: 10, RPCTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// Baseline through the Go router against C++ quorum.
	for i := range 10 {
		key := fmt.Sprintf("cppq-%d", i)
		if res, err := cl.Set(key, []byte("v")); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", key, res, err)
		}
	}
	// Kill one C++ node: quorum (W=2) must hold for surviving replica sets.
	procs[2].Kill()
	time.Sleep(300 * time.Millisecond)
	nodes, _ := testRing3.GetNodes("cppq-0", 2)
	t.Logf("replica set: %v", nodes)
	for i := range 10 {
		key := fmt.Sprintf("cppq2-%d", i)
		nodes, _ := testRing3.GetNodes(key, 2)
		dead := false
		for _, n := range nodes {
			if n == "node3" {
				dead = true
			}
		}
		if dead {
			continue
		}
		if res, err := cl.Set(key, []byte("v")); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q after kill: %+v %v", key, res, err)
		}
		if res, err := cl.Get(key); err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("get %q after kill: %+v %v", key, res, err)
		}
	}
}

func TestInteropGossipWithCpp(t *testing.T) {
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s", bin)
	}
	// Three nodes; only adjacent pairs know each other statically, so the
	// ends (C++ node1, Go gonode) can only meet through gossip relay:
	//   C++ node1 --peers node2      (knows G2 only)
	//   Go  node2 --peers node1,gonode (knows both)
	//   Go  gonode --peers node2     (knows G2 only)
	startGo := func(id string, l net.Listener, peers []tcp.Peer) (addr string, srv *tcp.Server, stop func()) {
		port := l.Addr().(*net.TCPAddr).Port
		srv = tcp.NewServer(id, port, store.NewLRUStore(1<<20, nil), nil)
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		srv.SetGossipInterval(200 * time.Millisecond)
		srv.SetProbeInterval(200 * time.Millisecond)
		srv.SetSuspectTimeout(600 * time.Millisecond)
		go srv.Serve(l)
		return l.Addr().String(), srv, srv.Shutdown
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p2 := l2.Addr().(*net.TCPAddr).Port
	l3, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p3 := l3.Addr().(*net.TCPAddr).Port

	cPort := freePort(t)
	_, srv2, stop2 := startGo("node2", l2, []tcp.Peer{{ID: "node1", Host: "127.0.0.1", Port: cPort}, {ID: "gonode", Host: "127.0.0.1", Port: p3}})
	defer stop2()
	_, srv3, stop3 := startGo("gonode", l3, []tcp.Peer{{ID: "node2", Host: "127.0.0.1", Port: p2}})
	defer stop3()

	cmd := exec.Command(bin, "--port", fmt.Sprint(cPort), "--node-id", "node1",
		"--peers", fmt.Sprintf("node2@127.0.0.1:%d", p2),
		"--gossip-interval", "200", "--ping-interval", "200", "--suspect-timeout", "600")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	done := make(chan struct{})
	go func() { cmd.Wait(); close(done) }()
	t.Cleanup(func() { cmd.Process.Kill(); <-done })
	cAddr := fmt.Sprintf("127.0.0.1:%d", cPort)
	waitReady(t, cAddr)

	// G2 must learn the C++ node (statically peered): sanity.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if info, ok := srv2.Table().Get("node1"); ok && info.State == cluster.Alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("G2 never learned the C++ peer")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// gonode can only learn node1 through G2's relayed gossip.
	deadline = time.Now().Add(20 * time.Second)
	for {
		if info, ok := srv3.Table().Get("node1"); ok && info.State == cluster.Alive && info.Host != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("gonode never learned node1 via gossip relay")
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The C++ node can only learn gonode through G2's relayed gossip.
	c := dial(t, cAddr)
	deadline = time.Now().Add(20 * time.Second)
	for {
		res, err := c.Admin(protocol.OpAdminCluster)
		if err == nil && res.Status == protocol.StatusOK {
			var nodes []map[string]any
			if err := json.Unmarshal(res.Value, &nodes); err == nil {
				for _, n := range nodes {
					if n["id"] == "gonode" {
						return
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("C++ node never learned gonode via gossip relay")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestInteropAEWithCpp exercises live anti-entropy between the Go and C++
// trees in both directions. Phase A restarts the C++ node empty (crash
// recovery must come from AE: the Go tree never auto-replays hints).
// Phase B injects disjoint keys via OpReplicate to each side (internal
// opcode: no fan-out, no hints anywhere) so AE does 100% of the repair,
// plus a tombstone that must converge to the Go side.
func TestInteropAEWithCpp(t *testing.T) {
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s", bin)
	}
	cPort := freePort(t)
	gListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gPort := gListener.Addr().(*net.TCPAddr).Port
	cAddr := fmt.Sprintf("127.0.0.1:%d", cPort)
	gAddr := gListener.Addr().String()

	mixedRing, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := mixedRing.SetNodes([]string{"node1", "gonode"}); err != nil {
		t.Fatal(err)
	}
	ownerOf := func(key string) string { return mixedRing.GetNode(key) }
	ownedBy := func(owner, prefix string, n int) []string {
		var out []string
		for i := 0; len(out) < n; i++ {
			if k := fmt.Sprintf("%s-%d", prefix, i); ownerOf(k) == owner {
				out = append(out, k)
			}
		}
		return out
	}

	var cppProc *os.Process
	startCpp := func() {
		cmd := exec.Command(bin, "--port", fmt.Sprint(cPort), "--node-id", "node1",
			"--peers", fmt.Sprintf("gonode@127.0.0.1:%d", gPort),
			"--replication-factor", "2", "--consistency", "async",
			"--anti-entropy-interval", "200", "--anti-entropy-buckets", "256",
			"--gossip-interval", "200", "--ping-interval", "200",
			"--suspect-timeout", "3600000")
		if err := cmd.Start(); err != nil {
			t.Fatalf("cannot start cinderd: %v", err)
		}
		cppProc = cmd.Process
	}
	var goSrv *tcp.Server
	var goMgr *repl.Manager
	startGo := func(l net.Listener) {
		st := store.NewLRUStore(1<<20, nil)
		srv := tcp.NewServer("gonode", l.Addr().(*net.TCPAddr).Port, st, nil)
		if err := srv.ConfigureCluster([]tcp.Peer{{ID: "node1", Host: "127.0.0.1", Port: cPort}}); err != nil {
			t.Fatal(err)
		}
		srv.SetSuspectTimeout(time.Hour)
		srv.SetGossipInterval(200 * time.Millisecond)
		srv.SetProbeInterval(200 * time.Millisecond)
		// Freeze the replay tick: phase A must converge through
		// anti-entropy only, never through hint replay.
		srv.SetReplayInterval(time.Hour)
		pool := client.NewPool([]client.NodeAddr{{ID: "node1", Host: "127.0.0.1", Port: cPort}}, 5*time.Second)
		mgr := repl.NewManager(st, "gonode", nil, pool)
		srv.ConfigureReplication(2, repl.Async, mgr)
		srv.ConfigureAntiEntropy(256, 1<<20)
		srv.SetAntiEntropyInterval(200 * time.Millisecond)
		go srv.Serve(l)
		goSrv, goMgr = srv, mgr
	}
	t.Cleanup(func() {
		if cppProc != nil {
			cppProc.Kill()
		}
		if goSrv != nil {
			goSrv.Shutdown()
		}
	})

	directSet := func(t *testing.T, addr, key, val string) {
		t.Helper()
		c := dial(t, addr)
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) {
			return c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(key), Value: []byte(val)})
		})
	}
	pollGet := func(t *testing.T, addr, key, want string, what string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			c, err := client.Dial(addr, 2*time.Second)
			if err == nil {
				// GetVersioned is strictly local on both trees (no
				// fan-out, no read repair), so convergence here proves
				// AE actually moved the key. Go's plain OpGet would fan
				// out through the replication manager and heal on read.
				res, err := c.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(key)})
				c.Close()
				if err == nil && res.Status == protocol.StatusOK && !res.Tombstone && string(res.Value) == want {
					return
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %q never converged on %s", what, key, addr)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Phase A: crash recovery Go -> C++. Kill C++, write gonode-owned keys
	// (async fan-outs fail into hints), restart C++ empty, AE converges it.
	// Hermetic: the Go replay tick is frozen above, so only AE can deliver
	// these keys.
	startCpp()
	waitReady(t, cAddr)
	startGo(gListener)
	goKeys := ownedBy("gonode", "ae-go-crash", 10)
	directSet(t, gAddr, goKeys[0], "v")
	pollGet(t, cAddr, goKeys[0], "v", "replication sanity")
	cppProc.Kill()
	cppProc = nil
	for _, k := range goKeys[1:] {
		directSet(t, gAddr, k, "v")
	}
	waitForHints(t, goMgr, len(goKeys)-1)
	startCpp()
	waitReady(t, cAddr)
	for _, k := range goKeys[1:] {
		pollGet(t, cAddr, k, "v", "AE crash recovery")
	}

	// Phase B: disjoint Replicate injection both ways. Internal opcodes apply
	// locally with no fan-out and no hints, so AE does all of the repair.
	cOnly, gOnly := ownedBy("node1", "ae-x-cpp", 10), ownedBy("gonode", "ae-x-go", 10)
	inject := func(t *testing.T, addr, key, val string, ver, writer uint64) {
		t.Helper()
		c := dial(t, addr)
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) {
			return c.Do(protocol.Request{Opcode: protocol.OpReplicate,
				Key: []byte(key), Value: []byte(val), Version: ver, WriterNodeHash: writer})
		})
	}
	for i, k := range cOnly {
		inject(t, cAddr, k, "cv", 1000+uint64(i), 111)
	}
	for i, k := range gOnly {
		inject(t, gAddr, k, "gv", 2000+uint64(i), 222)
	}
	for _, k := range cOnly {
		pollGet(t, gAddr, k, "cv", "AE C++ -> Go")
	}
	for _, k := range gOnly {
		pollGet(t, cAddr, k, "gv", "AE Go -> C++")
	}

	// Tombstone converges to the Go side through AE.
	c := dial(t, cAddr)
	mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) {
		return c.Do(protocol.Request{Opcode: protocol.OpHint,
			Key: []byte(cOnly[0]), Version: 5000, WriterNodeHash: 111, Tombstone: true})
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		gc, err := client.Dial(gAddr, 2*time.Second)
		if err == nil {
			res, err := gc.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(cOnly[0])})
			gc.Close()
			if err == nil && res.Status == protocol.StatusOK && res.Tombstone {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("tombstone for %q never reached Go", cOnly[0])
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestInteropRebalanceGoJoinsCpp starts a C++ pair, writes keys, then
// joins an empty Go node: the C++ pair must migrate the Go node's share
// to it through the regular gossip → ring → debounced-rebalance path.
func TestInteropRebalanceGoJoinsCpp(t *testing.T) {
	bin := cppBinary(t)
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("no C++ cinderd at %s", bin)
	}
	ports := map[string]int{"node1": freePort(t), "node2": freePort(t)}
	var procs []*os.Process
	startCpp := func(id string) {
		var peers []string
		for pid, p := range ports {
			if pid != id {
				peers = append(peers, fmt.Sprintf("%s@127.0.0.1:%d", pid, p))
			}
		}
		cmd := exec.Command(bin, "--port", fmt.Sprint(ports[id]), "--node-id", id,
			"--peers", strings.Join(peers, ","),
			"--replication-factor", "2", "--consistency", "async",
			"--quarantine-interval", "0",
			"--gossip-interval", "200", "--ping-interval", "200",
			"--suspect-timeout", "30000")
		if err := cmd.Start(); err != nil {
			t.Skipf("cannot start cinderd: %v", err)
		}
		procs = append(procs, cmd.Process)
	}
	startCpp("node1")
	startCpp("node2")
	t.Cleanup(func() {
		for _, p := range procs {
			p.Kill()
		}
	})
	addrs := map[string]string{}
	for id, port := range ports {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		waitReady(t, addr)
		addrs[id] = addr
	}

	// 2-node C++ ring predicts owners, exactly like the C++ tests.
	two, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := two.SetNodes([]string{"node1", "node2"}); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for i := 0; len(keys) < 30; i++ {
		k := fmt.Sprintf("rb-x-cpp-%d", i)
		owner := two.GetNode(k)
		d := dial(t, addrs[owner])
		mustStatus(t, protocol.StatusOK, func() (protocol.Response, error) {
			return d.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(k), Value: []byte("v-" + k)})
		})
		d.Close()
		keys = append(keys, k)
	}

	// Empty Go node joins with quarantine disabled.
	gListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gPort := gListener.Addr().(*net.TCPAddr).Port
	gAddr := gListener.Addr().String()
	gStore := store.NewLRUStore(1<<20, nil)
	gSrv := tcp.NewServer("gonode", gPort, gStore, nil)
	if err := gSrv.ConfigureCluster([]tcp.Peer{
		{ID: "node1", Host: "127.0.0.1", Port: ports["node1"]},
		{ID: "node2", Host: "127.0.0.1", Port: ports["node2"]},
	}); err != nil {
		t.Fatal(err)
	}
	gSrv.SetSuspectTimeout(time.Hour)
	gSrv.SetGossipInterval(200 * time.Millisecond)
	gSrv.SetProbeInterval(200 * time.Millisecond)
	gPool := client.NewPool([]client.NodeAddr{
		{ID: "node1", Host: "127.0.0.1", Port: ports["node1"]},
		{ID: "node2", Host: "127.0.0.1", Port: ports["node2"]},
	}, 5*time.Second)
	gMgr := repl.NewManager(gStore, "gonode", nil, gPool)
	gSrv.ConfigureReplication(2, repl.Async, gMgr)
	gSrv.ConfigureRebalance(2, 0)
	go gSrv.Serve(gListener)
	t.Cleanup(gSrv.Shutdown)

	// The Go node learns both C++ peers through gossip relay.
	deadline := time.Now().Add(15 * time.Second)
	for {
		a, aok := gSrv.Table().Get("node1")
		b, bok := gSrv.Table().Get("node2")
		if aok && bok && a.State == cluster.Alive && b.State == cluster.Alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("gonode never learned the C++ pair")
		}
		time.Sleep(100 * time.Millisecond)
	}

	three, err := hashring.New(hashring.DefaultVNodes, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := three.SetNodes([]string{"node1", "node2", "gonode"}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, k := range keys {
		nodes, err := three.GetNodes(k, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range nodes {
			if id == "gonode" {
				want = append(want, k)
			}
		}
	}
	if len(want) == 0 {
		t.Fatal("no key hashes into gonode's share")
	}
	deadline = time.Now().Add(30 * time.Second)
	for _, k := range want {
		for {
			// Local-only read: C++ plain Get serves the local copy or
			// redirects (no healing); Go's OpGet would fan out and
			// repair, so GetVersioned pins the assertion to migration.
			d, err := client.Dial(gAddr, 2*time.Second)
			if err == nil {
				res, err := d.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte(k)})
				d.Close()
				if err == nil && res.Status == protocol.StatusOK && !res.Tombstone && string(res.Value) == "v-"+k {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("C++ pair never migrated %q to gonode", k)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
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

func waitReady(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		c, err := client.Dial(addr, 500*time.Millisecond)
		if err == nil {
			if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
				c.Close()
				return
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become ready", addr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestInteropCountersUnsupportedOnGo(t *testing.T) {
	goAddr, stop := goServer(t)
	defer stop()
	c := dial(t, goAddr)
	res, err := c.Do(protocol.Request{Opcode: protocol.OpCounterGet, Key: []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != protocol.StatusNotSupported {
		t.Fatalf("go counter get = %v, want NotSupported (no counter store until M4)", res.Status)
	}
}

func TestInteropNegativeFraming(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		// Unknown opcode decodes to InvalidArgument, connection stays alive.
		raw, err := c.Raw([]byte{protocol.Magic, protocol.Version, 21, 0, 0, 0, 1, 0x00})
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		res, err := protocol.DecodeResponse(raw)
		if err != nil {
			t.Fatalf("[%s] %v", name, err)
		}
		if res.Status != protocol.StatusInvalidArgument {
			t.Fatalf("[%s] status = %v", name, res.Status)
		}
		// Connection must still serve after the bad frame.
		mustStatus(t, protocol.StatusOK, c.Ping)
	})
}

func TestInteropBadMagicCloses(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		if _, err := c.Raw([]byte{0x00, protocol.Version, 4, 0, 0, 0, 1, 0x00}); err == nil {
			t.Fatalf("[%s] bad magic must close the connection", name)
		}
	})
}

func TestInteropOversizedCloses(t *testing.T) {
	runBoth(t, func(t *testing.T, name, addr string) {
		c := dial(t, addr)
		// 2 MiB payload exceeds the 1 MiB connection buffer: close, no response.
		hdr := []byte{protocol.Magic, protocol.Version, 4, 0x00, 0x20, 0x00, 0x00}
		big := append(hdr, make([]byte, 2<<20)...)
		if _, err := c.Raw(big); err == nil {
			t.Fatalf("[%s] oversized frame must close the connection", name)
		}
	})
}
