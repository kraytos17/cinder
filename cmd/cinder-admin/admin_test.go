package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
)

func testServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return l.Addr().String(), srv.Shutdown
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var p int
	fmt.Sscanf(port, "%d", &p)
	return p
}

func runAdmin(t *testing.T, addr string, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"--port", fmt.Sprint(portOf(t, addr))}, args...)
	var out, errB strings.Builder
	code := run(full, &out, &errB)
	return out.String(), errB.String(), code
}

func TestAdminInfo(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "info")
	if !strings.Contains(out, `"node_id":"node1"`) || errOut != "" || code != 0 {
		t.Fatalf("info: out=%q err=%q code=%d", out, errOut, code)
	}
	// A standalone server with no cluster to join reports ready.
	if !strings.Contains(out, `"ready":true`) {
		t.Fatalf("info missing ready:true: out=%q", out)
	}
}

func TestAdminCluster(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "cluster")
	if !strings.Contains(out, "node1") || errOut != "" || code != 0 {
		t.Fatalf("cluster: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestAdminRing(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "ring")
	if !strings.Contains(out, "vnodes_per_node") || errOut != "" || code != 0 {
		t.Fatalf("ring: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestAdminCompactAndReload(t *testing.T) {
	addr, _ := testServer(t)
	if out, errOut, code := runAdmin(t, addr, "compact"); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("compact: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, errOut, code := runAdmin(t, addr, "config-reload"); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("config-reload: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestAdminShutdown(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "shutdown")
	if out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("shutdown: out=%q err=%q code=%d", out, errOut, code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var outB, errB strings.Builder
		if c := run([]string{"--port", fmt.Sprint(portOf(t, addr)), "ping"}, &outB, &errB); c != 0 {
			return // server is down
		}
		if time.Now().After(deadline) {
			t.Fatal("server still up after admin shutdown")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAdminUnknownCommand(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "defrag")
	if out != "" || !strings.Contains(errOut, "unknown command") || code == 0 {
		t.Fatalf("unknown: out=%q err=%q code=%d", out, errOut, code)
	}
}

// Draining a lone replica is refused: the CLI surfaces the server's
// error body and exits non-zero instead of hanging on progress.
func TestAdminDrainRefused(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runAdmin(t, addr, "drain", "--timeout-ms", "1000")
	if code == 0 {
		t.Fatalf("drain on RF=1: out=%q err=%q code=%d", out, errOut, code)
	}
	if !strings.Contains(errOut, "drain requires replication") {
		t.Fatalf("drain refusal: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestAdminHelpAndVersion(t *testing.T) {
	var out, errOut strings.Builder
	if code := run([]string{"--help"}, &out, &errOut); code != 0 {
		t.Fatalf("help exit=%d stderr=%q", code, errOut.String())
	}
	for _, want := range []string{"info", "cluster", "compact", "shutdown", "drain", "--client-secret", "--client-secret-admin"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help missing %q", want)
		}
	}
	out.Reset()
	errOut.Reset()
	if code := run([]string{"--version"}, &out, &errOut); code != 0 || !strings.Contains(out.String(), "cinder-admin (go)") {
		t.Fatalf("version exit=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func testTLSRoleServer(t *testing.T) string {
	t.Helper()
	fx := "../../tests/fixtures"
	if _, err := os.Stat(fx + "/ca.pem"); err != nil {
		t.Skipf("no fixtures at %s", fx)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureSecrets("", "client-secret"); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureAdminSecret("admin-secret"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return l.Addr().String()
}

func runTLSAdmin(t *testing.T, addr string, args ...string) (string, string, int) {
	t.Helper()
	fx := "../../tests/fixtures"
	full := append([]string{"--port", fmt.Sprint(portOf(t, addr)), "--tls", "--tls-ca", fx + "/ca.pem"}, args...)
	var out, errB strings.Builder
	code := run(full, &out, &errB)
	return out.String(), errB.String(), code
}

// With split roles, mutating admin requires the admin bearer while
// read-only admin works on either bearer.
func TestAdminRoleSplitEndToEnd(t *testing.T) {
	addr := testTLSRoleServer(t)
	if out, _, code := runTLSAdmin(t, addr, "--client-secret", "client-secret", "compact"); code == 0 {
		t.Fatalf("compact with data secret must be denied: %q", out)
	}
	if out, errOut, code := runTLSAdmin(t, addr, "--client-secret-admin", "admin-secret", "compact"); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("compact with admin secret: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, errOut, code := runTLSAdmin(t, addr, "--client-secret", "client-secret", "info"); code != 0 || errOut != "" || !strings.Contains(out, `"node_id":"node1"`) {
		t.Fatalf("info with data secret: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, errOut, code := runTLSAdmin(t, addr, "--client-secret-admin", "admin-secret", "info"); code != 0 || errOut != "" || !strings.Contains(out, `"node_id":"node1"`) {
		t.Fatalf("info with admin secret: out=%q err=%q code=%d", out, errOut, code)
	}
}
