package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

// testServer spawns an in-process Go server; run() talks to it over TCP
// exactly like the installed binary would.
func testServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<20, nil), nil)
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

func runCLI(t *testing.T, addr string, args ...string) (string, string, int) {
	t.Helper()
	full := append([]string{"--port", fmt.Sprint(portOf(t, addr))}, args...)
	var out, errB strings.Builder
	code := run(full, &out, &errB)
	return out.String(), errB.String(), code
}

func TestCliPing(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runCLI(t, addr, "ping")
	if out != "pong\n" || errOut != "" || code != 0 {
		t.Fatalf("ping: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestCliSetGet(t *testing.T) {
	addr, _ := testServer(t)
	if out, errOut, code := runCLI(t, addr, "set", "greeting", "hello"); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("set: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, errOut, code := runCLI(t, addr, "get", "greeting"); out != "hello\n" || errOut != "" || code != 0 {
		t.Fatalf("get: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestCliGetNotFound(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runCLI(t, addr, "get", "nope")
	if !strings.Contains(out, "not found") || errOut != "" || code != 0 {
		t.Fatalf("miss: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestCliConnectRefused(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := l.Addr().String()
	l.Close()
	out, errOut, code := runCLI(t, dead, "ping")
	if !strings.Contains(strings.ToLower(errOut), "connection refused") || out != "" || code == 0 {
		t.Fatalf("refused: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestCliDel(t *testing.T) {
	addr, _ := testServer(t)
	if out, _, _ := runCLI(t, addr, "set", "temp", "x"); out != "OK\n" {
		t.Fatalf("set: %q", out)
	}
	if out, _, code := runCLI(t, addr, "del", "temp"); out != "OK\n" || code != 0 {
		t.Fatalf("del: out=%q code=%d", out, code)
	}
}

func TestCliMget(t *testing.T) {
	addr, _ := testServer(t)
	runCLI(t, addr, "set", "a", "1")
	runCLI(t, addr, "set", "b", "2")
	out, errOut, code := runCLI(t, addr, "mget", "a", "b", "missing")
	if !strings.Contains(out, "a: 1\n") || !strings.Contains(out, "b: 2\n") || !strings.Contains(out, "missing") {
		t.Fatalf("mget: %q", out)
	}
	if errOut != "" || code == 0 {
		t.Fatalf("mget with a miss must exit nonzero: err=%q code=%d", errOut, code)
	}
}

func TestCliMset(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runCLI(t, addr, "mset", "a", "1", "b", "2")
	if !strings.Contains(out, "a: OK\n") || !strings.Contains(out, "b: OK\n") || errOut != "" || code != 0 {
		t.Fatalf("mset: out=%q err=%q code=%d", out, errOut, code)
	}
	out, _, _ = runCLI(t, addr, "mget", "a", "b")
	if !strings.Contains(out, "a: 1\n") || !strings.Contains(out, "b: 2\n") {
		t.Fatalf("mget after mset: %q", out)
	}
	// Odd arg count is rejected.
	out, errOut, code = runCLI(t, addr, "mset", "a")
	if out != "" || !strings.Contains(errOut, "mset requires key/value pairs") || code == 0 {
		t.Fatalf("mset odd: out=%q err=%q code=%d", out, errOut, code)
	}
	// TTL applies to every pair (flag after positionals, like C++).
	out, errOut, code = runCLI(t, addr, "mset", "c", "3", "--ttl", "60000")
	if !strings.Contains(out, "c: OK\n") || errOut != "" || code != 0 {
		t.Fatalf("mset ttl: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, _, _ = runCLI(t, addr, "get", "c"); !strings.Contains(out, "3\n") {
		t.Fatalf("get ttl key: %q", out)
	}
}

func TestCliGetVersioned(t *testing.T) {
	addr, _ := testServer(t)
	runCLI(t, addr, "set", "k", "v")
	out, errOut, code := runCLI(t, addr, "get-versioned", "k")
	if errOut != "" || code != 0 {
		t.Fatalf("get-versioned: err=%q code=%d", errOut, code)
	}
	if !strings.Contains(out, "v\n") || !strings.Contains(out, "version: ") || !strings.Contains(out, "writer: ") {
		t.Fatalf("get-versioned: %q", out)
	}
	v1 := parseVersion(t, out)
	runCLI(t, addr, "set", "k", "v2")
	out, _, _ = runCLI(t, addr, "get-versioned", "k")
	if !strings.Contains(out, "v2\n") {
		t.Fatalf("get-versioned v2: %q", out)
	}
	if v2 := parseVersion(t, out); v2 <= v1 {
		t.Fatalf("versions not monotonic: %d <= %d", v2, v1)
	}
	out, errOut, _ = runCLI(t, addr, "get-versioned", "nope")
	if !strings.Contains(out, "not found") || errOut != "" {
		t.Fatalf("versioned miss: out=%q err=%q", out, errOut)
	}
	runCLI(t, addr, "set", "t", "x", "--ttl", "60000")
	out, _, _ = runCLI(t, addr, "get-versioned", "t")
	if !strings.Contains(out, "x\n") || !strings.Contains(out, "expires_at: ") {
		t.Fatalf("ttl versioned: %q", out)
	}
}

func parseVersion(t *testing.T, out string) uint64 {
	t.Helper()
	idx := strings.Index(out, "version: ")
	if idx < 0 {
		t.Fatalf("no version in %q", out)
	}
	var v uint64
	fmt.Sscanf(out[idx:], "version: %d", &v)
	return v
}

func TestCliInfoClusterRing(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runCLI(t, addr, "info")
	if !strings.Contains(out, `"node_id":"node1"`) || errOut != "" || code != 0 {
		t.Fatalf("info: out=%q err=%q code=%d", out, errOut, code)
	}
	out, _, _ = runCLI(t, addr, "cluster")
	if !strings.Contains(out, "node1") {
		t.Fatalf("cluster: %q", out)
	}
	out, _, _ = runCLI(t, addr, "ring")
	if !strings.Contains(out, "vnodes_per_node") {
		t.Fatalf("ring: %q", out)
	}
}

func TestCliUnknownCommand(t *testing.T) {
	addr, _ := testServer(t)
	out, errOut, code := runCLI(t, addr, "frobnicate")
	if out != "" || !strings.Contains(errOut, "unknown command") || code == 0 {
		t.Fatalf("unknown: out=%q err=%q code=%d", out, errOut, code)
	}
}

func TestCliVersion(t *testing.T) {
	var out, errB strings.Builder
	if code := run([]string{"--version"}, &out, &errB); code != 0 {
		t.Fatalf("version exit %d", code)
	}
	if !strings.Contains(out.String(), "cinder-cli") {
		t.Fatalf("version: %q", out.String())
	}
}

// Go CLI against a Go TLS server (fixture certs): exercises the --tls
// path end to end.
func TestCliTLS(t *testing.T) {
	fx := "../../tests/fixtures"
	if _, err := os.Stat(fx + "/ca.pem"); err != nil {
		t.Skipf("no fixtures at %s", fx)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<20, nil), nil)
	if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	addr := l.Addr().String()
	tlsFlags := []string{"--tls", "--tls-ca", fx + "/ca.pem"}
	if out, errOut, code := runCLI(t, addr, append(tlsFlags, "ping")...); out != "pong\n" || errOut != "" || code != 0 {
		t.Fatalf("tls ping: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, _, code := runCLI(t, addr, append(tlsFlags, "set", "k", "v")...); out != "OK\n" || code != 0 {
		t.Fatalf("tls set: out=%q code=%d", out, code)
	}
	if out, _, _ := runCLI(t, addr, append(tlsFlags, "get", "k")...); out != "v\n" {
		t.Fatalf("tls get: %q", out)
	}
	// Plaintext CLI against a TLS server fails.
	if _, errOut, code := runCLI(t, addr, "ping"); code == 0 || errOut == "" {
		t.Fatalf("plaintext to TLS server: code=%d err=%q", code, errOut)
	}
}

// TestCliClientSecret exercises --client-secret against a TLS server with
// client auth: missing/wrong secrets are denied, the right one works.
func TestCliClientSecret(t *testing.T) {
	fx := "../../tests/fixtures"
	if _, err := os.Stat(fx + "/ca.pem"); err != nil {
		t.Skipf("no fixtures at %s", fx)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer("node1", l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<20, nil), nil)
	if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureSecrets("", "topsecret"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	addr := l.Addr().String()
	base := []string{"--tls", "--tls-ca", fx + "/ca.pem"}
	if out, errOut, code := runCLI(t, addr, append(base, "ping")...); code == 0 || !strings.Contains(errOut, "permission denied") || out != "" {
		t.Fatalf("missing secret: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, _, code := runCLI(t, addr, append(append(base, "--client-secret", "wrong"), "ping")...); code == 0 || out != "" {
		t.Fatalf("wrong secret: out=%q code=%d", out, code)
	}
	ok := append(base, "--client-secret", "topsecret")
	if out, errOut, code := runCLI(t, addr, append(ok, "set", "k", "v")...); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("good secret set: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, _, _ := runCLI(t, addr, append(ok, "get", "k")...); out != "v\n" {
		t.Fatalf("good secret get: %q", out)
	}
}

// Go CLI against the real C++ server: the v6 wire contract holds both ways.
func TestCliAgainstCpp(t *testing.T) {
	cppAddr := os.Getenv("CINDER_TEST_CPP_ADDR")
	if cppAddr == "" {
		t.Skip("set CINDER_TEST_CPP_ADDR=host:port to test the Go CLI against C++ cinderd")
	}
	if out, errOut, code := runCLI(t, cppAddr, "ping"); out != "pong\n" || errOut != "" || code != 0 {
		t.Fatalf("ping: out=%q err=%q code=%d", out, errOut, code)
	}
	key := fmt.Sprintf("go-cli-%d", time.Now().UnixNano())
	if out, _, code := runCLI(t, cppAddr, "set", key, "v"); out != "OK\n" || code != 0 {
		t.Fatalf("set: out=%q code=%d", out, code)
	}
	if out, _, _ := runCLI(t, cppAddr, "get", key); out != "v\n" {
		t.Fatalf("get: %q", out)
	}
}

func TestCliCounterRoundTrip(t *testing.T) {
	addr, _ := testServer(t)
	if out, errOut, code := runCLI(t, addr, "counter-get", "c"); !strings.Contains(out, "not found") || errOut != "" || code != 0 {
		t.Fatalf("cget miss: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, errOut, code := runCLI(t, addr, "counter-incr", "c", "5"); out != "OK\n" || errOut != "" || code != 0 {
		t.Fatalf("incr: out=%q err=%q code=%d", out, errOut, code)
	}
	if out, _, _ := runCLI(t, addr, "counter-get", "c"); out != "5\n" {
		t.Fatalf("cget: %q, want 5", out)
	}
	if out, _, code := runCLI(t, addr, "counter-decr", "c", "2"); out != "OK\n" || code != 0 {
		t.Fatalf("decr: out=%q code=%d", out, code)
	}
	if out, _, _ := runCLI(t, addr, "counter-get", "c"); out != "3\n" {
		t.Fatalf("cget after decr: %q, want 3", out)
	}
	// Default delta is 1.
	if out, _, code := runCLI(t, addr, "counter-incr", "d"); out != "OK\n" || code != 0 {
		t.Fatalf("incr default: out=%q code=%d", out, code)
	}
	if out, _, _ := runCLI(t, addr, "counter-get", "d"); out != "1\n" {
		t.Fatalf("cget default: %q, want 1", out)
	}
}

func TestCliCounterBadArgs(t *testing.T) {
	addr, _ := testServer(t)
	if _, _, code := runCLI(t, addr, "counter-incr"); code == 0 {
		t.Fatal("incr without key must fail")
	}
	if _, _, code := runCLI(t, addr, "counter-decr", "k"); code != 0 {
		t.Fatal("decr without delta must default to 1")
	}
	if _, errOut, code := runCLI(t, addr, "counter-incr", "k", "nope"); code == 0 || !strings.Contains(errOut, "invalid delta") {
		t.Fatalf("bad delta: code=%d err=%q", code, errOut)
	}
	if _, errOut, code := runCLI(t, addr, "counter-incr", "k", "-3"); code == 0 || !strings.Contains(errOut, "invalid delta") {
		t.Fatalf("negative delta: code=%d err=%q", code, errOut)
	}
	if _, _, code := runCLI(t, addr, "counter-get"); code == 0 {
		t.Fatal("cget without key must fail")
	}
}
