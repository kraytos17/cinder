package grpcgw_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/kraytos17/cinder/internal/auth"
	"github.com/kraytos17/cinder/internal/grpcgw"
	gen "github.com/kraytos17/cinder/internal/grpcgw/gen"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/store"
)

var (
	OK       = gen.StatusCode_STATUS_CODE_UNSPECIFIED
	NotFound = gen.StatusCode_STATUS_CODE_NOT_FOUND
	NotReady = gen.StatusCode_STATUS_CODE_NOT_READY
)

func startGateway(t testing.TB, id string) (gen.CinderCacheServiceClient, func()) {
	t.Helper()
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer(id, tl.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(tl)
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := grpcgw.New(srv)
	go gw.Serve(gl)
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		gw.Stop()
		srv.Shutdown()
	})
	return gen.NewCinderCacheServiceClient(conn), func() {}
}

// startAuthedGateway is startGateway with a client secret configured;
// callers attach credentials via bearerCtx (or nothing, to test denial).
func startAuthedGateway(t testing.TB, secret string) gen.CinderCacheServiceClient {
	t.Helper()
	fx := fixturesDir(t)
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", tl.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(tl)
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := grpcgw.New(srv)
	gw.SetClientSecret(secret)
	if err := gw.SetTLS(filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem")); err != nil {
		t.Fatal(err)
	}
	go gw.Serve(gl)
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(tlsClientConfig(t, filepath.Join(fx, "ca.pem"))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
		gw.Stop()
		srv.Shutdown()
	})
	c := gen.NewCinderCacheServiceClient(conn)
	return c
}

// bearerCtx attaches "Bearer <hex(client-token)>" like a C++ client would.
func bearerCtx(secret string) context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+hex.EncodeToString(auth.ClientToken(secret))))
}

func TestGrpcAuthMissingCredentialsDenied(t *testing.T) {
	c := startAuthedGateway(t, "s3cr3t")
	if _, err := c.Ping(context.Background(), &gen.PingRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ping without bearer: %v", err)
	}
}

func TestGrpcAuthWrongSecretDenied(t *testing.T) {
	c := startAuthedGateway(t, "s3cr3t")
	md := metadata.Pairs("authorization", "Bearer "+hex.EncodeToString(auth.ClientToken("wrong")))
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	if _, err := c.Ping(ctx, &gen.PingRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ping with wrong bearer: %v", err)
	}
	if _, err := c.Ping(context.Background(), &gen.PingRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("ping with empty bearer: %v", err)
	}
}

func TestGrpcAuthSetGetWithBearer(t *testing.T) {
	c := startAuthedGateway(t, "s3cr3t")
	ctx := bearerCtx("s3cr3t")
	if set, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("set: %+v %v", set, err)
	}
	got, err := c.Get(ctx, &gen.GetRequest{Key: "k"})
	if err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("get: %+v %v", got, err)
	}
}

func TestGrpcAuthMultiSetAndInfoWithBearer(t *testing.T) {
	c := startAuthedGateway(t, "s3cr3t")
	ctx := bearerCtx("s3cr3t")
	ms, err := c.MultiSet(ctx, &gen.MultiSetRequest{Requests: []*gen.SetRequest{{Key: "a", Value: "1"}}})
	if err != nil || len(ms.Responses) != 1 || ms.Responses[0].Status != OK {
		t.Fatalf("multiset: %+v %v", ms, err)
	}
	info, err := c.Info(ctx, &gen.InfoRequest{})
	if err != nil || info.GetJson() == "" {
		t.Fatalf("info: %+v %v", info, err)
	}
}

func TestGrpcAuthUnconfiguredIsOpen(t *testing.T) {
	c, _ := startGateway(t, "node1")
	if pong, err := c.Ping(context.Background(), &gen.PingRequest{}); err != nil || pong.Status != OK {
		t.Fatalf("open gateway ping: %+v %v", pong, err)
	}
}

func TestGatewaySetAndGet(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	set, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"})
	if err != nil || set.Status != OK {
		t.Fatalf("set: %+v %v", set, err)
	}
	got, err := c.Get(ctx, &gen.GetRequest{Key: "k"})
	if err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("get: %+v %v", got, err)
	}
}

func TestGatewayGetNotFound(t *testing.T) {
	c, _ := startGateway(t, "node1")
	got, err := c.Get(context.Background(), &gen.GetRequest{Key: "missing"})
	if err != nil || got.Status != NotFound || got.GetValue() != "" {
		t.Fatalf("get miss: %+v %v", got, err)
	}
}

func TestGatewayDelete(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	if _, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	del, err := c.Delete(ctx, &gen.DeleteRequest{Key: "k"})
	if err != nil || del.Status != OK {
		t.Fatalf("delete: %+v %v", del, err)
	}
	// Plain read hides the tombstone; versioned read reports NOT_FOUND too.
	got, err := c.Get(ctx, &gen.GetRequest{Key: "k"})
	if err != nil || got.Status != NotFound {
		t.Fatalf("get after delete: %+v %v", got, err)
	}
	ver, err := c.GetVersioned(ctx, &gen.GetVersionedRequest{Key: "k"})
	if err != nil || ver.Status != NotFound {
		t.Fatalf("versioned after delete: %+v %v", ver, err)
	}
	// Deletes always succeed, even on missing keys.
	del, err = c.Delete(ctx, &gen.DeleteRequest{Key: "missing"})
	if err != nil || del.Status != OK {
		t.Fatalf("delete miss: %+v %v", del, err)
	}
}

func TestGatewayPing(t *testing.T) {
	c, _ := startGateway(t, "node1")
	pong, err := c.Ping(context.Background(), &gen.PingRequest{})
	if err != nil || pong.Status != OK {
		t.Fatalf("ping: %+v %v", pong, err)
	}
}

func TestGatewayInfoClusterRing(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	info, err := c.Info(ctx, &gen.InfoRequest{})
	if err != nil || info.GetJson() == "" {
		t.Fatalf("info: %+v %v", info, err)
	}
	if !contains(info.GetJson(), `"node_id":"node1"`) {
		t.Fatalf("info json: %s", info.GetJson())
	}
	cluster, err := c.ClusterInfo(ctx, &gen.ClusterInfoRequest{})
	if err != nil || !contains(cluster.GetJson(), "node1") {
		t.Fatalf("cluster: %+v %v", cluster, err)
	}
	ring, err := c.RingInfo(ctx, &gen.RingInfoRequest{})
	if err != nil || !contains(ring.GetJson(), "vnodes_per_node") {
		t.Fatalf("ring: %+v %v", ring, err)
	}
}

func TestGatewayMultiGetSet(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	ms, err := c.MultiSet(ctx, &gen.MultiSetRequest{Requests: []*gen.SetRequest{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"},
	}})
	if err != nil || len(ms.Responses) != 2 {
		t.Fatalf("multiset: %+v %v", ms, err)
	}
	for _, r := range ms.Responses {
		if r.Status != OK {
			t.Fatalf("multiset item: %+v", r)
		}
	}
	mg, err := c.MultiGet(ctx, &gen.MultiGetRequest{Keys: []string{"a", "b", "missing"}})
	if err != nil || len(mg.Responses) != 3 {
		t.Fatalf("multiget: %+v %v", mg, err)
	}
	if mg.Responses[0].GetValue() != "1" || mg.Responses[1].GetValue() != "2" {
		t.Fatalf("multiget values: %+v", mg.Responses)
	}
	if mg.Responses[2].Status != NotFound {
		t.Fatalf("multiget miss: %+v", mg.Responses[2])
	}
}

func TestGatewaySetWithTTL(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	if _, err := c.Set(ctx, &gen.SetRequest{Key: "ttl-key", Value: "ttl-val", TtlMs: 60000}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, &gen.GetRequest{Key: "ttl-key"})
	if err != nil || got.Status != OK || got.GetValue() != "ttl-val" {
		t.Fatalf("get ttl key: %+v %v", got, err)
	}
	ver, err := c.GetVersioned(ctx, &gen.GetVersionedRequest{Key: "ttl-key"})
	if err != nil || ver.Status != OK || ver.GetExpiresAtMs() != 0 {
		t.Fatalf("versioned ttl key must hide expiry (C++ parity): %+v %v", ver, err)
	}
}

func TestGatewayGetVersionedMeta(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	if _, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	one, err := c.GetVersioned(ctx, &gen.GetVersionedRequest{Key: "k"})
	if err != nil || one.Status != OK || one.GetValue() != "v" || one.Version == 0 {
		t.Fatalf("versioned: %+v %v", one, err)
	}
	if _, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v2"}); err != nil {
		t.Fatal(err)
	}
	two, err := c.GetVersioned(ctx, &gen.GetVersionedRequest{Key: "k"})
	if err != nil || two.GetValue() != "v2" || two.Version <= one.Version {
		t.Fatalf("versions not monotonic: %+v %+v %v", one, two, err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestGatewaySetNotReadyOnNonOwner(t *testing.T) {
	// Two peered nodes RF=1: a gateway Set/MultiSet for a foreign key
	// reports NOT_READY, mirroring the C++ ownership gate (and TCP
	// redirects). Rings are complete from static seeding, no gossip wait.
	type node struct {
		srv  *tcp.Server
		gw   *grpcgw.Gateway
		cli  gen.CinderCacheServiceClient
		conn *grpc.ClientConn
	}
	nodes := make([]node, 0, 2)
	addrs := map[string]string{}
	listeners := make([]net.Listener, 0, 2)
	for _, id := range []string{"node1", "node2"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		addrs[id] = l.Addr().String()
	}
	ports := map[string]int{}
	for id, a := range addrs {
		_, p, _ := net.SplitHostPort(a)
		var pi int
		fmt.Sscanf(p, "%d", &pi)
		ports[id] = pi
	}
	for i, id := range []string{"node1", "node2"} {
		var peers []tcp.Peer
		for pid, port := range ports {
			if pid != id {
				peers = append(peers, tcp.Peer{ID: pid, Host: "127.0.0.1", Port: port})
			}
		}
		srv := tcp.NewServer(id, ports[id], store.NewLRUStore(1<<20, nil), nil)
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		go srv.Serve(listeners[i])
		gl, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		gw := grpcgw.New(srv)
		go gw.Serve(gl)
		conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node{srv: srv, gw: gw, cli: gen.NewCinderCacheServiceClient(conn), conn: conn})
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.conn.Close()
			n.gw.Stop()
			n.srv.Shutdown()
		}
	})
	// Find a key owned by node2 and Set it through node1's gateway.
	foreign := ""
	for i := range 200 {
		if k := fmt.Sprintf("foreign-%d", i); nodes[0].srv.OwnerOf(k) == "node2" {
			foreign = k
			break
		}
	}
	if foreign == "" {
		t.Fatal("no key owned by node2")
	}
	ctx := context.Background()
	set, err := nodes[0].cli.Set(ctx, &gen.SetRequest{Key: foreign, Value: "v"})
	if err != nil || set.Status != NotReady {
		t.Fatalf("set on non-owner: %+v %v", set, err)
	}
	ms, err := nodes[0].cli.MultiSet(ctx, &gen.MultiSetRequest{Requests: []*gen.SetRequest{{Key: foreign, Value: "v"}}})
	if err != nil || len(ms.Responses) != 1 || ms.Responses[0].Status != NotReady {
		t.Fatalf("multiset on non-owner: %+v %v", ms, err)
	}
	// Through the owner it succeeds.
	if set, err := nodes[1].cli.Set(ctx, &gen.SetRequest{Key: foreign, Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("set on owner: %+v %v", set, err)
	}
}

// cppGrpcBinary locates a gRPC-enabled C++ build, skipping when absent.
// Prefers tls-grpc (verified working); falls back to debug-grpc, which may
// be stale (undefined abseil symbols) until rebuilt.
func cppGrpcBinary(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("CINDER_TEST_CINDERD_GRPC_PATH"); p != "" {
		return p
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..")
	for _, preset := range []string{"tls-grpc", "debug-grpc"} {
		p := filepath.Join(root, "build", preset, "bin", "cinderd")
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Skip("no gRPC C++ cinderd under build/{tls-grpc,debug-grpc}")
	return ""
}

// TestInteropGoClientCppGrpcServer runs the Go gRPC client against a real
// C++ gRPC gateway: the status mapping and framing must agree both ways.
func TestInteropGoClientCppGrpcServer(t *testing.T) {
	bin := cppGrpcBinary(t)
	tcpL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpL.Addr().(*net.TCPAddr).Port
	tcpL.Close()
	grpcL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcPort := grpcL.Addr().(*net.TCPAddr).Port
	grpcL.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(tcpPort), "--node-id", "node1",
		"--grpc-port", fmt.Sprint(grpcPort))
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	defer cmd.Process.Kill()
	addr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	var conn *grpc.ClientConn
	deadline := time.Now().Add(15 * time.Second)
	for {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			c := gen.NewCinderCacheServiceClient(conn)
			if pong, err := c.Ping(context.Background(), &gen.PingRequest{}); err == nil && pong.Status == OK {
				break
			}
			conn.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("C++ gRPC gateway did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer conn.Close()
	c := gen.NewCinderCacheServiceClient(conn)
	ctx := context.Background()
	if set, err := c.Set(ctx, &gen.SetRequest{Key: "xcpp", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("set: %+v %v", set, err)
	}
	got, err := c.Get(ctx, &gen.GetRequest{Key: "xcpp"})
	if err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("get: %+v %v", got, err)
	}
	ver, err := c.GetVersioned(ctx, &gen.GetVersionedRequest{Key: "xcpp"})
	if err != nil || ver.Status != OK || ver.GetValue() != "v" || ver.Version == 0 {
		t.Fatalf("versioned: %+v %v", ver, err)
	}
	del, err := c.Delete(ctx, &gen.DeleteRequest{Key: "xcpp"})
	if err != nil || del.Status != OK {
		t.Fatalf("delete: %+v %v", del, err)
	}
	miss, err := c.Get(ctx, &gen.GetRequest{Key: "xcpp"})
	if err != nil || miss.Status != NotFound {
		t.Fatalf("get after delete: %+v %v", miss, err)
	}
}

// fixturesDir locates the repo's tests/fixtures (shared with the C++ TLS
// suite).
func fixturesDir(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "tests", "fixtures")
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err != nil {
		t.Skipf("no fixtures at %s", dir)
	}
	return dir
}

// tlsClientConfig builds client credentials chaining to the fixture CA,
// mirroring tlscfg.Client's chain-only verification.
func tlsClientConfig(t testing.TB, caPath string) credentials.TransportCredentials {
	t.Helper()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatal("no certs in CA file")
	}
	return credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
}

// startTLSGateway serves the gateway over TLS; secret "" means no bearer
// check (mirroring C++: TLS alone does not imply auth).
func startTLSGateway(t *testing.T, secret string) gen.CinderCacheServiceClient {
	t.Helper()
	fx := fixturesDir(t)
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", tl.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(tl)
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := grpcgw.New(srv)
	if secret != "" {
		gw.SetClientSecret(secret)
	}
	if err := gw.SetTLS(filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem")); err != nil {
		t.Fatal(err)
	}
	go gw.Serve(gl)
	t.Cleanup(func() {
		gw.Stop()
		srv.Shutdown()
	})
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(tlsClientConfig(t, filepath.Join(fx, "ca.pem"))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return gen.NewCinderCacheServiceClient(conn)
}

func TestGatewayTLSRoundTrip(t *testing.T) {
	c := startTLSGateway(t, "")
	ctx := context.Background()
	if set, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("tls set: %+v %v", set, err)
	}
	if got, err := c.Get(ctx, &gen.GetRequest{Key: "k"}); err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("tls get: %+v %v", got, err)
	}
}

func TestGatewayPlaintextRejected(t *testing.T) {
	c := startTLSGateway(t, "")
	// Bypass the TLS client: raw insecure channel against a TLS listener
	// must fail at the handshake.
	_ = c
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", tl.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(tl)
	defer srv.Shutdown()
	fx := fixturesDir(t)
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := grpcgw.New(srv)
	if err := gw.SetTLS(filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem")); err != nil {
		t.Fatal(err)
	}
	go gw.Serve(gl)
	defer gw.Stop()
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	plain := gen.NewCinderCacheServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := plain.Ping(ctx, &gen.PingRequest{}); err == nil {
		t.Fatal("plaintext RPC on a TLS gateway succeeded")
	}
}

func TestGatewayAuthRequiresTLS(t *testing.T) {
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", tl.Addr().(*net.TCPAddr).Port, st, nil)
	go srv.Serve(tl)
	defer srv.Shutdown()
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer gl.Close()
	gw := grpcgw.New(srv)
	defer gw.Stop()
	gw.SetClientSecret("s3cr3t")
	// No SetTLS: Serve must refuse instead of serving auth over plaintext.
	if err := gw.Serve(gl); err == nil {
		t.Fatal("authenticated gateway served without TLS")
	}
}

func TestGatewayTLSWithBearer(t *testing.T) {
	c := startTLSGateway(t, "s3cr3t")
	if _, err := c.Ping(context.Background(), &gen.PingRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing bearer over TLS: %v", err)
	}
	ctx := bearerCtx("s3cr3t")
	if pong, err := c.Ping(ctx, &gen.PingRequest{}); err != nil || pong.Status != OK {
		t.Fatalf("bearer over TLS: %+v %v", pong, err)
	}
	if set, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("authed set over TLS: %+v %v", set, err)
	}
}

// TestInteropGoClientCppGrpcTLSAuth runs the Go TLS+gRPC client with a
// bearer against a C++ gateway with the same client secret (skips without
// a gRPC-enabled C++ build, like the other interop cases).
func TestInteropGoClientCppGrpcTLSAuth(t *testing.T) {
	fx := fixturesDir(t)
	bin := cppGrpcBinary(t)
	tcpL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpL.Addr().(*net.TCPAddr).Port
	tcpL.Close()
	grpcL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcPort := grpcL.Addr().(*net.TCPAddr).Port
	grpcL.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(tcpPort), "--node-id", "node1",
		"--grpc-port", fmt.Sprint(grpcPort),
		"--tls", "--tls-cert", filepath.Join(fx, "server.pem"),
		"--tls-key", filepath.Join(fx, "server-key.pem"),
		"--client-shared-secret", "s3cr3t")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	defer cmd.Process.Kill()
	addr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	var conn *grpc.ClientConn
	deadline := time.Now().Add(15 * time.Second)
	for {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(tlsClientConfig(t, filepath.Join(fx, "ca.pem"))))
		if err == nil {
			c := gen.NewCinderCacheServiceClient(conn)
			if pong, err := c.Ping(bearerCtx("s3cr3t"), &gen.PingRequest{}); err == nil && pong.Status == OK {
				break
			}
			conn.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("C++ TLS+auth gateway did not become ready")
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer conn.Close()
	c := gen.NewCinderCacheServiceClient(conn)
	if set, err := c.Set(bearerCtx("s3cr3t"), &gen.SetRequest{Key: "xcpp", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("set: %+v %v", set, err)
	}
	got, err := c.Get(bearerCtx("s3cr3t"), &gen.GetRequest{Key: "xcpp"})
	if err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("get: %+v %v", got, err)
	}
}

func TestGatewayCounterRoundTrip(t *testing.T) {
	c, _ := startGateway(t, "node1")
	ctx := context.Background()
	if res, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "c", Delta: 5}); err != nil || res.Status != OK {
		t.Fatalf("incr: %+v %v", res, err)
	}
	got, err := c.CounterGet(ctx, &gen.CounterGetRequest{Key: "c"})
	if err != nil || got.Status != OK || got.GetValue() != 5 {
		t.Fatalf("get: %+v %v", got, err)
	}
	if res, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "c", Delta: 2, Decrement: true}); err != nil || res.Status != OK {
		t.Fatalf("decr: %+v %v", res, err)
	}
	got, err = c.CounterGet(ctx, &gen.CounterGetRequest{Key: "c"})
	if err != nil || got.Status != OK || got.GetValue() != 3 {
		t.Fatalf("get after decr: %+v %v", got, err)
	}
	// Zero delta is a no-op OK that creates nothing.
	if res, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "fresh"}); err != nil || res.Status != OK {
		t.Fatalf("zero adjust: %+v %v", res, err)
	}
	if got, err := c.CounterGet(ctx, &gen.CounterGetRequest{Key: "fresh"}); err != nil || got.Status != NotFound {
		t.Fatalf("fresh get: %+v %v", got, err)
	}
}

func TestGatewayCounterAuthDenied(t *testing.T) {
	c := startAuthedGateway(t, "s3cr3t")
	if _, err := c.CounterAdjust(context.Background(), &gen.CounterAdjustRequest{Key: "c", Delta: 1}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("adjust without bearer: %v", err)
	}
	if _, err := c.CounterGet(context.Background(), &gen.CounterGetRequest{Key: "c"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("get without bearer: %v", err)
	}
	if _, err := c.CounterAdjust(bearerCtx("s3cr3t"), &gen.CounterAdjustRequest{Key: "c", Delta: 1}); err != nil {
		t.Fatalf("authed adjust: %v", err)
	}
	if got, err := c.CounterGet(bearerCtx("s3cr3t"), &gen.CounterGetRequest{Key: "c"}); err != nil || got.Status != OK || got.GetValue() != 1 {
		t.Fatalf("authed get: %+v %v", got, err)
	}
}

// TestGatewayWithServerSecretsEnforced mirrors real cinderd wiring: the
// server itself enforces the client secret (ConfigureSecrets) and the
// gateway carries the same secret. Gateway RPCs must still succeed — the
// gateway forwards its verified bearer so server-side policy (ownership,
// replication, quorum) applies instead of denying every call.
func TestGatewayWithServerSecretsEnforced(t *testing.T) {
	fx := fixturesDir(t)
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.NewLRUStore(1<<20, nil)
	srv := tcp.NewServer("node1", tl.Addr().(*net.TCPAddr).Port, st, nil)
	if err := srv.ConfigureTLS(filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem"), ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureSecrets("", "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(tl)
	defer srv.Shutdown()
	gl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gw := grpcgw.New(srv)
	gw.SetClientSecret("s3cr3t")
	if err := gw.SetTLS(filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem")); err != nil {
		t.Fatal(err)
	}
	go gw.Serve(gl)
	defer gw.Stop()
	conn, err := grpc.NewClient(gl.Addr().String(), grpc.WithTransportCredentials(tlsClientConfig(t, filepath.Join(fx, "ca.pem"))))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := gen.NewCinderCacheServiceClient(conn)
	ctx := bearerCtx("s3cr3t")
	if set, err := c.Set(ctx, &gen.SetRequest{Key: "k", Value: "v"}); err != nil || set.Status != OK {
		t.Fatalf("authed set with server secrets: %+v %v", set, err)
	}
	if got, err := c.Get(ctx, &gen.GetRequest{Key: "k"}); err != nil || got.Status != OK || got.GetValue() != "v" {
		t.Fatalf("authed get with server secrets: %+v %v", got, err)
	}
	if adj, err := c.CounterAdjust(ctx, &gen.CounterAdjustRequest{Key: "c", Delta: 4}); err != nil || adj.Status != OK {
		t.Fatalf("authed counter adjust with server secrets: %+v %v", adj, err)
	}
	if got, err := c.CounterGet(ctx, &gen.CounterGetRequest{Key: "c"}); err != nil || got.Status != OK || got.GetValue() != 4 {
		t.Fatalf("authed counter get with server secrets: %+v %v", got, err)
	}
	if info, err := c.Info(ctx, &gen.InfoRequest{}); err != nil || info.GetJson() == "" {
		t.Fatalf("authed info with server secrets: %+v %v", info, err)
	}
}
