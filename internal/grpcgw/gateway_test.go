package grpcgw_test

import (
	"context"
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

func startGateway(t *testing.T, id string) (gen.CinderCacheServiceClient, func()) {
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
func startAuthedGateway(t *testing.T, secret string) gen.CinderCacheServiceClient {
	t.Helper()
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
	var nodes []node
	addrs := map[string]string{}
	var listeners []net.Listener
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
func cppGrpcBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("CINDER_TEST_CINDERD_GRPC_PATH"); p != "" {
		return p
	}
	_, file, _, _ := runtime.Caller(0)
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
