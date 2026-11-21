package tcp_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/auth"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

// listenTCP is a loopback listener with an ephemeral port.
func listenTCP(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// tlsServer starts a node with the fixture certificate, optional secrets,
// and no client-certificate verification.
func tlsServer(t *testing.T, id string, shared, clientSecret string) (string, *tcp.Server) {
	t.Helper()
	fx := fixturesDir(t)
	l := listenTCP(t)
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: id, Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureSecrets(shared, clientSecret); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return l.Addr().String(), srv
}

func tlsClient(t *testing.T, addr string, s client.Secrets) *client.Client {
	t.Helper()
	fx := fixturesDir(t)
	cfg, err := tlscfg.Client("", "", fx+"/ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.DialTLS(addr, 5*time.Second, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetSecrets(s)
	return c
}

func TestAuthOpenByDefault(t *testing.T) {
	addr, _ := tlsServer(t, "node1", "", "")
	c := tlsClient(t, addr, client.Secrets{})
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpPing}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("open server ping: %+v %v", res, err)
	}
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpAdminInfo}); err != nil || res.Status != protocol.StatusOK || !res.HasValue {
		t.Fatalf("open server admin: %+v %v", res, err)
	}
}

func TestAuthClientSecretGate(t *testing.T) {
	addr, _ := tlsServer(t, "node1", "", "client-secret")

	// No token: denied on client and admin opcodes, with no value body.
	anon := tlsClient(t, addr, client.Secrets{})
	for _, op := range []protocol.Opcode{protocol.OpPing, protocol.OpGet, protocol.OpSet, protocol.OpAdminInfo} {
		res, err := anon.Do(protocol.Request{Opcode: op, Key: []byte("k"), Value: []byte("v")})
		if err != nil {
			t.Fatal(err)
		}
		if res.Status != protocol.StatusPermissionDenied {
			t.Fatalf("opcode %d without token: got %v, want PermissionDenied", op, res.Status)
		}
		if res.HasValue {
			t.Fatalf("opcode %d denied response must carry no value", op)
		}
	}
	// Wrong token: denied.
	wrong := tlsClient(t, addr, client.Secrets{Client: "nope"})
	if res, err := wrong.Do(protocol.Request{Opcode: protocol.OpPing}); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("wrong token ping: %+v %v", res, err)
	}
	// Right token: full access, including admin and versioned reads.
	good := tlsClient(t, addr, client.Secrets{Client: "client-secret"})
	if res, err := good.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("authed set: %+v %v", res, err)
	}
	if res, err := good.Do(protocol.Request{Opcode: protocol.OpGetVersioned, Key: []byte("k")}); err != nil || res.Status != protocol.StatusOK || string(res.Value) != "v" {
		t.Fatalf("authed get-versioned: %+v %v", res, err)
	}
	if res, err := good.Do(protocol.Request{Opcode: protocol.OpAdminCluster}); err != nil || res.Status != protocol.StatusOK || !res.HasValue {
		t.Fatalf("authed admin: %+v %v", res, err)
	}
}

func TestAuthNodeSecretGate(t *testing.T) {
	addr, srv := tlsServer(t, "node1", "shared-secret", "")

	// Client opcodes stay open when only the node secret is set.
	anon := tlsClient(t, addr, client.Secrets{})
	if res, err := anon.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("client op without token: %+v %v", res, err)
	}
	// Internal opcodes require a node token minted by a known id.
	rep := protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("r"), Value: []byte("v"), Version: 9, WriterNodeHash: 7}
	if res, err := anon.Do(rep); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("replicate without token: %+v %v", res, err)
	}
	stranger := tlsClient(t, addr, client.Secrets{NodeID: "nodeX", Shared: "shared-secret"})
	if res, err := stranger.Do(rep); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("replicate with unknown-id token: %+v %v", res, err)
	}
	wrong := tlsClient(t, addr, client.Secrets{NodeID: "node1", Shared: "wrong"})
	if res, err := wrong.Do(rep); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("replicate with wrong secret: %+v %v", res, err)
	}
	member := tlsClient(t, addr, client.Secrets{NodeID: "node1", Shared: "shared-secret"})
	if res, err := member.Do(rep); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("replicate with member token: %+v %v", res, err)
	}
	if e, ok := srv.Store().GetVersioned("r"); !ok || string(e.Value) != "v" {
		t.Fatalf("replicated entry missing: %+v %v", e, ok)
	}
	// Admin falls back to the node token when no client secret is set.
	if res, err := member.Do(protocol.Request{Opcode: protocol.OpAdminInfo}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("admin with node token: %+v %v", res, err)
	}
	if res, err := anon.Do(protocol.Request{Opcode: protocol.OpAdminInfo}); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("admin without token: %+v %v", res, err)
	}
}

func TestAuthClientSecretRequiresTLS(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureSecrets("", "client-secret"); err == nil {
		t.Fatal("client secret without TLS must fail closed")
	}
	if err := srv.ConfigureSecrets("shared-only", ""); err != nil {
		t.Fatalf("node secret without TLS must be allowed: %v", err)
	}
}

// TestAuthFailuresCounted pins the abuse-resistance signal: PermissionDenied
// on client/admin opcodes bumps the client reason, replication denials bump
// the node reason. Denied responses must still carry no value body.
func TestAuthFailuresCounted(t *testing.T) {
	addr, srv := tlsServer(t, "node1", "shared-secret", "client-secret")
	anon := tlsClient(t, addr, client.Secrets{})
	res, err := anon.Do(protocol.Request{Opcode: protocol.OpPing})
	if err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("anon ping: %+v %v", res, err)
	}
	if res.HasValue {
		t.Fatal("denied client response must carry no value")
	}
	if got := srv.Metrics().AuthFailClient.Load(); got != 1 {
		t.Fatalf("client denials = %d, want 1", got)
	}
	rep := protocol.Request{Opcode: protocol.OpReplicate, Key: []byte("r"), Value: []byte("v"), Version: 9, WriterNodeHash: 7}
	if res, err := anon.Do(rep); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("anon replicate: %+v %v", res, err)
	}
	if got := srv.Metrics().AuthFailNode.Load(); got != 1 {
		t.Fatalf("node denials = %d, want 1", got)
	}
	if res, err := anon.Do(protocol.Request{Opcode: protocol.OpAdminInfo}); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("anon admin: %+v %v", res, err)
	}
	if got := srv.Metrics().AuthFailClient.Load(); got != 2 {
		t.Fatalf("client denials after admin = %d, want 2", got)
	}
}

// TestAuthCrossNodeNodeTokens proves the deliberate deviation from a
// literal C++ port: the receiver accepts tokens minted by any known peer,
// not just itself. A self-only check would break every write here, since
// the sender always mints with its own id.
func TestAuthCrossNodeNodeTokens(t *testing.T) {
	type node struct {
		id  string
		srv *tcp.Server
		mgr *repl.Manager
	}
	nodes := map[string]*node{}
	listeners := make([]net.Listener, 0, 2)
	ports := map[string]int{}
	for _, id := range []string{"node1", "node2"} {
		l := listenTCP(t)
		listeners = append(listeners, l)
		ports[id] = l.Addr().(*net.TCPAddr).Port
	}
	for i, id := range []string{"node1", "node2"} {
		var peers []tcp.Peer
		var addrs []client.NodeAddr
		for pid, port := range ports {
			if pid == id {
				continue
			}
			peers = append(peers, tcp.Peer{ID: pid, Host: "127.0.0.1", Port: netport.Port(port)})
			addrs = append(addrs, client.NodeAddr{ID: pid, Host: "127.0.0.1", Port: netport.Port(port)})
		}
		st := store.NewLRUStore(1<<20, nil)
		srv := tcp.NewServer(tcp.ServerConfig{NodeID: id, Port: netport.Port(ports[id]), Store: st})
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		srv.SetSuspectTimeout(time.Hour)
		if err := srv.ConfigureSecrets("shared-secret", ""); err != nil {
			t.Fatal(err)
		}
		pool := client.NewPool(addrs, 5*time.Second)
		pool.SetSecrets(client.Secrets{NodeID: id, Shared: "shared-secret"})
		mgr := repl.NewManager(repl.Config{Local: st, Self: id, Clock: nil, Transport: pool})
		if err := srv.ConfigureReplication(2, repl.Async, mgr); err != nil {
			t.Fatal(err)
		}
		go srv.Serve(listeners[i])
		t.Cleanup(srv.Shutdown)
		nodes[id] = &node{id: id, srv: srv, mgr: mgr}
	}
	// Write each key to its owner (RF=2 replicates to the other node).
	for i := range 10 {
		k := fmt.Sprintf("xnode-key-%d", i)
		addr := listeners[0].Addr().String()
		if ringOwner(t, k) != "node1" {
			addr = listeners[1].Addr().String()
		}
		d, err := client.Dial(addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		res, err := d.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte(k), Value: []byte("v")})
		d.Close()
		if err != nil || res.Status != protocol.StatusOK {
			t.Fatalf("set %q: %+v %v", k, res, err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		done := true
		for i := range 10 {
			k := fmt.Sprintf("xnode-key-%d", i)
			if _, ok := nodes["node2"].mgr.Local().GetVersioned(k); !ok {
				done = false
				break
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("cross-node replication with node auth never converged")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestAuthRoleSplit pins the data/admin role separation. With an admin
// secret configured, mutating admin opcodes require the admin token while
// data opcodes and read-only admin accept either the data or the admin
// token. Replication still requires the node token; without an admin
// secret the single client secret keeps full access (backward compat).
func TestAuthRoleSplit(t *testing.T) {
	newSplitServer := func(t *testing.T) *tcp.Server {
		t.Helper()
		fx := fixturesDir(t)
		srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
		if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
			t.Fatal(err)
		}
		if err := srv.ConfigureSecrets("shared-secret", "client-secret"); err != nil {
			t.Fatal(err)
		}
		if err := srv.ConfigureAdminSecret("admin-secret"); err != nil {
			t.Fatal(err)
		}
		return srv
	}
	srv := newSplitServer(t)
	dataTok := auth.ClientToken("client-secret")
	adminTok := auth.ClientToken("admin-secret")
	nodeTok := auth.NodeToken("shared-secret", "node1", time.Now())

	cases := []struct {
		name   string
		op     protocol.Opcode
		token  []byte
		wantOK bool
	}{
		{"data with data token", protocol.OpPing, dataTok, true},
		{"data with admin token", protocol.OpPing, adminTok, true},
		{"data with node token", protocol.OpPing, nil, false},
		{"data anon", protocol.OpPing, nil, false},
		{"read-only admin with data token", protocol.OpAdminCluster, dataTok, true},
		{"read-only admin with admin token", protocol.OpAdminCluster, adminTok, true},
		{"read-only admin anon", protocol.OpAdminCluster, nil, false},
		{"mutating admin with admin token", protocol.OpAdminCompact, adminTok, true},
		{"mutating admin with data token", protocol.OpAdminCompact, dataTok, false},
		{"mutating admin with node token", protocol.OpAdminCompact, nodeTok, false},
		{"mutating admin anon", protocol.OpAdminCompact, nil, false},
	}
	for _, tc := range cases {
		res := srv.DispatchWithAuth(protocol.Request{Opcode: tc.op}, tc.token)
		if ok := res.Status == protocol.StatusOK; ok != tc.wantOK {
			t.Errorf("%s: status=%v wantOK=%v", tc.name, res.Status, tc.wantOK)
		}
		if res.Status == protocol.StatusPermissionDenied && res.HasValue {
			t.Errorf("%s: denial must carry no value", tc.name)
		}
	}

	// Backward compat: single client secret (no admin) keeps full access,
	// including mutating admin.
	fx := fixturesDir(t)
	legacy := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := legacy.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	if err := legacy.ConfigureSecrets("", "client-secret"); err != nil {
		t.Fatal(err)
	}
	if res := legacy.DispatchWithAuth(protocol.Request{Opcode: protocol.OpAdminCompact}, dataTok); res.Status != protocol.StatusOK {
		t.Fatalf("legacy single-secret compact: %+v", res)
	}
}

func TestConfigureAdminSecretRequiresTLS(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureAdminSecret("admin-secret"); err == nil {
		t.Fatal("admin secret without TLS must fail closed")
	}
}

func TestAuthRotationAcceptsPrevious(t *testing.T) {
	fx := fixturesDir(t)
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureTLS(fx+"/server.pem", fx+"/server-key.pem", ""); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureSecrets("shared-new", "client-new"); err != nil {
		t.Fatal(err)
	}
	if err := srv.ConfigureAdminSecret("admin-new"); err != nil {
		t.Fatal(err)
	}
	if err := srv.SetPreviousSecrets("shared-old", "client-old", "admin-old"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cases := []struct {
		name   string
		op     protocol.Opcode
		token  []byte
		wantOK bool
	}{
		{"replicate current node token", protocol.OpReplicate, auth.NodeToken("shared-new", "node1", now), true},
		{"replicate previous node token", protocol.OpReplicate, auth.NodeToken("shared-old", "node1", now), true},
		{"replicate wrong token", protocol.OpReplicate, auth.NodeToken("shared-bogus", "node1", now), false},
		{"data current token", protocol.OpPing, auth.ClientToken("client-new"), true},
		{"data previous token", protocol.OpPing, auth.ClientToken("client-old"), true},
		{"data admin previous token", protocol.OpPing, auth.ClientToken("admin-old"), true},
		{"read-only admin previous token", protocol.OpAdminCluster, auth.ClientToken("client-old"), true},
		{"mutating admin current token", protocol.OpAdminCompact, auth.ClientToken("admin-new"), true},
		{"mutating admin previous token", protocol.OpAdminCompact, auth.ClientToken("admin-old"), true},
		{"mutating admin data current token denied", protocol.OpAdminCompact, auth.ClientToken("client-new"), false},
		{"mutating admin data previous token denied", protocol.OpAdminCompact, auth.ClientToken("client-old"), false},
	}
	for _, tc := range cases {
		req := protocol.Request{Opcode: tc.op}
		if tc.op == protocol.OpReplicate {
			req.Key, req.Value, req.Version, req.WriterNodeHash = []byte("r"), []byte("v"), 9, 7
		}
		res := srv.DispatchWithAuth(req, tc.token)
		if ok := res.Status == protocol.StatusOK; ok != tc.wantOK {
			t.Errorf("%s: status=%v wantOK=%v", tc.name, res.Status, tc.wantOK)
		}
	}
}

func TestSetPreviousSecretsRequiresTLS(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(0), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.SetPreviousSecrets("", "client-old", ""); err == nil {
		t.Fatal("client previous secret without TLS must fail closed")
	}
	if err := srv.SetPreviousSecrets("shared-old", "", ""); err != nil {
		t.Fatalf("node previous secret without TLS must be allowed: %v", err)
	}
}

func TestMaxConnectionsRefusesBeyondLimit(t *testing.T) {
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.SetMaxConnections(1); err != nil {
		t.Fatal(err)
	}
	if err := srv.SetMaxConnections(0); err == nil {
		t.Fatal("zero max connections must fail")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	addr := l.Addr().String()
	held, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	// Give the acceptor a moment to register the held connection.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("second dial: %v", err)
		}
		c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		buf := make([]byte, 1)
		_, err = c.Read(buf)
		c.Close()
		if err != nil {
			break // refused: server closed the over-limit socket
		}
		if time.Now().After(deadline) {
			t.Fatal("over-limit connection was served")
		}
		time.Sleep(20 * time.Millisecond)
	}
	held.Close()
	// A freed slot serves again.
	c, err := client.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if res, err := c.Ping(); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("ping after release: %+v %v", res, err)
	}
}
