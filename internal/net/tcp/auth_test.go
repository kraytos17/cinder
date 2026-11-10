package tcp_test

import (
	"fmt"
	"net"
	"testing"
	"time"

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

// tlsServer starts a node with the fixture certificate (no client-cert
// verification) and the given secrets. TLS is required to configure a
// client secret, mirroring the C++ startup gate.
func tlsServer(t *testing.T, id string, shared, clientSecret string) (string, *tcp.Server) {
	t.Helper()
	fx := fixturesDir(t)
	l := listenTCP(t)
	srv := tcp.NewServer(id, l.Addr().(*net.TCPAddr).Port, store.NewLRUStore(1<<20, nil), nil)
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
	res, err := c.Do(protocol.Request{Opcode: protocol.OpAdminInfo})
	if err != nil || res.Status != protocol.StatusOK || !res.HasValue {
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
	srv := tcp.NewServer("node1", 0, store.NewLRUStore(1<<20, nil), nil)
	if err := srv.ConfigureSecrets("", "client-secret"); err == nil {
		t.Fatal("client secret without TLS must fail closed")
	}
	if err := srv.ConfigureSecrets("shared-only", ""); err != nil {
		t.Fatalf("node secret without TLS must be allowed: %v", err)
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
		srv := tcp.NewServer(id, ports[id], st, nil)
		if err := srv.ConfigureCluster(peers); err != nil {
			t.Fatal(err)
		}
		srv.SetSuspectTimeout(time.Hour)
		if err := srv.ConfigureSecrets("shared-secret", ""); err != nil {
			t.Fatal(err)
		}
		pool := client.NewPool(addrs, 5*time.Second)
		pool.SetSecrets(client.Secrets{NodeID: id, Shared: "shared-secret"})
		mgr := repl.NewManager(st, id, nil, pool)
		srv.ConfigureReplication(2, repl.Async, mgr)
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
