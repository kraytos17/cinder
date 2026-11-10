package tcp_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/net/protocol"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

// fixturesDir locates the repo's tests/fixtures (shared with the C++ TLS
// suite: ca.pem, server.pem, server-key.pem).
func fixturesDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	dir := filepath.Join(filepath.Dir(file), "..", "..", "..", "tests", "fixtures")
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err != nil {
		t.Skipf("no fixtures at %s", dir)
	}
	return dir
}

// genCerts mints a throwaway CA + server cert (localhost/127.0.0.1 SANs)
// for hermetic Go↔Go TLS tests that must not depend on fixtures.
func genCerts(t *testing.T) (caPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "go test ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	srvTpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM := func(path, typ string, der []byte) {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
			t.Fatal(err)
		}
	}
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "server.pem")
	keyPath = filepath.Join(dir, "server-key.pem")
	writePEM(caPath, "CERTIFICATE", caDER)
	writePEM(certPath, "CERTIFICATE", srvDER)
	keyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(keyPath, "EC PRIVATE KEY", keyDER)
	return caPath, certPath, keyPath
}

func startTLSServer(t *testing.T, certFile, keyFile, caFile string) (string, *tcp.Server) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := tcp.NewServer(tcp.ServerConfig{NodeID: "node1", Port: netport.Port(l.Addr().(*net.TCPAddr).Port), Store: store.NewLRUStore(1<<20, nil)})
	if err := srv.ConfigureTLS(certFile, keyFile, caFile); err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(srv.Shutdown)
	return l.Addr().String(), srv
}

func mustSetGet(t *testing.T, c *client.Client) {
	t.Helper()
	if res, err := c.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("set: %+v %v", res, err)
	}
	res, err := c.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("k")})
	if err != nil || res.Status != protocol.StatusOK || string(res.Value) != "v" {
		t.Fatalf("get: %+v %v", res, err)
	}
}

func TestTLSGoToGo(t *testing.T) {
	caPath, certPath, keyPath := genCerts(t)
	addr, _ := startTLSServer(t, certPath, keyPath, "")
	cfg, err := tlscfg.Client("", "", caPath)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.DialTLS(addr, 5*time.Second, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustSetGet(t, c)
}

func TestTLSPlaintextRejected(t *testing.T) {
	_, certPath, keyPath := genCerts(t)
	addr, _ := startTLSServer(t, certPath, keyPath, "")
	c, err := client.Dial(addr, 2*time.Second)
	if err != nil {
		return // refused outright is also fine
	}
	defer c.Close()
	if _, err := c.Do(protocol.Request{Opcode: protocol.OpPing}); err == nil {
		t.Fatal("plaintext RPC on a TLS server succeeded")
	}
}

func TestTLSClientAuth(t *testing.T) {
	caPath, certPath, keyPath := genCerts(t)
	addr, _ := startTLSServer(t, certPath, keyPath, caPath)
	// Client presenting a certificate: accepted.
	good, err := tlscfg.Client(certPath, keyPath, caPath)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.DialTLS(addr, 5*time.Second, good)
	if err != nil {
		t.Fatalf("cert-bearing client rejected: %v", err)
	}
	mustSetGet(t, c)
	c.Close()
	// Client without a certificate: the handshake may complete lazily,
	// but no RPC can succeed (the server rejects on first read).
	bad, err := tlscfg.Client("", "", caPath)
	if err != nil {
		t.Fatal(err)
	}
	if c2, err := client.DialTLS(addr, 5*time.Second, bad); err == nil {
		defer c2.Close()
		if res, err := c2.Do(protocol.Request{Opcode: protocol.OpPing}); err == nil {
			c2.Close()
			t.Fatalf("cert-less client completed an RPC: %+v", res)
		}
	}
	// Wrong CA: chain verification fails.
	otherCA, _, _ := genCerts(t)
	wrong, err := tlscfg.Client("", "", otherCA)
	if err != nil {
		t.Fatal(err)
	}
	if c3, err := client.DialTLS(addr, 5*time.Second, wrong); err == nil {
		c3.Close()
		t.Fatal("client trusting another CA accepted")
	}
}

// tlsCppBinary locates the TLS-enabled C++ build, skipping when absent.
func tlsCppBinary(t *testing.T, name string) string {
	t.Helper()
	if p := os.Getenv("CINDER_TEST_TLS_" + name); p != "" {
		return p
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("no caller")
	}
	root := filepath.Join(filepath.Dir(file), "..", "..", "..")
	p := filepath.Join(root, "build", "debug-tls", "bin", name)
	if _, err := os.Stat(p); err != nil {
		t.Skipf("no TLS C++ %s at %s", name, p)
	}
	return p
}

// TestInteropGoClientCppTLSServer runs the Go TLS client against a real
// C++ TLS server (fixture certs): chain-only verification must accept the
// fixture chain exactly like the C++ client does.
func TestInteropGoClientCppTLSServer(t *testing.T) {
	fx := fixturesDir(t)
	bin := tlsCppBinary(t, "cinderd")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--node-id", "node1",
		"--tls", "--tls-cert", filepath.Join(fx, "server.pem"),
		"--tls-key", filepath.Join(fx, "server-key.pem"))
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	defer cmd.Process.Kill()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for {
		cfg, _ := tlscfg.Client("", "", filepath.Join(fx, "ca.pem"))
		c, err := client.DialTLS(addr, 500*time.Millisecond, cfg)
		if err == nil {
			if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
				c.Close()
				break
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("C++ TLS server did not become ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg, err := tlscfg.Client("", "", filepath.Join(fx, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.DialTLS(addr, 5*time.Second, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	mustSetGet(t, c)
}

// TestInteropCppClientGoTLSServer runs the real C++ TLS CLI against a Go
// TLS server (fixture certs): proves the Go handshake, framing, and
// dispatch interoperate with a non-Go peer.
func TestInteropCppClientGoTLSServer(t *testing.T) {
	fx := fixturesDir(t)
	cli := tlsCppBinary(t, "cinder-cli")
	addr, _ := startTLSServer(t, filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem"), "")
	_, port, _ := net.SplitHostPort(addr)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"ping"}, "pong\n"},
		{[]string{"set", "xcpp", "v"}, "OK\n"},
		{[]string{"get", "xcpp"}, "v\n"},
	} {
		args := append([]string{"--port", port, "--tls", "--tls-ca", filepath.Join(fx, "ca.pem")}, tc.args...)
		cmd := exec.Command(cli, args...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%v failed: %v", tc.args, err)
		}
		if string(out) != tc.want {
			t.Fatalf("%v: got %q want %q", tc.args, out, tc.want)
		}
	}
}

// TestInteropGoClientCppAuthServer runs the Go TLS client with a client
// secret against a C++ TLS server requiring it: proves the token framing
// and HMAC contract agree across implementations.
func TestInteropGoClientCppAuthServer(t *testing.T) {
	fx := fixturesDir(t)
	bin := tlsCppBinary(t, "cinderd")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--node-id", "node1",
		"--tls", "--tls-cert", filepath.Join(fx, "server.pem"),
		"--tls-key", filepath.Join(fx, "server-key.pem"),
		"--client-shared-secret", "s3cr3t")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start cinderd: %v", err)
	}
	defer cmd.Process.Kill()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dial := func(s client.Secrets) (*client.Client, error) {
		cfg, _ := tlscfg.Client("", "", filepath.Join(fx, "ca.pem"))
		c, err := client.DialTLS(addr, 500*time.Millisecond, cfg)
		if err != nil {
			return nil, err
		}
		c.SetSecrets(s)
		return c, nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := dial(client.Secrets{Client: "s3cr3t"})
		if err == nil {
			if res, err := c.Ping(); err == nil && res.Status == protocol.StatusOK {
				c.Close()
				break
			}
			c.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("C++ auth server did not become ready")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// No token: denied even though the handshake succeeded.
	anon, err := dial(client.Secrets{})
	if err != nil {
		t.Fatal(err)
	}
	defer anon.Close()
	if res, err := anon.Do(protocol.Request{Opcode: protocol.OpPing}); err != nil || res.Status != protocol.StatusPermissionDenied {
		t.Fatalf("anonymous ping: %+v %v", res, err)
	}
	// Right token: full access.
	good, err := dial(client.Secrets{Client: "s3cr3t"})
	if err != nil {
		t.Fatal(err)
	}
	defer good.Close()
	if res, err := good.Do(protocol.Request{Opcode: protocol.OpSet, Key: []byte("k"), Value: []byte("v")}); err != nil || res.Status != protocol.StatusOK {
		t.Fatalf("authed set: %+v %v", res, err)
	}
	if res, err := good.Do(protocol.Request{Opcode: protocol.OpGet, Key: []byte("k")}); err != nil || res.Status != protocol.StatusOK || string(res.Value) != "v" {
		t.Fatalf("authed get: %+v %v", res, err)
	}
}

// TestInteropCppClientGoAuthServer runs the real C++ TLS CLI with a client
// secret against a Go server requiring it.
func TestInteropCppClientGoAuthServer(t *testing.T) {
	fx := fixturesDir(t)
	cli := tlsCppBinary(t, "cinder-cli")
	addr, srv := startTLSServer(t, filepath.Join(fx, "server.pem"), filepath.Join(fx, "server-key.pem"), "")
	_ = srv
	if err := srv.ConfigureSecrets("", "s3cr3t"); err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(addr)
	base := []string{"--port", port, "--tls", "--tls-ca", filepath.Join(fx, "ca.pem")}
	// Without the secret the C++ CLI is denied.
	cmd := exec.Command(cli, append(base, "ping")...)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("anonymous C++ CLI succeeded: %q", out)
	}
	// With it, full access.
	authed := append(base, "--client-secret", "s3cr3t") //nolint:gocritic // base is reused for multiple arg variants below; aliasing is intended
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"ping"}, "pong\n"},
		{[]string{"set", "xcpp", "v"}, "OK\n"},
		{[]string{"get", "xcpp"}, "v\n"},
	} {
		cmd := exec.Command(cli, append(authed, tc.args...)...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("%v failed: %v", tc.args, err)
		}
		if string(out) != tc.want {
			t.Fatalf("%v: got %q want %q", tc.args, out, tc.want)
		}
	}
}
