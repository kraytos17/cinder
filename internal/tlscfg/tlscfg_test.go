package tlscfg

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func mintCAAndServer(t *testing.T, dns []string, ips []net.IP) (caPath, certPath, keyPath string) {
	t.Helper()
	dir := t.TempDir()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
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
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "srv"},
		DNSNames: dns, IPAddresses: ips,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
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

func handshakeWith(t *testing.T, cfg *tls.Config, addr string) error {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer raw.Close()
	c := tls.Client(raw, cfg)
	if err := c.Handshake(); err != nil {
		return err
	}
	return c.Close()
}

// A client bound to the expected hostname must accept the fixture SANs,
// while a client bound to an unexpected name must fail the handshake.
func TestClientForHostVerifiesHostname(t *testing.T) {
	caPath, certPath, keyPath := mintCAAndServer(t, []string{"cinder.test"}, []net.IP{net.ParseIP("127.0.0.1")})

	srvCfg, err := Server(certPath, keyPath, "")
	if err != nil {
		t.Fatal(err)
	}
	l, err := tls.Listen("tcp", "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 1)
				_, _ = c.Read(buf)
			}()
		}
	}()

	good, err := ClientForHost("", "", caPath, "cinder.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := handshakeWith(t, good, l.Addr().String()); err != nil {
		t.Fatalf("correct hostname must verify: %v", err)
	}

	bad, err := ClientForHost("", "", caPath, "evil.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := handshakeWith(t, bad, l.Addr().String()); err == nil {
		t.Fatal("wrong hostname must fail verification")
	}

	ipGood, err := ClientForHost("", "", caPath, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := handshakeWith(t, ipGood, l.Addr().String()); err != nil {
		t.Fatalf("IP SAN must verify: %v", err)
	}
}
