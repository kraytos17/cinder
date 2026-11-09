// Package tlscfg builds TLS configurations for Cinder peers (Go port).
//
// Semantics mirror the C++ Asio wiring: TLS 1.2 minimum, PEM certificate
// chain + key on the server, optional CA file enabling peer verification
// on both sides. Verification is chain-only (no hostname check), exactly
// like asio verify_peer: test certificates carry 127.0.0.1 SANs, but
// production certificates may not, and the C++ client would still
// connect — so the Go client must too.
package tlscfg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// Server returns a server-side config. With a CA file, clients must present
// a verifiable certificate (mirrors set_verify_mode(verify_peer)); without
// it any client is accepted (the TLS handshake still encrypts).
func Server(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		pool, err := loadRoots(caFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	return cfg, nil
}

// Client returns a client-side config. With a CA file the server chain is
// verified (chain-only, no hostname check); without it verification is
// skipped entirely (mirrors verify_none). certFile/keyFile (both required
// together) present a client certificate for servers that verify peers.
func Client(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("tlscfg: --tls-cert and --tls-key must be given together")
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if caFile != "" {
		pool, err := loadRoots(caFile)
		if err != nil {
			return nil, err
		}
		cfg.VerifyPeerCertificate = chainOnlyVerifier(pool)
	}
	return cfg, nil
}

// chainOnlyVerifier verifies the presented chain against roots without
// any hostname check, mirroring asio verify_peer.
func chainOnlyVerifier(pool *x509.CertPool) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("tlscfg: no server certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		opts := x509.VerifyOptions{
			Roots:     pool,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		// Intermediates ride along like any chain builder.
		for _, raw := range rawCerts[1:] {
			if c, err := x509.ParseCertificate(raw); err == nil {
				opts.Intermediates.AddCert(c)
			}
		}
		_, err = leaf.Verify(opts)
		return err
	}
}

func loadRoots(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("tlscfg: no certificates in " + caFile)
	}
	return pool, nil
}
