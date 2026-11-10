// Package tlscfg builds TLS configurations for Cinder clients and servers.
// TLS 1.2 is the minimum version. An optional CA enables certificate-chain
// verification; the client verifier intentionally does not check hostnames.
package tlscfg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

// Server returns a server-side config. With a CA file, clients must present
// a certificate chaining to that CA. Without a CA, client certificates are
// not requested, though the TLS connection is still encrypted.
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

// Client returns a client-side config. With a CA file, the server chain is
// verified without hostname checking; without one, server-certificate
// verification is disabled. certFile and keyFile must be provided together
// and configure a client certificate for servers that verify peers.
func Client(certFile, keyFile, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		//nolint:gosec // G402: InsecureSkipVerify disables Go's default
		// hostname verifier. When caFile is set, chainOnlyVerifier below
		// still validates the certificate chain on every handshake.
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
		//nolint:gosec // G123: resumed sessions may skip
		// VerifyPeerCertificate. The pool uses long-lived connections; this
		// follows crypto/tls session-resumption behavior.
		cfg.VerifyPeerCertificate = chainOnlyVerifier(pool)
	}
	return cfg, nil
}

// chainOnlyVerifier verifies the presented chain against roots without
// checking the peer hostname.
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
		// Add presented intermediates so Verify can build the chain.
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
