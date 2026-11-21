// Package tlscfg builds TLS configurations for Cinder clients and servers.
// TLS 1.2 is the minimum version. An optional CA enables certificate-chain
// verification. Client() verifies only the chain (no hostname) for
// backward-compatible inter-node use; ClientForHost additionally verifies
// the peer hostname (DNS or IP SAN) and is the default for external
// clients.
package tlscfg

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/netip"
	"os"
)

// Server returns a server-side config. With a CA file, clients must present
// a certificate chaining to that CA. Without a CA, client certificates are
// not requested, though the TLS connection is still encrypted.
func Server(certFile, keyFile, caFile string) (*tls.Config, error) {
	auth := tls.NoClientCert
	if caFile != "" {
		auth = tls.RequireAndVerifyClientCert
	}
	return ServerWithOptions(certFile, keyFile, caFile, auth, tls.VersionTLS12)
}

// ParseClientAuth parses --tls-client-auth: none disables client-cert
// verification, request verifies when a certificate is presented,
// require rejects handshakes without a valid chained certificate.
func ParseClientAuth(s string) (tls.ClientAuthType, error) {
	switch s {
	case "none":
		return tls.NoClientCert, nil
	case "request":
		return tls.VerifyClientCertIfGiven, nil
	case "require":
		return tls.RequireAndVerifyClientCert, nil
	default:
		return tls.NoClientCert, errors.New("tlscfg: --tls-client-auth must be none|request|require")
	}
}

// ParseMinVersion parses --tls-min-version ("1.2" or "1.3").
func ParseMinVersion(s string) (uint16, error) {
	switch s {
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, errors.New("tlscfg: --tls-min-version must be 1.2|1.3")
	}
}

// ServerWithOptions returns a server-side config with explicit client-cert
// policy and minimum TLS version. Modes that verify peer certificates
// require a CA file; MinVersion is applied verbatim to the config.
func ServerWithOptions(certFile, keyFile, caFile string, auth tls.ClientAuthType, minVersion uint16) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   minVersion,
	}
	switch auth {
	case tls.NoClientCert:
		// No peer verification even when a CA is configured.
	case tls.RequestClientCert, tls.RequireAnyClientCert, tls.VerifyClientCertIfGiven, tls.RequireAndVerifyClientCert:
		if caFile == "" {
			return nil, errors.New("tlscfg: client-cert verification requires a CA file")
		}

		pool, err := loadRoots(caFile)
		if err != nil {
			return nil, err
		}

		cfg.ClientAuth = auth
		cfg.ClientCAs = pool
	default:
		return nil, errors.New("tlscfg: unknown client auth mode")
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

// ClientForHost returns a client-side config like Client, but when caFile
// and serverName are both set it also verifies the peer hostname against
// the presented SANs (DNS names and IP SANs via VerifyOptions.DNSName).
// An empty serverName falls back to chain-only verification for
// backward compatibility. Without a CA, verification is disabled and
// serverName is carried as SNI only when it is a DNS name.
func ClientForHost(certFile, keyFile, caFile, serverName string) (*tls.Config, error) {
	cfg, err := Client(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	if caFile == "" || serverName == "" {
		if serverName != "" {
			setServerName(cfg, serverName)
		}
		return cfg, nil
	}

	pool, err := loadRoots(caFile)
	if err != nil {
		return nil, err
	}
	//nolint:gosec // G123: resumed sessions may skip
	// VerifyPeerCertificate. The pool uses long-lived connections; this
	// follows crypto/tls session-resumption behavior.
	cfg.VerifyPeerCertificate = chainAndHostnameVerifier(pool, serverName)
	setServerName(cfg, serverName)
	return cfg, nil
}

// setServerName carries a DNS name as SNI; IP literals are left empty
// since SNI with an IP is not sent and the custom verifier checks IP
// SANs via DNSName instead.
func setServerName(cfg *tls.Config, serverName string) {
	if _, err := netip.ParseAddr(serverName); err == nil {
		return
	}
	cfg.ServerName = serverName
}

// chainAndHostnameVerifier verifies the chain and that the leaf covers
// serverName (DNS SAN or IP SAN). Intermediates from the peer complete
// the chain.
func chainAndHostnameVerifier(pool *x509.CertPool, serverName string) func([][]byte, [][]*x509.Certificate) error {
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
			DNSName:   serverName,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		for _, raw := range rawCerts[1:] {
			if c, err := x509.ParseCertificate(raw); err == nil {
				opts.Intermediates.AddCert(c)
			}
		}

		_, err = leaf.Verify(opts)
		return err
	}
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
