package tcp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

func (s *Server) ConfigureReplication(rf hashring.ReplicaCount, mode repl.Mode, m *repl.Manager) error {
	if rf < 1 || rf > hashring.MaxReplicas {
		return hashring.ErrInvalidReplicaCount
	}
	if !mode.Valid() {
		return repl.ErrInvalidMode
	}

	s.replMu.Lock()
	s.repl = m
	s.rf = rf
	s.mode = mode
	s.replMu.Unlock()
	if m != nil {
		m.SetMetrics(s.met)
		m.SetLogger(s.log())
	}
	return nil
}

// ConfigureTLS enables TLS with a certificate chain and key, optionally
// requiring client certificates from the configured CA. Call before Serve;
// it also enables TLS for inter-node pool connections. The certificate is
// re-read from disk on every handshake, so Reload can rotate it without
// disturbing established connections.
func (s *Server) ConfigureTLS(certFile, keyFile, caFile string) error {
	cfg, err := tlscfg.Server(certFile, keyFile, caFile)
	if err != nil {
		return err
	}

	cfg.GetCertificate = s.certReloader()
	s.tlsMu.Lock()
	s.tlsCfg = cfg
	s.tlsCertFile, s.tlsKeyFile, s.tlsCAFile = certFile, keyFile, caFile
	s.tlsMu.Unlock()
	s.ringMu.RLock()
	pool := s.pool
	s.ringMu.RUnlock()
	if pool != nil {
		// Inter-node connections present this node's own certificate.
		ccfg, err := tlscfg.Client(certFile, keyFile, caFile)
		if err != nil {
			return err
		}
		pool.SetTLS(ccfg)
	}
	return nil
}

// certReloader returns a GetCertificate callback that loads the currently
// configured chain from disk. A failed load fails that handshake; the last
// good in-memory certificate keeps serving until a load succeeds.
func (s *Server) certReloader() func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		s.tlsMu.RLock()
		certFile, keyFile := s.tlsCertFile, s.tlsKeyFile
		s.tlsMu.RUnlock()
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return &cert, nil
	}
}

// rotateTLSCertificate swaps the serving certificate after Reload validated
// the new files. The CA file is unchanged (rotating peer verification
// requires a restart) and established connections are unaffected.
func (s *Server) rotateTLSCertificate(certFile, keyFile string, cert tls.Certificate) error {
	s.tlsMu.RLock()
	caFile := s.tlsCAFile
	s.tlsMu.RUnlock()
	cfg, err := tlscfg.Server(certFile, keyFile, caFile)
	if err != nil {
		return err
	}

	cfg.Certificates = []tls.Certificate{cert}
	cfg.GetCertificate = s.certReloader()
	s.tlsMu.Lock()
	s.tlsCfg = cfg
	s.tlsCertFile, s.tlsKeyFile = certFile, keyFile
	s.tlsMu.Unlock()
	return nil
}

// ConfigureSecrets enables authentication: sharedSecret guards internal
// replication opcodes with time-windowed node tokens, clientSharedSecret
// guards client/admin opcodes (and the gRPC bearer) with the static client
// token. Empty secrets leave their corresponding traffic class
// unauthenticated. A static client bearer requires TLS; configure TLS
// before setting clientSecret. Node tokens can be used without TLS.
func (s *Server) ConfigureSecrets(shared, clientSecret string) error {
	if clientSecret != "" {
		s.tlsMu.RLock()
		tlsCfg := s.tlsCfg
		s.tlsMu.RUnlock()
		if tlsCfg == nil {
			return errors.New("tcp: client auth requires TLS (replayable bearer token)")
		}
	}

	s.authMu.Lock()
	s.sharedSecret = shared
	s.clientSharedSecret = clientSecret
	s.authMu.Unlock()
	// Inter-node traffic carries node tokens minted with this node's id.
	s.ringMu.RLock()
	pool := s.pool
	s.ringMu.RUnlock()
	if pool != nil {
		pool.SetSecrets(client.Secrets{NodeID: s.nodeID, Shared: shared})
	}
	return nil
}

// ConfigurePersistence enables the WAL + snapshots in dir, recovering
// before serving. The store hook is wired after recovery so replay never
// logs itself. Call before Serve.
func (s *Server) ConfigurePersistence(dir string) error {
	m := persist.NewManager(dir, s.store, nil)
	if err := m.Recover(); err != nil {
		return err
	}

	s.store.SetPersistence(func(key string, e store.Entry, hardDelete bool) error {
		return m.OnWrite(key, e, hardDelete)
	})

	m.SetLogger(s.log())
	s.persistMu.Lock()
	s.persist = m
	s.persistMu.Unlock()
	return nil
}

// SetFsyncPolicy configures the persistence manager's WAL sync policy.
// Accepts "none", "always", or "interval:<milliseconds>".
func (s *Server) SetFsyncPolicy(raw string) error {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return fmt.Errorf("persistence is not configured")
	}

	mode, interval, err := persist.ParseFsyncPolicy(raw)
	if err != nil {
		return err
	}
	return m.SetFsyncPolicy(mode, interval)
}

// Compact snapshots and rotates the WAL; wired to ADMIN_COMPACT.
func (s *Server) Compact() error {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return nil
	}
	return m.Compact()
}

// SetSnapshotInterval changes the periodic auto-compact cadence (0 disables
// the time trigger). No-op without ConfigurePersistence.
func (s *Server) SetSnapshotInterval(d time.Duration) {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return
	}
	m.SetSnapshotInterval(d)
}

// SetMaxWALBytes caps the active WAL past which auto-compact fires (0
// disables the size trigger). No-op without ConfigurePersistence.
func (s *Server) SetMaxWALBytes(n uint64) {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return
	}
	m.SetMaxWALBytes(n)
}

// SetMaxWALEntries caps the active WAL record count past which
// MaybeCompact fires (0 disables the count trigger).
func (s *Server) SetMaxWALEntries(n uint64) {
	s.persistMu.RLock()
	m := s.persist
	s.persistMu.RUnlock()
	if m == nil {
		return
	}
	m.SetMaxWALEntries(n)
}

func (s *Server) ConfigureAntiEntropy(numBuckets uint32, fetchBytes int) {
	s.ringMu.RLock()
	ring := s.ring
	pool := s.pool
	s.ringMu.RUnlock()
	m := ae.NewManager(ae.Config{
		Store: s.store, Ring: ring, Self: s.nodeID, Transport: pool,
		Buckets: numBuckets, FetchBytes: fetchBytes,
	})

	m.SetCounterStore(s.counters)
	m.SetMetrics(s.met)
	m.SetLogger(s.log())
	s.aeMu.Lock()
	s.ae = m
	s.aeMu.Unlock()
}

// SetAntiEntropyInterval changes the AE round period.
func (s *Server) SetAntiEntropyInterval(d time.Duration) {
	s.aeMu.Lock()
	defer s.aeMu.Unlock()
	s.aeInterval = d
}

func (s *Server) SetGossipInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.gossipInterval = d
	if s.gossip != nil {
		s.gossip.SetInterval(d)
	}
}

// SetProbeInterval changes the failure-detector tick period.
func (s *Server) SetProbeInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.probeInterval = d
}

// SetSuspectTimeout changes the suspect/dead timeout.
func (s *Server) SetSuspectTimeout(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.suspectTimeout = d
	if s.fd != nil {
		s.fd.SetSuspectTimeout(d)
	}
}

// SetGossipRPCTimeout bounds gossip and failure-detector RPCs. A
// non-positive duration selects the 1s default. Existing pool connections
// are dropped so they redial under the new budget.
func (s *Server) SetGossipRPCTimeout(d time.Duration) {
	if d <= 0 {
		d = time.Second
	}

	s.gossipRPCTimeoutNs.Store(int64(d))
	s.ringMu.RLock()
	pool := s.pool
	s.ringMu.RUnlock()
	if pool != nil {
		pool.SetTimeout(d)
	}
}

// SetReplayInterval changes the base hint-replay delay: the loop retries
// at this cadence while rounds make progress, backing off toward the max
// on consecutive all-failed rounds (tests use short ones). Zero or
// negative selects the 1s default.
func (s *Server) SetReplayInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.replayInterval = d
}

// SetReplayMaxInterval caps the hint-replay backoff. Zero or negative
// selects the 30s default.
func (s *Server) SetReplayMaxInterval(d time.Duration) {
	s.ringMu.Lock()
	defer s.ringMu.Unlock()
	s.replayMax = d
}

// SetWriteTimeout bounds a single response write; exceeding it closes
// the connection as a slow consumer. Zero or negative selects the
// writeTimeout default.
func (s *Server) SetWriteTimeout(d time.Duration) {
	s.writeTimeoutNs.Store(int64(d))
}

// currentWriteTimeout returns the configured write budget, or the
// default when unset or non-positive.
func (s *Server) currentWriteTimeout() time.Duration {
	if d := time.Duration(s.writeTimeoutNs.Load()); d > 0 {
		return d
	}
	return writeTimeout
}
