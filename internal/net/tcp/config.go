package tcp

import (
	"errors"
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
// it also enables TLS for inter-node pool connections.
func (s *Server) ConfigureTLS(certFile, keyFile, caFile string) error {
	cfg, err := tlscfg.Server(certFile, keyFile, caFile)
	if err != nil {
		return err
	}

	s.tlsMu.Lock()
	s.tlsCfg = cfg
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

	s.store.SetPersistence(func(key string, e store.Entry, hardDelete bool) {
		m.OnWrite(key, e, hardDelete)
	})

	m.SetLogger(s.log())
	s.persistMu.Lock()
	s.persist = m
	s.persistMu.Unlock()
	return nil
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
