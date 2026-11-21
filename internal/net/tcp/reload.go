package tcp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/kraytos17/cinder/internal/log"
)

// Reloadable keys accepted by OpAdminConfigReload and SIGHUP reloads.
// Anything else in a reload payload is rejected with InvalidArgument:
// RF, bind, port, node-id, data-dir, and peers require a restart.
const (
	reloadKeyLogLevel         = "log_level"
	reloadKeyGossipIntervalMs = "gossip_interval_ms"
	reloadKeyPingIntervalMs   = "ping_interval_ms"
	reloadKeySuspectTimeoutMs = "suspect_timeout_ms"
	reloadKeyAEIntervalMs     = "anti_entropy_interval_ms"
	reloadKeyTLSCert          = "tls_cert"
	reloadKeyTLSKey           = "tls_key"
	reloadKeyCapacity         = "capacity"
	reloadKeyTombstoneGCMs    = "tombstone_gc_ms"
	reloadKeyMaxHintBytes     = "max_hint_bytes"
	reloadKeyMaxConnections   = "max_connections"
	reloadKeySharedSecret     = "shared_secret"
	reloadKeySharedSecretPrev = "shared_secret_previous"
	reloadKeyClientSecret     = "client_secret"
	reloadKeyClientSecretPrev = "client_secret_previous"
	reloadKeyAdminSecret      = "admin_secret"
	reloadKeyAdminSecretPrev  = "admin_secret_previous"
)

// reloadableKeys lists every key Reload applies, in stable order.
var reloadableKeys = []string{
	reloadKeyLogLevel,
	reloadKeyGossipIntervalMs,
	reloadKeyPingIntervalMs,
	reloadKeySuspectTimeoutMs,
	reloadKeyAEIntervalMs,
	reloadKeyTLSCert,
	reloadKeyTLSKey,
	reloadKeyCapacity,
	reloadKeyTombstoneGCMs,
	reloadKeyMaxHintBytes,
	reloadKeyMaxConnections,
	reloadKeySharedSecret,
	reloadKeySharedSecretPrev,
	reloadKeyClientSecret,
	reloadKeyClientSecretPrev,
	reloadKeyAdminSecret,
	reloadKeyAdminSecretPrev,
}

// ReloadableConfig is the set of settings a running node may change
// without a restart. Durations are milliseconds; TLS paths are PEM files.
// A zero CreatedBy... (no): every field applies as given, so callers that
// want partial updates must start from the active config and overlay.
type ReloadableConfig struct {
	LogLevel         string `json:"log_level"`
	GossipIntervalMs int64  `json:"gossip_interval_ms"`
	PingIntervalMs   int64  `json:"ping_interval_ms"`
	SuspectTimeoutMs int64  `json:"suspect_timeout_ms"`
	AEIntervalMs     int64  `json:"anti_entropy_interval_ms"`
	TLSCertFile      string `json:"tls_cert"`
	TLSKeyFile       string `json:"tls_key"`
	Capacity         uint64 `json:"capacity"`
	MaxConnections   int    `json:"max_connections"`
	TombstoneGCMs    int64  `json:"tombstone_gc_ms"`
	MaxHintBytes     uint64 `json:"max_hint_bytes"`
	SharedSecret     string `json:"shared_secret"`
	SharedSecretPrev string `json:"shared_secret_previous"`
	ClientSecret     string `json:"client_secret"`
	ClientSecretPrev string `json:"client_secret_previous"`
	AdminSecret      string `json:"admin_secret"`
	AdminSecretPrev  string `json:"admin_secret_previous"`
}

// toDurationMs converts a millisecond count to a duration.
func toDurationMs(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// isReloadableKey reports whether k may appear in a reload payload.
func isReloadableKey(k string) bool {
	return slices.Contains(reloadableKeys, k)
}

// Reload validates every field first, then applies the whole config.
// Validation failure leaves the running config untouched. It returns the
// canonical applied key list. Existing connections keep serving; TLS
// changes affect only subsequently established connections.
func (s *Server) Reload(rc ReloadableConfig) ([]string, error) {
	lvl, err := log.ParseLevel(rc.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("log_level: %w", err)
	}
	for _, iv := range []struct {
		name  string
		value int64
	}{
		{reloadKeyGossipIntervalMs, rc.GossipIntervalMs},
		{reloadKeyPingIntervalMs, rc.PingIntervalMs},
		{reloadKeySuspectTimeoutMs, rc.SuspectTimeoutMs},
		{reloadKeyAEIntervalMs, rc.AEIntervalMs},
		{reloadKeyTombstoneGCMs, rc.TombstoneGCMs},
	} {
		if iv.value < 0 {
			return nil, fmt.Errorf("%s must be >= 0", iv.name)
		}
	}
	if rc.Capacity == 0 {
		return nil, errors.New("capacity must be positive")
	}

	if rc.MaxConnections < 1 {
		return nil, errors.New("max_connections must be positive")
	}
	if rc.PingIntervalMs > 0 && rc.SuspectTimeoutMs > 0 && rc.SuspectTimeoutMs <= rc.PingIntervalMs {
		return nil, errors.New("suspect_timeout_ms must be greater than ping_interval_ms")
	}
	if (rc.TLSCertFile != "") != (rc.TLSKeyFile != "") {
		return nil, errors.New("tls_cert and tls_key must be given together")
	}

	var newCert *tls.Certificate
	if rc.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(rc.TLSCertFile, rc.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls_cert/tls_key: %w", err)
		}
		newCert = &cert
	}
	if rc.ClientSecret != "" || rc.ClientSecretPrev != "" || rc.AdminSecret != "" || rc.AdminSecretPrev != "" {
		s.tlsMu.RLock()
		tlsCfg := s.tlsCfg
		s.tlsMu.RUnlock()
		if tlsCfg == nil {
			return nil, errors.New("client/admin secrets require TLS")
		}
	}

	ms := toDurationMs(rc.GossipIntervalMs)
	s.SetGossipInterval(ms)
	s.SetProbeInterval(toDurationMs(rc.PingIntervalMs))
	s.SetSuspectTimeout(toDurationMs(rc.SuspectTimeoutMs))
	s.SetAntiEntropyInterval(toDurationMs(rc.AEIntervalMs))
	if err := s.ConfigureSecrets(rc.SharedSecret, rc.ClientSecret); err != nil {
		return nil, err
	}
	if err := s.ConfigureAdminSecret(rc.AdminSecret); err != nil {
		return nil, err
	}
	if err := s.SetPreviousSecrets(rc.SharedSecretPrev, rc.ClientSecretPrev, rc.AdminSecretPrev); err != nil {
		return nil, err
	}
	if newCert != nil {
		if err := s.rotateTLSCertificate(rc.TLSCertFile, rc.TLSKeyFile, *newCert); err != nil {
			return nil, err
		}
	}
	if err := s.SetMaxConnections(rc.MaxConnections); err != nil {
		return nil, err
	}

	s.store.SetCapacity(rc.Capacity)
	s.store.SetTombstoneGCInterval(toDurationMs(rc.TombstoneGCMs))
	s.replMu.RLock()
	replMgr := s.repl
	s.replMu.RUnlock()
	if replMgr != nil {
		replMgr.SetMaxHintBytes(rc.MaxHintBytes)
	}

	leveled := s.log().Level(lvl)
	s.SetLogger(&leveled)
	return slices.Clone(reloadableKeys), nil
}
