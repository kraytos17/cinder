package tcp

import (
	"crypto/tls"
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
	TombstoneGCMs    int64  `json:"tombstone_gc_ms"`
	MaxHintBytes     uint64 `json:"max_hint_bytes"`
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
		return nil, fmt.Errorf("capacity must be positive")
	}
	if (rc.TLSCertFile != "") != (rc.TLSKeyFile != "") {
		return nil, fmt.Errorf("tls_cert and tls_key must be given together")
	}

	var newCert *tls.Certificate
	if rc.TLSCertFile != "" {
		cert, err := tls.LoadX509KeyPair(rc.TLSCertFile, rc.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls_cert/tls_key: %w", err)
		}
		newCert = &cert
	}

	ms := toDurationMs(rc.GossipIntervalMs)
	s.SetGossipInterval(ms)
	s.SetProbeInterval(toDurationMs(rc.PingIntervalMs))
	s.SetSuspectTimeout(toDurationMs(rc.SuspectTimeoutMs))
	s.SetAntiEntropyInterval(toDurationMs(rc.AEIntervalMs))
	if newCert != nil {
		if err := s.rotateTLSCertificate(rc.TLSCertFile, rc.TLSKeyFile, *newCert); err != nil {
			return nil, err
		}
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
	return append([]string(nil), reloadableKeys...), nil
}
