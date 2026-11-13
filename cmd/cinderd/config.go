package main

import (
	"encoding/json/v2"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/net/tcp"
)

// daemonConfig is the single exported config representation used by both
// command-line flags and JSON config files. configFile is process metadata.
type daemonConfig struct {
	Port                 int    `json:"port"`
	GRPCPort             int    `json:"grpc-port"`
	MetricsPort          int    `json:"metrics-port"`
	BindHost             string `json:"bind"`
	AdvertiseHost        string `json:"advertise-host"`
	GRPCBind             string `json:"grpc-bind"`
	MetricsBind          string `json:"metrics-bind"`
	NodeID               string `json:"node-id"`
	Peers                string `json:"peers"`
	Consistency          string `json:"consistency"`
	EvictionPolicy       string `json:"eviction-policy"`
	LogLevel             string `json:"log-level"`
	ReplicationFactor    int    `json:"replication-factor"`
	GossipIntervalMs     int    `json:"gossip-interval-ms"`
	ProbeIntervalMs      int    `json:"ping-interval-ms"`
	SuspectMs            int    `json:"suspect-timeout-ms"`
	GossipRPCTimeoutMs   int    `json:"gossip-rpc-timeout-ms"`
	AEIntervalMs         int    `json:"anti-entropy-interval-ms"`
	AEBuckets            int    `json:"anti-entropy-buckets"`
	QuarantineIntervalMs int    `json:"quarantine-interval-ms"`
	Capacity             uint64 `json:"capacity"`
	MaxWALBytes          uint64 `json:"max-wal-bytes"`
	MaxWALEntries        uint64 `json:"max-wal-entries"`
	MaxHintBytes         uint64 `json:"max-hint-bytes"`
	TLS                  bool   `json:"tls"`
	EnablePersistence    bool   `json:"enable-persistence"`
	TLSCert              string `json:"tls-cert"`
	TLSKey               string `json:"tls-key"`
	TLSCA                string `json:"tls-ca"`
	SharedSecret         string `json:"shared-secret"`
	ClientSecret         string `json:"client-shared-secret"`
	DataDir              string `json:"data-dir"`
	SnapshotIntervalS    int    `json:"snapshot-interval-s"`
	RPCTimeoutMs         int    `json:"rpc-timeout-ms"`
	AEFetchBytes         int    `json:"anti-entropy-fetch-bytes"`
	TombstoneGCMS        int    `json:"tombstone-gc-ms"`
	configFile           string
	ShutdownTimeoutMs    int    `json:"shutdown-timeout-ms"`
	FsyncPolicy          string `json:"fsync"`
	nodeIDExplicit       bool   `json:"-"`
}

func defaultDaemonConfig() daemonConfig {
	return daemonConfig{
		Port: 7000, BindHost: "127.0.0.1", NodeID: "node1", Capacity: 67_108_864,
		ReplicationFactor: 1, Consistency: "async", GossipIntervalMs: 1000,
		ProbeIntervalMs: 1000, SuspectMs: 3000, AEIntervalMs: 30000,
		AEBuckets: 256, QuarantineIntervalMs: 10000, GRPCBind: "127.0.0.1",
		MetricsBind: "127.0.0.1", SnapshotIntervalS: 60, MaxWALBytes: 64 << 20,
		MaxWALEntries: 10000, RPCTimeoutMs: 5000, AEFetchBytes: 1 << 20,
		EvictionPolicy: "lru", LogLevel: "info", ShutdownTimeoutMs: 10000, FsyncPolicy: "none",
	}
}

func (cfg daemonConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{Name: "port", Value: cfg.Port, Usage: "TCP listen port"},
		&cli.StringFlag{Name: "bind", Value: cfg.BindHost, Usage: "TCP listener bind host or IP"},
		&cli.StringFlag{Name: "advertise-host", Value: cfg.AdvertiseHost, Usage: "reachable host advertised to cluster peers (defaults to --bind; required with wildcard --bind)"},
		&cli.StringFlag{Name: "node-id", Value: cfg.NodeID, Usage: "stable node identifier"},
		&cli.Uint64Flag{Name: "capacity", Value: cfg.Capacity, Usage: "store capacity in bytes"},
		&cli.StringFlag{Name: "peers", Value: cfg.Peers, Usage: "comma-separated id@host:port cluster members"},
		&cli.IntFlag{Name: "replication-factor", Value: cfg.ReplicationFactor, Usage: "replica count (1 = none)"},
		&cli.StringFlag{Name: "consistency", Value: cfg.Consistency, Usage: "write consistency: async|quorum"},
		&cli.IntFlag{Name: "gossip-interval-ms", Value: cfg.GossipIntervalMs, Usage: "gossip round interval"},
		&cli.IntFlag{Name: "ping-interval-ms", Value: cfg.ProbeIntervalMs, Usage: "failure-detector probe interval"},
		&cli.IntFlag{Name: "suspect-timeout-ms", Value: cfg.SuspectMs, Usage: "suspect and dead escalation timeout"},
		&cli.IntFlag{Name: "gossip-rpc-timeout-ms", Value: cfg.GossipRPCTimeoutMs, Usage: "gossip/probe RPC deadline in milliseconds (0 = min(ping interval, 1s))"},
		&cli.IntFlag{Name: "anti-entropy-interval-ms", Value: cfg.AEIntervalMs, Usage: "anti-entropy round interval"},
		&cli.IntFlag{Name: "anti-entropy-buckets", Value: cfg.AEBuckets, Usage: "anti-entropy digest buckets"},
		&cli.IntFlag{Name: "quarantine-interval-ms", Value: cfg.QuarantineIntervalMs, Usage: "rejoin warming window (0 = disabled)"},
		&cli.IntFlag{Name: "grpc-port", Value: cfg.GRPCPort, Usage: "gRPC gateway port (0 = disabled)"},
		&cli.StringFlag{Name: "grpc-bind", Value: cfg.GRPCBind, Usage: "gRPC gateway bind host or IP"},
		&cli.IntFlag{Name: "metrics-port", Value: cfg.MetricsPort, Usage: "Prometheus /metrics HTTP port (0 = disabled)"},
		&cli.StringFlag{Name: "metrics-bind", Value: cfg.MetricsBind, Usage: "metrics and pprof bind host or IP"},
		&cli.BoolFlag{Name: "tls", Value: cfg.TLS, Usage: "enable TLS encryption"},
		&cli.StringFlag{Name: "tls-cert", Value: cfg.TLSCert, Usage: "path to TLS certificate chain (PEM)"},
		&cli.StringFlag{Name: "tls-key", Value: cfg.TLSKey, Usage: "path to TLS private key (PEM)"},
		&cli.StringFlag{Name: "tls-ca", Value: cfg.TLSCA, Usage: "path to CA certificate for peer verification (PEM)"},
		&cli.StringFlag{Name: "shared-secret", Value: cfg.SharedSecret, Usage: "shared secret for node authentication (or CINDER_SHARED_SECRET)"},
		&cli.StringFlag{Name: "client-shared-secret", Value: cfg.ClientSecret, Usage: "shared secret for client authentication, requires --tls (or CINDER_CLIENT_SECRET)"},
		&cli.BoolFlag{Name: "enable-persistence", Value: cfg.EnablePersistence, Usage: "enable WAL + snapshots"},
		&cli.StringFlag{Name: "data-dir", Value: cfg.DataDir, Usage: "persistence directory (required with --enable-persistence)"},
		&cli.IntFlag{Name: "snapshot-interval-s", Value: cfg.SnapshotIntervalS, Usage: "seconds between automatic snapshots (0 = disabled)"},
		&cli.StringFlag{Name: "fsync", Value: cfg.FsyncPolicy, Usage: "WAL sync policy: none|always|interval:<ms> (default none)"},
		&cli.Uint64Flag{Name: "max-wal-bytes", Value: cfg.MaxWALBytes, Usage: "active WAL bytes before out-of-cycle compact (0 = disabled)"},
		&cli.Uint64Flag{Name: "max-wal-entries", Value: cfg.MaxWALEntries, Usage: "active WAL records before out-of-cycle compact (0 = disabled)"},
		&cli.IntFlag{Name: "rpc-timeout-ms", Value: cfg.RPCTimeoutMs, Usage: "replication and anti-entropy RPC deadline in milliseconds (0 = no timeout)"},
		&cli.IntFlag{Name: "anti-entropy-fetch-bytes", Value: cfg.AEFetchBytes, Usage: "per-round anti-entropy repair fetch budget in bytes"},
		&cli.Uint64Flag{Name: "max-hint-bytes", Value: cfg.MaxHintBytes, Usage: "hinted-handoff byte budget (0 = default 64MiB)"},
		&cli.IntFlag{Name: "tombstone-gc-ms", Value: cfg.TombstoneGCMS, Usage: "delete-marker GC grace in milliseconds (0 = default 5min)"},
		&cli.IntFlag{Name: "shutdown-timeout-ms", Value: 10000, Usage: "graceful shutdown deadline in milliseconds"},
		&cli.StringFlag{Name: "eviction-policy", Value: cfg.EvictionPolicy, Usage: "eviction policy: lru|lfu"},
		&cli.StringFlag{Name: "log-level", Value: cfg.LogLevel, Usage: "log level: trace|debug|info|warn|error"},
		&cli.StringFlag{Name: "config", Value: cfg.configFile, Usage: "JSON config file (flags override file values)"},
		&cli.BoolFlag{Name: "version", Value: false, Usage: "print version and exit"},
	}
}

func readDaemonConfig(cmd *cli.Command) daemonConfig {
	return readDaemonConfigWithBase(cmd, defaultDaemonConfig())
}

// readDaemonConfigWithBase resolves the effective config: base supplies
// defaults (possibly overlaid from --config), and only explicitly passed
// flags override it. Secrets fall back to the environment when unset in
// both the file and flags.
func readDaemonConfigWithBase(cmd *cli.Command, base daemonConfig) daemonConfig {
	cfg := base
	if cmd.IsSet("port") {
		cfg.Port = cmd.Int("port")
	}
	if cmd.IsSet("bind") {
		cfg.BindHost = cmd.String("bind")
	}
	if cmd.IsSet("advertise-host") {
		cfg.AdvertiseHost = cmd.String("advertise-host")
	}
	if cmd.IsSet("node-id") {
		cfg.NodeID = cmd.String("node-id")
		cfg.nodeIDExplicit = true
	}
	if cmd.IsSet("capacity") {
		cfg.Capacity = cmd.Uint64("capacity")
	}
	if cmd.IsSet("peers") {
		cfg.Peers = cmd.String("peers")
	}
	if cmd.IsSet("replication-factor") {
		cfg.ReplicationFactor = cmd.Int("replication-factor")
	}
	if cmd.IsSet("consistency") {
		cfg.Consistency = cmd.String("consistency")
	}
	if cmd.IsSet("gossip-interval-ms") {
		cfg.GossipIntervalMs = cmd.Int("gossip-interval-ms")
	}
	if cmd.IsSet("ping-interval-ms") {
		cfg.ProbeIntervalMs = cmd.Int("ping-interval-ms")
	}
	if cmd.IsSet("suspect-timeout-ms") {
		cfg.SuspectMs = cmd.Int("suspect-timeout-ms")
	}
	if cmd.IsSet("gossip-rpc-timeout-ms") {
		cfg.GossipRPCTimeoutMs = cmd.Int("gossip-rpc-timeout-ms")
	}
	if cmd.IsSet("anti-entropy-interval-ms") {
		cfg.AEIntervalMs = cmd.Int("anti-entropy-interval-ms")
	}
	if cmd.IsSet("anti-entropy-buckets") {
		cfg.AEBuckets = cmd.Int("anti-entropy-buckets")
	}
	if cmd.IsSet("quarantine-interval-ms") {
		cfg.QuarantineIntervalMs = cmd.Int("quarantine-interval-ms")
	}
	if cmd.IsSet("grpc-port") {
		cfg.GRPCPort = cmd.Int("grpc-port")
	}
	if cmd.IsSet("grpc-bind") {
		cfg.GRPCBind = cmd.String("grpc-bind")
	}
	if cmd.IsSet("metrics-port") {
		cfg.MetricsPort = cmd.Int("metrics-port")
	}
	if cmd.IsSet("metrics-bind") {
		cfg.MetricsBind = cmd.String("metrics-bind")
	}
	if cmd.IsSet("tls") {
		cfg.TLS = cmd.Bool("tls")
	}
	if cmd.IsSet("tls-cert") {
		cfg.TLSCert = cmd.String("tls-cert")
	}
	if cmd.IsSet("tls-key") {
		cfg.TLSKey = cmd.String("tls-key")
	}
	if cmd.IsSet("tls-ca") {
		cfg.TLSCA = cmd.String("tls-ca")
	}
	if cmd.IsSet("shared-secret") {
		cfg.SharedSecret = cmd.String("shared-secret")
	}
	if cmd.IsSet("client-shared-secret") {
		cfg.ClientSecret = cmd.String("client-shared-secret")
	}
	if cmd.IsSet("enable-persistence") {
		cfg.EnablePersistence = cmd.Bool("enable-persistence")
	}
	if cmd.IsSet("data-dir") {
		cfg.DataDir = cmd.String("data-dir")
	}
	if cmd.IsSet("snapshot-interval-s") {
		cfg.SnapshotIntervalS = cmd.Int("snapshot-interval-s")
	}
	if cmd.IsSet("fsync") {
		cfg.FsyncPolicy = cmd.String("fsync")
	}
	if cmd.IsSet("max-wal-bytes") {
		cfg.MaxWALBytes = cmd.Uint64("max-wal-bytes")
	}
	if cmd.IsSet("max-wal-entries") {
		cfg.MaxWALEntries = cmd.Uint64("max-wal-entries")
	}
	if cmd.IsSet("rpc-timeout-ms") {
		cfg.RPCTimeoutMs = cmd.Int("rpc-timeout-ms")
	}
	if cmd.IsSet("anti-entropy-fetch-bytes") {
		cfg.AEFetchBytes = cmd.Int("anti-entropy-fetch-bytes")
	}
	if cmd.IsSet("max-hint-bytes") {
		cfg.MaxHintBytes = cmd.Uint64("max-hint-bytes")
	}
	if cmd.IsSet("tombstone-gc-ms") {
		cfg.TombstoneGCMS = cmd.Int("tombstone-gc-ms")
	}
	if cmd.IsSet("shutdown-timeout-ms") {
		cfg.ShutdownTimeoutMs = cmd.Int("shutdown-timeout-ms")
	}
	if cmd.IsSet("eviction-policy") {
		cfg.EvictionPolicy = cmd.String("eviction-policy")
	}
	if cmd.IsSet("log-level") {
		cfg.LogLevel = cmd.String("log-level")
	}
	if cmd.IsSet("config") {
		cfg.configFile = cmd.String("config")
	}

	cfg.SharedSecret = configuredSecret(cfg.SharedSecret, "CINDER_SHARED_SECRET")
	cfg.ClientSecret = configuredSecret(cfg.ClientSecret, "CINDER_CLIENT_SECRET")
	return cfg
}

// overlayConfigFile applies a JSON config file onto base. Unknown keys are
// rejected; absent keys keep the base values.
func overlayConfigFile(base daemonConfig, path string) (daemonConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return base, err
	}

	decoded := base
	if err := json.Unmarshal(data, &decoded, json.RejectUnknownMembers(true)); err != nil {
		return base, fmt.Errorf("invalid --config file: %w", err)
	}

	var keys map[string]any
	if err := json.Unmarshal(data, &keys); err == nil {
		if _, ok := keys["node-id"]; ok {
			decoded.nodeIDExplicit = true
		}
	}
	return decoded, nil
}

func resolveDaemonConfig(cmd *cli.Command, configPath string) (daemonConfig, error) {
	base := defaultDaemonConfig()
	if configPath != "" {
		var err error
		base, err = overlayConfigFile(base, configPath)
		if err != nil {
			return base, err
		}
	}

	cfg := readDaemonConfigWithBase(cmd, base)
	cfg.configFile = configPath
	return cfg, nil
}

// ReloadableFromDaemon projects the reloadable subset of a daemon config.
func ReloadableFromDaemon(cfg daemonConfig) tcp.ReloadableConfig {
	return tcp.ReloadableConfig{
		LogLevel:         cfg.LogLevel,
		GossipIntervalMs: int64(cfg.GossipIntervalMs),
		PingIntervalMs:   int64(cfg.ProbeIntervalMs),
		SuspectTimeoutMs: int64(cfg.SuspectMs),
		AEIntervalMs:     int64(cfg.AEIntervalMs),
		TLSCertFile:      cfg.TLSCert,
		TLSKeyFile:       cfg.TLSKey,
		Capacity:         cfg.Capacity,
		TombstoneGCMs:    int64(cfg.TombstoneGCMS),
		MaxHintBytes:     cfg.MaxHintBytes,
	}
}
