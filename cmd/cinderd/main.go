// Command cinderd runs a Cinder cache node and its configured cluster services.
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/grpcgw"
	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/netport"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

var version = "dev"

func buildVersion() string {
	if version != "dev" {
		return version
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 7 {
			return "dev-" + s.Value[:7]
		}
	}
	return version
}

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
}

func defaultDaemonConfig() daemonConfig {
	return daemonConfig{
		Port: 7000, BindHost: "127.0.0.1", NodeID: "node1", Capacity: 67_108_864,
		ReplicationFactor: 1, Consistency: "async", GossipIntervalMs: 1000,
		ProbeIntervalMs: 1000, SuspectMs: 3000, AEIntervalMs: 30000,
		AEBuckets: 256, QuarantineIntervalMs: 10000, GRPCBind: "127.0.0.1",
		MetricsBind: "127.0.0.1", SnapshotIntervalS: 60, MaxWALBytes: 64 << 20,
		MaxWALEntries: 10000, RPCTimeoutMs: 5000, AEFetchBytes: 1 << 20,
		EvictionPolicy: "lru", LogLevel: "info",
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
		&cli.Uint64Flag{Name: "max-wal-bytes", Value: cfg.MaxWALBytes, Usage: "active WAL bytes before out-of-cycle compact (0 = disabled)"},
		&cli.Uint64Flag{Name: "max-wal-entries", Value: cfg.MaxWALEntries, Usage: "active WAL records before out-of-cycle compact (0 = disabled)"},
		&cli.IntFlag{Name: "rpc-timeout-ms", Value: cfg.RPCTimeoutMs, Usage: "replication and anti-entropy RPC deadline in milliseconds (0 = no timeout)"},
		&cli.IntFlag{Name: "anti-entropy-fetch-bytes", Value: cfg.AEFetchBytes, Usage: "per-round anti-entropy repair fetch budget in bytes"},
		&cli.Uint64Flag{Name: "max-hint-bytes", Value: cfg.MaxHintBytes, Usage: "hinted-handoff byte budget (0 = default 64MiB)"},
		&cli.IntFlag{Name: "tombstone-gc-ms", Value: cfg.TombstoneGCMS, Usage: "delete-marker GC grace in milliseconds (0 = default 5min)"},
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

func run(args []string, stdout, stderr io.Writer) int {
	cfg := defaultDaemonConfig()
	root := &cli.Command{
		Name:                   "cinderd",
		Usage:                  "run a Cinder cache node",
		Version:                buildVersion(),
		HideVersion:            true,
		Suggest:                false,
		Writer:                 stdout,
		ErrWriter:              stderr,
		HideHelpCommand:        true,
		UseShortOptionHandling: true,
		ExitErrHandler:         func(context.Context, *cli.Command, error) {},
		Flags:                  cfg.flags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if cmd.Bool("version") {
				fmt.Fprintf(stdout, "cinderd (go) %s\n", buildVersion())
				return nil
			}

			cfg, err := resolveDaemonConfig(cmd, cmd.String("config"))
			if err != nil {
				return daemonUsageError(err)
			}
			return runDaemon(ctx, cfg, stderr, func() (tcp.ReloadableConfig, error) {
				fresh, err := resolveDaemonConfig(cmd, cmd.String("config"))
				if err != nil {
					return tcp.ReloadableConfig{}, err
				}
				return ReloadableFromDaemon(fresh), nil
			})
		},
	}

	err := root.Run(context.Background(), append([]string{"cinderd"}, args...))
	if err != nil {
		fmt.Fprintln(stderr, err)
		if ec, ok := errors.AsType[cli.ExitCoder](err); ok {
			return ec.ExitCode()
		}
		return 2
	}
	return 0
}

func configureSecurityAndPersistence(srv *tcp.Server, cfg daemonConfig) error {
	if cfg.TLS {
		if cfg.TLSCert == "" || cfg.TLSKey == "" {
			return fmt.Errorf("--tls requires --tls-cert and --tls-key")
		}
		if err := srv.ConfigureTLS(cfg.TLSCert, cfg.TLSKey, cfg.TLSCA); err != nil {
			return fmt.Errorf("tls: %w", err)
		}
	}
	if cfg.SharedSecret != "" || cfg.ClientSecret != "" {
		if err := srv.ConfigureSecrets(cfg.SharedSecret, cfg.ClientSecret); err != nil {
			return fmt.Errorf("secrets: %w", err)
		}
	}
	if cfg.EnablePersistence {
		if cfg.DataDir == "" {
			return fmt.Errorf("--data-dir is required with --enable-persistence")
		}
		if err := srv.ConfigurePersistence(cfg.DataDir); err != nil {
			return fmt.Errorf("persistence: %w", err)
		}

		srv.SetSnapshotInterval(time.Duration(cfg.SnapshotIntervalS) * time.Second)
		srv.SetMaxWALBytes(cfg.MaxWALBytes)
		srv.SetMaxWALEntries(cfg.MaxWALEntries)
	}
	return nil
}

func configureReplication(srv *tcp.Server, st *store.Store, cfg daemonConfig, peerList []tcp.Peer, replicaCount hashring.ReplicaCount) error {
	if replicaCount <= 1 {
		return nil
	}

	mode, err := repl.ParseMode(cfg.Consistency)
	if err != nil {
		return daemonUsageError(errors.New("--consistency must be async|quorum"))
	}

	addrs := make([]client.NodeAddr, 0, len(peerList))
	for _, p := range peerList {
		addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
	}

	pool := client.NewPool(addrs, time.Duration(cfg.RPCTimeoutMs)*time.Millisecond)
	if cfg.TLS {
		ccfg, err := tlscfg.Client(cfg.TLSCert, cfg.TLSKey, cfg.TLSCA)
		if err != nil {
			return daemonUsageError(fmt.Errorf("pool tls: %w", err))
		}
		pool.SetTLS(ccfg)
	}
	if cfg.SharedSecret != "" {
		pool.SetSecrets(client.Secrets{NodeID: cfg.NodeID, Shared: cfg.SharedSecret})
	}

	mgr := repl.NewManager(repl.Config{Local: st, Self: cfg.NodeID, Clock: nil, Transport: pool})
	if cfg.MaxHintBytes > 0 {
		mgr.SetMaxHintBytes(cfg.MaxHintBytes)
	}
	if err := srv.ConfigureReplication(replicaCount, mode, mgr); err != nil {
		return daemonUsageError(fmt.Errorf("replication configuration: %w", err))
	}
	if cfg.EnablePersistence {
		j, _, err := ae.OpenJournal(filepath.Join(cfg.DataDir, "hints.log"), mgr.HintQueue())
		if err != nil {
			return daemonUsageError(fmt.Errorf("hint journal: %w", err))
		}
		mgr.SetHintJournal(j)
	}
	return nil
}

func daemonUsageError(err error) error   { return cli.Exit("cinderd: "+err.Error(), 2) }
func daemonRuntimeError(err error) error { return cli.Exit("cinderd: "+err.Error(), 1) }

// validatedConfig carries the parsed outputs of flag validation so runDaemon
// does not parse anything twice.
type validatedConfig struct {
	selfHost      string
	peerList      []tcp.Peer
	replicaCount  hashring.ReplicaCount
	policy        store.Policy
	gossipTimeout time.Duration
	warns         []string
}

// validateDaemonConfig runs every startup validation and returns all
// failures at once, so one bad invocation reports everything instead of
// forcing fix-and-rerun cycles. Warnings cover valid-but-ineffective
// settings; they are logged after the logger exists.
func validateDaemonConfig(cfg daemonConfig) (validatedConfig, []error) {
	var out validatedConfig
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}

	selfHost, err := validateListenerHosts(cfg.BindHost, cfg.AdvertiseHost, cfg.MetricsBind, cfg.GRPCBind)
	fail(err)

	out.selfHost = selfHost
	fail(validateAuxPortRange(cfg.MetricsPort, cfg.GRPCPort))

	errs = append(errs, validateCoreFlags(cfg.Port, cfg.AEBuckets, cfg.TombstoneGCMS)...)
	peerList, err := tcp.ParsePeers(cfg.Peers)
	fail(err)

	out.peerList = peerList
	fail(validateClusterPort(cfg.Port, len(peerList)))

	gossipTimeout, err := clusterRPCTimeout(cfg.ProbeIntervalMs, cfg.SuspectMs, cfg.GossipIntervalMs, cfg.GossipRPCTimeoutMs)
	fail(err)

	out.gossipTimeout = gossipTimeout
	fail(validateGRPCTLS(cfg.GRPCPort, cfg.TLS, cfg.ClientSecret))

	replicaCount, err := hashring.NewReplicaCount(cfg.ReplicationFactor)
	if err != nil {
		errs = append(errs, fmt.Errorf("--replication-factor must be between 1 and 32: %w", err))
	}

	out.replicaCount = replicaCount
	policy, err := store.ParsePolicy(cfg.EvictionPolicy)
	fail(err)

	out.policy = policy
	_, err = log.ParseLevel(cfg.LogLevel)
	fail(err)
	fail(validateNonNegativeIntervals(cfg.AEIntervalMs, cfg.QuarantineIntervalMs, cfg.SnapshotIntervalS, cfg.RPCTimeoutMs, cfg.AEFetchBytes))

	fail(validateCapacity(cfg.Capacity))
	fail(validateListenerPortCollisions(cfg.Port, cfg.GRPCPort, cfg.MetricsPort))
	if _, err := repl.ParseMode(cfg.Consistency); err != nil {
		errs = append(errs, errors.New("--consistency must be async|quorum"))
	} else if cfg.ReplicationFactor == 1 {
		out.warns = append(out.warns, "--consistency is ignored at replication-factor 1")
	}

	if cfg.MaxHintBytes > 0 && cfg.ReplicationFactor == 1 {
		out.warns = append(out.warns, "--max-hint-bytes has no effect at replication-factor 1")
	}
	return out, errs
}

func runDaemon(parent context.Context, cfg daemonConfig, stderr io.Writer, onReload func() (tcp.ReloadableConfig, error)) error {
	vc, errs := validateDaemonConfig(cfg)
	if len(errs) > 0 {
		msgs := make([]string, 0, len(errs))
		for _, err := range errs {
			msgs = append(msgs, "cinderd: "+err.Error())
		}
		return cli.Exit(strings.Join(msgs, "\n"), 2)
	}

	selfHost, gossipTimeout, peerList := vc.selfHost, vc.gossipTimeout, vc.peerList
	replicaCount, policy := vc.replicaCount, vc.policy
	lvl, _ := log.ParseLevel(cfg.LogLevel)
	logger := log.New(stderr, lvl)
	for _, w := range vc.warns {
		logger.Warn().Msg(w)
	}

	var st *store.Store
	switch policy {
	case store.LRUPolicy:
		st = store.NewLRUStore(cfg.Capacity, nil)
	case store.LFUPolicy:
		st = store.NewLFUStore(cfg.Capacity, nil)
	}
	if cfg.TombstoneGCMS > 0 {
		st.SetTombstoneGCInterval(time.Duration(cfg.TombstoneGCMS) * time.Millisecond)
	}

	srv := tcp.NewServer(tcp.ServerConfig{NodeID: cfg.NodeID, Port: netport.Port(cfg.Port), AdvertiseHost: selfHost, Store: st})
	srv.SetLogger(&logger)
	logger.Info().Str("node", cfg.NodeID).Str("bind", cfg.BindHost).Str("advertise_host", selfHost).Int("port", cfg.Port).Msg("starting")
	srv.SetGossipRPCTimeout(gossipTimeout)
	if err := srv.ConfigureCluster(peerList); err != nil {
		return daemonUsageError(fmt.Errorf("cluster: %w", err))
	}

	srv.SetGossipInterval(time.Duration(cfg.GossipIntervalMs) * time.Millisecond)
	srv.SetProbeInterval(time.Duration(cfg.ProbeIntervalMs) * time.Millisecond)
	srv.SetSuspectTimeout(time.Duration(cfg.SuspectMs) * time.Millisecond)
	srv.SetAntiEntropyInterval(time.Duration(cfg.AEIntervalMs) * time.Millisecond)
	srv.ConfigureAntiEntropy(uint32(cfg.AEBuckets), cfg.AEFetchBytes)
	if err := srv.ConfigureRebalance(replicaCount, time.Duration(cfg.QuarantineIntervalMs)*time.Millisecond); err != nil {
		return daemonUsageError(fmt.Errorf("rebalance configuration: %w", err))
	}
	if err := configureSecurityAndPersistence(srv, cfg); err != nil {
		return err
	}
	if err := configureReplication(srv, st, cfg, peerList, replicaCount); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.BindHost, strconv.Itoa(cfg.Port)))
	if err != nil {
		return daemonRuntimeError(fmt.Errorf("listen: %w", err))
	}

	defer l.Close()
	var msrv *http.Server
	var gw *grpcgw.Gateway
	if cfg.MetricsPort > 0 {
		ml, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.MetricsBind, strconv.Itoa(cfg.MetricsPort)))
		if err != nil {
			return daemonRuntimeError(fmt.Errorf("metrics listen: %w", err))
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.NewHandler(srv.Metrics(), srv.MetricGauges))
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		msrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := msrv.Serve(ml); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error().Err(err).Msg("metrics")
			}
		}()
	}
	if cfg.GRPCPort > 0 {
		gl, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.GRPCBind, strconv.Itoa(cfg.GRPCPort)))
		if err != nil {
			return daemonRuntimeError(fmt.Errorf("grpc listen: %w", err))
		}

		gw = grpcgw.New(srv)
		gw.SetClientSecret(cfg.ClientSecret)
		if cfg.TLS {
			if err := gw.SetTLS(cfg.TLSCert, cfg.TLSKey); err != nil {
				return daemonUsageError(fmt.Errorf("grpc tls: %w", err))
			}
		}
		go func() {
			if err := gw.Serve(gl); err != nil {
				logger.Error().Err(err).Msg("grpc")
			}
		}()
	}
	go func() {
		if err := srv.Serve(l); err != nil {
			logger.Error().Err(err).Msg("serve")
		}
	}()

	if onReload != nil {
		hupCh := make(chan os.Signal, 1)
		signal.Notify(hupCh, syscall.SIGHUP)
		defer signal.Stop(hupCh)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-hupCh:
					rc, err := onReload()
					if err != nil {
						logger.Error().Err(err).Msg("config reload failed")
						continue
					}

					applied, err := srv.Reload(rc)
					if err != nil {
						logger.Error().Err(err).Msg("config reload failed")
						continue
					}
					logger.Info().Strs("applied", applied).Msg("config reloaded")
				}
			}
		}()
	}

	<-ctx.Done()
	stop()
	logger.Info().Msg("shutting down")
	if msrv != nil {
		msrv.Close()
	}
	if gw != nil {
		gw.Stop()
	}

	srv.Shutdown()
	return nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
