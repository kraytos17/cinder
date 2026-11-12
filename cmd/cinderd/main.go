// Command cinderd runs a Cinder cache node and its configured cluster services.
package main

import (
	"context"
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
	port, grpcPort, metricsPort                                       int
	bindHost, advertiseHost, grpcBind, metricsBind                    string
	nodeID, peers, consistency, evictionPolicy, logLevel              string
	replicationFactor, gossipIntervalMs, probeIntervalMs, suspectMs   int
	gossipRPCTimeoutMs, aeIntervalMs, aeBuckets, quarantineIntervalMs int
	capacity, maxWALBytes, maxWALEntries, maxHintBytes                uint64
	tls, enablePersistence                                            bool
	tlsCert, tlsKey, tlsCA, sharedSecret, clientSecret, dataDir       string
	snapshotIntervalS, rpcTimeoutMs, aeFetchBytes, tombstoneGCMS      int
}

func defaultDaemonConfig() daemonConfig {
	return daemonConfig{
		port: 7000, bindHost: "127.0.0.1", nodeID: "node1", capacity: 67_108_864,
		replicationFactor: 1, consistency: "async", gossipIntervalMs: 1000,
		probeIntervalMs: 1000, suspectMs: 3000, aeIntervalMs: 30000,
		aeBuckets: 256, quarantineIntervalMs: 10000, grpcBind: "127.0.0.1",
		metricsBind: "127.0.0.1", snapshotIntervalS: 60, maxWALBytes: 64 << 20,
		maxWALEntries: 10000, rpcTimeoutMs: 5000, aeFetchBytes: 1 << 20,
		evictionPolicy: "lru", logLevel: "info",
	}
}

func (cfg daemonConfig) flags() []cli.Flag {
	return []cli.Flag{
		&cli.IntFlag{Name: "port", Value: cfg.port, Usage: "TCP listen port"},
		&cli.StringFlag{Name: "bind", Value: cfg.bindHost, Usage: "TCP listener bind host or IP"},
		&cli.StringFlag{Name: "advertise-host", Value: cfg.advertiseHost, Usage: "reachable host advertised to cluster peers (defaults to --bind; required with wildcard --bind)"},
		&cli.StringFlag{Name: "node-id", Value: cfg.nodeID, Usage: "stable node identifier"},
		&cli.Uint64Flag{Name: "capacity", Value: cfg.capacity, Usage: "store capacity in bytes"},
		&cli.StringFlag{Name: "peers", Value: cfg.peers, Usage: "comma-separated id@host:port cluster members"},
		&cli.IntFlag{Name: "replication-factor", Value: cfg.replicationFactor, Usage: "replica count (1 = none)"},
		&cli.StringFlag{Name: "consistency", Value: cfg.consistency, Usage: "write consistency: async|quorum"},
		&cli.IntFlag{Name: "gossip-interval-ms", Value: cfg.gossipIntervalMs, Usage: "gossip round interval"},
		&cli.IntFlag{Name: "ping-interval-ms", Value: cfg.probeIntervalMs, Usage: "failure-detector probe interval"},
		&cli.IntFlag{Name: "suspect-timeout-ms", Value: cfg.suspectMs, Usage: "suspect and dead escalation timeout"},
		&cli.IntFlag{Name: "gossip-rpc-timeout-ms", Value: cfg.gossipRPCTimeoutMs, Usage: "gossip/probe RPC deadline in milliseconds (0 = min(ping interval, 1s))"},
		&cli.IntFlag{Name: "anti-entropy-interval-ms", Value: cfg.aeIntervalMs, Usage: "anti-entropy round interval"},
		&cli.IntFlag{Name: "anti-entropy-buckets", Value: cfg.aeBuckets, Usage: "anti-entropy digest buckets"},
		&cli.IntFlag{Name: "quarantine-interval-ms", Value: cfg.quarantineIntervalMs, Usage: "rejoin warming window (0 = disabled)"},
		&cli.IntFlag{Name: "grpc-port", Value: cfg.grpcPort, Usage: "gRPC gateway port (0 = disabled)"},
		&cli.StringFlag{Name: "grpc-bind", Value: cfg.grpcBind, Usage: "gRPC gateway bind host or IP"},
		&cli.IntFlag{Name: "metrics-port", Value: cfg.metricsPort, Usage: "Prometheus /metrics HTTP port (0 = disabled)"},
		&cli.StringFlag{Name: "metrics-bind", Value: cfg.metricsBind, Usage: "metrics and pprof bind host or IP"},
		&cli.BoolFlag{Name: "tls", Value: cfg.tls, Usage: "enable TLS encryption"},
		&cli.StringFlag{Name: "tls-cert", Value: cfg.tlsCert, Usage: "path to TLS certificate chain (PEM)"},
		&cli.StringFlag{Name: "tls-key", Value: cfg.tlsKey, Usage: "path to TLS private key (PEM)"},
		&cli.StringFlag{Name: "tls-ca", Value: cfg.tlsCA, Usage: "path to CA certificate for peer verification (PEM)"},
		&cli.StringFlag{Name: "shared-secret", Value: cfg.sharedSecret, Usage: "shared secret for node authentication (or CINDER_SHARED_SECRET)"},
		&cli.StringFlag{Name: "client-shared-secret", Value: cfg.clientSecret, Usage: "shared secret for client authentication, requires --tls (or CINDER_CLIENT_SECRET)"},
		&cli.BoolFlag{Name: "enable-persistence", Value: cfg.enablePersistence, Usage: "enable WAL + snapshots"},
		&cli.StringFlag{Name: "data-dir", Value: cfg.dataDir, Usage: "persistence directory (required with --enable-persistence)"},
		&cli.IntFlag{Name: "snapshot-interval-s", Value: cfg.snapshotIntervalS, Usage: "seconds between automatic snapshots (0 = disabled)"},
		&cli.Uint64Flag{Name: "max-wal-bytes", Value: cfg.maxWALBytes, Usage: "active WAL bytes before out-of-cycle compact (0 = disabled)"},
		&cli.Uint64Flag{Name: "max-wal-entries", Value: cfg.maxWALEntries, Usage: "active WAL records before out-of-cycle compact (0 = disabled)"},
		&cli.IntFlag{Name: "rpc-timeout-ms", Value: cfg.rpcTimeoutMs, Usage: "replication and anti-entropy RPC deadline in milliseconds (0 = no timeout)"},
		&cli.IntFlag{Name: "anti-entropy-fetch-bytes", Value: cfg.aeFetchBytes, Usage: "per-round anti-entropy repair fetch budget in bytes"},
		&cli.Uint64Flag{Name: "max-hint-bytes", Value: cfg.maxHintBytes, Usage: "hinted-handoff byte budget (0 = default 64MiB)"},
		&cli.IntFlag{Name: "tombstone-gc-ms", Value: cfg.tombstoneGCMS, Usage: "delete-marker GC grace in milliseconds (0 = default 5min)"},
		&cli.StringFlag{Name: "eviction-policy", Value: cfg.evictionPolicy, Usage: "eviction policy: lru|lfu"},
		&cli.StringFlag{Name: "log-level", Value: cfg.logLevel, Usage: "log level: trace|debug|info|warn|error"},
		&cli.BoolFlag{Name: "version", Value: false, Usage: "print version and exit"},
	}
}

func readDaemonConfig(cmd *cli.Command) daemonConfig {
	cfg := daemonConfig{
		port: cmd.Int("port"), bindHost: cmd.String("bind"), advertiseHost: cmd.String("advertise-host"),
		nodeID: cmd.String("node-id"), capacity: cmd.Uint64("capacity"), peers: cmd.String("peers"),
		replicationFactor: cmd.Int("replication-factor"), consistency: cmd.String("consistency"),
		gossipIntervalMs: cmd.Int("gossip-interval-ms"), probeIntervalMs: cmd.Int("ping-interval-ms"),
		suspectMs: cmd.Int("suspect-timeout-ms"), gossipRPCTimeoutMs: cmd.Int("gossip-rpc-timeout-ms"),
		aeIntervalMs: cmd.Int("anti-entropy-interval-ms"), aeBuckets: cmd.Int("anti-entropy-buckets"),
		quarantineIntervalMs: cmd.Int("quarantine-interval-ms"), grpcPort: cmd.Int("grpc-port"),
		grpcBind: cmd.String("grpc-bind"), metricsPort: cmd.Int("metrics-port"), metricsBind: cmd.String("metrics-bind"),
		tls: cmd.Bool("tls"), tlsCert: cmd.String("tls-cert"), tlsKey: cmd.String("tls-key"), tlsCA: cmd.String("tls-ca"),
		sharedSecret: cmd.String("shared-secret"), clientSecret: cmd.String("client-shared-secret"),
		enablePersistence: cmd.Bool("enable-persistence"), dataDir: cmd.String("data-dir"),
		snapshotIntervalS: cmd.Int("snapshot-interval-s"), maxWALBytes: cmd.Uint64("max-wal-bytes"),
		maxWALEntries: cmd.Uint64("max-wal-entries"), rpcTimeoutMs: cmd.Int("rpc-timeout-ms"),
		aeFetchBytes: cmd.Int("anti-entropy-fetch-bytes"), maxHintBytes: cmd.Uint64("max-hint-bytes"),
		tombstoneGCMS: cmd.Int("tombstone-gc-ms"), evictionPolicy: cmd.String("eviction-policy"), logLevel: cmd.String("log-level"),
	}

	cfg.sharedSecret = configuredSecret(cfg.sharedSecret, "CINDER_SHARED_SECRET")
	cfg.clientSecret = configuredSecret(cfg.clientSecret, "CINDER_CLIENT_SECRET")
	return cfg
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
			return runDaemon(ctx, readDaemonConfig(cmd), stderr)
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
	if cfg.tls {
		if cfg.tlsCert == "" || cfg.tlsKey == "" {
			return fmt.Errorf("--tls requires --tls-cert and --tls-key")
		}
		if err := srv.ConfigureTLS(cfg.tlsCert, cfg.tlsKey, cfg.tlsCA); err != nil {
			return fmt.Errorf("tls: %w", err)
		}
	}
	if cfg.sharedSecret != "" || cfg.clientSecret != "" {
		if err := srv.ConfigureSecrets(cfg.sharedSecret, cfg.clientSecret); err != nil {
			return fmt.Errorf("secrets: %w", err)
		}
	}
	if cfg.enablePersistence {
		if cfg.dataDir == "" {
			return fmt.Errorf("--data-dir is required with --enable-persistence")
		}
		if err := srv.ConfigurePersistence(cfg.dataDir); err != nil {
			return fmt.Errorf("persistence: %w", err)
		}

		srv.SetSnapshotInterval(time.Duration(cfg.snapshotIntervalS) * time.Second)
		srv.SetMaxWALBytes(cfg.maxWALBytes)
		srv.SetMaxWALEntries(cfg.maxWALEntries)
	}
	return nil
}

func configureReplication(srv *tcp.Server, st *store.Store, cfg daemonConfig, peerList []tcp.Peer, replicaCount hashring.ReplicaCount) error {
	if replicaCount <= 1 {
		return nil
	}

	mode, err := repl.ParseMode(cfg.consistency)
	if err != nil {
		return daemonUsageError(errors.New("--consistency must be async|quorum"))
	}

	addrs := make([]client.NodeAddr, 0, len(peerList))
	for _, p := range peerList {
		addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
	}

	pool := client.NewPool(addrs, time.Duration(cfg.rpcTimeoutMs)*time.Millisecond)
	if cfg.tls {
		ccfg, err := tlscfg.Client(cfg.tlsCert, cfg.tlsKey, cfg.tlsCA)
		if err != nil {
			return daemonUsageError(fmt.Errorf("pool tls: %w", err))
		}
		pool.SetTLS(ccfg)
	}
	if cfg.sharedSecret != "" {
		pool.SetSecrets(client.Secrets{NodeID: cfg.nodeID, Shared: cfg.sharedSecret})
	}

	mgr := repl.NewManager(repl.Config{Local: st, Self: cfg.nodeID, Clock: nil, Transport: pool})
	if cfg.maxHintBytes > 0 {
		mgr.SetMaxHintBytes(cfg.maxHintBytes)
	}
	if err := srv.ConfigureReplication(replicaCount, mode, mgr); err != nil {
		return daemonUsageError(fmt.Errorf("replication configuration: %w", err))
	}
	if cfg.enablePersistence {
		j, _, err := ae.OpenJournal(filepath.Join(cfg.dataDir, "hints.log"), mgr.HintQueue())
		if err != nil {
			return daemonUsageError(fmt.Errorf("hint journal: %w", err))
		}
		mgr.SetHintJournal(j)
	}
	return nil
}

func daemonUsageError(err error) error   { return cli.Exit("cinderd: "+err.Error(), 2) }
func daemonRuntimeError(err error) error { return cli.Exit("cinderd: "+err.Error(), 1) }

func runDaemon(parent context.Context, cfg daemonConfig, stderr io.Writer) error {
	selfHost, err := validateListenerHosts(cfg.bindHost, cfg.advertiseHost, cfg.metricsBind, cfg.grpcBind)
	if err != nil {
		return daemonUsageError(err)
	}
	if cfg.metricsPort < 0 || cfg.metricsPort > 65535 || cfg.grpcPort < 0 || cfg.grpcPort > 65535 {
		return daemonUsageError(errors.New("--metrics-port and --grpc-port must be between 0 and 65535"))
	}
	if err := validateCoreFlags(cfg.port, cfg.aeBuckets, cfg.tombstoneGCMS); err != nil {
		return daemonUsageError(err)
	}

	peerList, err := tcp.ParsePeers(cfg.peers)
	if err != nil {
		return daemonUsageError(err)
	}
	if err := validateClusterPort(cfg.port, len(peerList)); err != nil {
		return daemonUsageError(err)
	}

	gossipTimeout, err := clusterRPCTimeout(cfg.probeIntervalMs, cfg.suspectMs, cfg.gossipIntervalMs, cfg.gossipRPCTimeoutMs)
	if err != nil {
		return daemonUsageError(err)
	}
	if err := validateGRPCTLS(cfg.grpcPort, cfg.tls, cfg.clientSecret); err != nil {
		return daemonUsageError(err)
	}

	replicaCount, err := hashring.NewReplicaCount(cfg.replicationFactor)
	if err != nil {
		return daemonUsageError(errors.New("--replication-factor must be between 1 and 32"))
	}

	policy, err := store.ParsePolicy(cfg.evictionPolicy)
	if err != nil {
		return daemonUsageError(errors.New("--eviction-policy must be lru|lfu"))
	}

	lvl, err := log.ParseLevel(cfg.logLevel)
	if err != nil {
		return daemonUsageError(err)
	}

	logger := log.New(stderr, lvl)
	var st *store.Store
	switch policy {
	case store.LRUPolicy:
		st = store.NewLRUStore(cfg.capacity, nil)
	case store.LFUPolicy:
		st = store.NewLFUStore(cfg.capacity, nil)
	}
	if cfg.tombstoneGCMS > 0 {
		st.SetTombstoneGCInterval(time.Duration(cfg.tombstoneGCMS) * time.Millisecond)
	}

	srv := tcp.NewServer(tcp.ServerConfig{NodeID: cfg.nodeID, Port: netport.Port(cfg.port), AdvertiseHost: selfHost, Store: st})
	srv.SetLogger(&logger)
	logger.Info().Str("node", cfg.nodeID).Str("bind", cfg.bindHost).Str("advertise_host", selfHost).Int("port", cfg.port).Msg("starting")
	srv.SetGossipRPCTimeout(gossipTimeout)
	if err := srv.ConfigureCluster(peerList); err != nil {
		return daemonUsageError(fmt.Errorf("cluster: %w", err))
	}

	srv.SetGossipInterval(time.Duration(cfg.gossipIntervalMs) * time.Millisecond)
	srv.SetProbeInterval(time.Duration(cfg.probeIntervalMs) * time.Millisecond)
	srv.SetSuspectTimeout(time.Duration(cfg.suspectMs) * time.Millisecond)
	srv.SetAntiEntropyInterval(time.Duration(cfg.aeIntervalMs) * time.Millisecond)
	srv.ConfigureAntiEntropy(uint32(cfg.aeBuckets), cfg.aeFetchBytes)
	if err := srv.ConfigureRebalance(replicaCount, time.Duration(cfg.quarantineIntervalMs)*time.Millisecond); err != nil {
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
	l, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.bindHost, strconv.Itoa(cfg.port)))
	if err != nil {
		return daemonRuntimeError(fmt.Errorf("listen: %w", err))
	}

	defer l.Close()
	var msrv *http.Server
	var gw *grpcgw.Gateway
	if cfg.metricsPort > 0 {
		ml, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.metricsBind, strconv.Itoa(cfg.metricsPort)))
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
	if cfg.grpcPort > 0 {
		gl, err := lc.Listen(ctx, "tcp", net.JoinHostPort(cfg.grpcBind, strconv.Itoa(cfg.grpcPort)))
		if err != nil {
			return daemonRuntimeError(fmt.Errorf("grpc listen: %w", err))
		}

		gw = grpcgw.New(srv)
		gw.SetClientSecret(cfg.clientSecret)
		if cfg.tls {
			if err := gw.SetTLS(cfg.tlsCert, cfg.tlsKey); err != nil {
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
