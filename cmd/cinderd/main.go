// Command cinderd runs a Cinder cache node and its configured cluster services.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

// version is the release tag, optionally set with -ldflags
// "-X main.version=X". When it remains "dev", buildVersion reports the
// module version or VCS revision embedded by the Go toolchain when available.
var version = "dev"

// buildVersion reports the stamped version, then the module version, then
// a short VCS revision, and finally "dev" when build metadata is unavailable.
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

func main() {
	var (
		port               = flag.Int("port", 7000, "TCP listen port")
		bindHost           = flag.String("bind", "127.0.0.1", "TCP listener bind host or IP")
		advertiseHost      = flag.String("advertise-host", "", "reachable host advertised to cluster peers (defaults to --bind; required with wildcard --bind)")
		nodeID             = flag.String("node-id", "node1", "stable node identifier")
		capacity           = flag.Uint64("capacity", 67_108_864, "store capacity in bytes")
		peers              = flag.String("peers", "", "comma-separated id@host:port cluster members")
		replFact           = flag.Int("replication-factor", 1, "replica count (1 = none)")
		consist            = flag.String("consistency", "async", "write consistency: async|quorum")
		gossipMs           = flag.Int("gossip-interval-ms", 1000, "gossip round interval")
		probeMs            = flag.Int("ping-interval-ms", 1000, "failure-detector probe interval")
		suspectMs          = flag.Int("suspect-timeout-ms", 3000, "suspect and dead escalation timeout")
		gossipRPCTimeoutMs = flag.Int("gossip-rpc-timeout-ms", 0, "gossip/probe RPC deadline in milliseconds (0 = min(ping interval, 1s))")
		aeInterval         = flag.Int("anti-entropy-interval-ms", 30000, "anti-entropy round interval")
		aeBuckets          = flag.Int("anti-entropy-buckets", 256, "anti-entropy digest buckets")
		quarantineMs       = flag.Int("quarantine-interval-ms", 10000, "rejoin warming window before a node receives migrated keys (0 = disabled)")
		grpcPort           = flag.Int("grpc-port", 0, "gRPC gateway port (0 = disabled)")
		grpcBind           = flag.String("grpc-bind", "127.0.0.1", "gRPC gateway bind host or IP")
		metricsPort        = flag.Int("metrics-port", 0, "Prometheus /metrics HTTP port (0 = disabled)")
		metricsBind        = flag.String("metrics-bind", "127.0.0.1", "metrics and pprof bind host or IP")
		tls                = flag.Bool("tls", false, "enable TLS encryption")
		tlsCert            = flag.String("tls-cert", "", "path to TLS certificate chain (PEM)")
		tlsKey             = flag.String("tls-key", "", "path to TLS private key (PEM)")
		tlsCA              = flag.String("tls-ca", "", "path to CA certificate for peer verification (PEM)")
		sharedSecret       = flag.String("shared-secret", "", "shared secret for node authentication (or CINDER_SHARED_SECRET)")
		clientSecret       = flag.String("client-shared-secret", "", "shared secret for client authentication, requires --tls (or CINDER_CLIENT_SECRET)")
		enablePersist      = flag.Bool("enable-persistence", false, "enable WAL + snapshots")
		dataDir            = flag.String("data-dir", "", "persistence directory (required with --enable-persistence)")
		snapshotSecs       = flag.Int("snapshot-interval-s", 60, "seconds between automatic snapshots (0 = disabled)")
		maxWALBytes        = flag.Uint64("max-wal-bytes", 64<<20, "active WAL bytes before out-of-cycle compact (0 = disabled)")
		maxWALEntries      = flag.Uint64("max-wal-entries", 10000, "active WAL records before out-of-cycle compact (0 = disabled)")
		rpcTimeoutMs       = flag.Int("rpc-timeout-ms", 5000, "replication and anti-entropy RPC deadline in milliseconds (0 = no timeout)")
		aeFetchBytes       = flag.Int("anti-entropy-fetch-bytes", 1<<20, "per-round anti-entropy repair fetch budget in bytes")
		maxHintBytes       = flag.Uint64("max-hint-bytes", 0, "hinted-handoff byte budget (0 = default 64MiB)")
		tombstoneMs        = flag.Int("tombstone-gc-ms", 0, "delete-marker GC grace in milliseconds (0 = default 5min)")
		evictPolicy        = flag.String("eviction-policy", "lru", "eviction policy: lru|lfu")
		logLevel           = flag.String("log-level", "info", "log level: trace|debug|info|warn|error")
		showVers           = flag.Bool("version", false, "print version and exit")
	)

	flag.Parse()
	if *showVers {
		fmt.Printf("cinderd (go) %s\n", buildVersion())
		return
	}

	selfHost, err := validateListenerHosts(*bindHost, *advertiseHost, *metricsBind, *grpcBind)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	logger := log.New(os.Stderr, lvl)
	peerList, err := tcp.ParsePeers(*peers)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}
	if err := validateClusterPort(*port, len(peerList)); err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	gossipRPCTimeout, err := clusterRPCTimeout(*probeMs, *suspectMs, *gossipMs, *gossipRPCTimeoutMs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	replicaCount, err := hashring.NewReplicaCount(*replFact)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd: --replication-factor must be between 1 and 32")
		os.Exit(2)
	}

	policy, err := store.ParsePolicy(*evictPolicy)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd: --eviction-policy must be lru|lfu")
		os.Exit(2)
	}

	var st *store.Store
	switch policy {
	case store.LRUPolicy:
		st = store.NewLRUStore(*capacity, nil)
	case store.LFUPolicy:
		st = store.NewLFUStore(*capacity, nil)
	}
	if err := validateCoreFlags(*port, *aeBuckets, *tombstoneMs); err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}
	if *tombstoneMs > 0 {
		st.SetTombstoneGCInterval(time.Duration(*tombstoneMs) * time.Millisecond)
	}

	srv := tcp.NewServer(tcp.ServerConfig{NodeID: *nodeID, Port: netport.Port(*port), AdvertiseHost: selfHost, Store: st})
	srv.SetLogger(&logger)
	logger.Info().Str("node", *nodeID).Str("bind", *bindHost).Str("advertise_host", selfHost).Int("port", *port).Msg("starting")
	srv.SetGossipRPCTimeout(gossipRPCTimeout)
	if err := srv.ConfigureCluster(peerList); err != nil {
		logger.Error().Err(err).Msg("cluster")
		os.Exit(2)
	}

	srv.SetGossipInterval(time.Duration(*gossipMs) * time.Millisecond)
	srv.SetProbeInterval(time.Duration(*probeMs) * time.Millisecond)
	srv.SetSuspectTimeout(time.Duration(*suspectMs) * time.Millisecond)
	srv.SetAntiEntropyInterval(time.Duration(*aeInterval) * time.Millisecond)
	srv.ConfigureAntiEntropy(uint32(*aeBuckets), *aeFetchBytes)
	if err := srv.ConfigureRebalance(replicaCount, time.Duration(*quarantineMs)*time.Millisecond); err != nil {
		logger.Error().Err(err).Msg("rebalance configuration")
		os.Exit(2)
	}
	if *tls {
		if *tlsCert == "" || *tlsKey == "" {
			logger.Error().Msg("--tls requires --tls-cert and --tls-key")
			os.Exit(2)
		}
		if err := srv.ConfigureTLS(*tlsCert, *tlsKey, *tlsCA); err != nil {
			logger.Error().Err(err).Msg("tls")
			os.Exit(2)
		}
	}

	shared := configuredSecret(*sharedSecret, "CINDER_SHARED_SECRET")
	clientSec := configuredSecret(*clientSecret, "CINDER_CLIENT_SECRET")
	if err := validateGRPCTLS(*grpcPort, *tls, clientSec); err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}
	if shared != "" || clientSec != "" {
		if err := srv.ConfigureSecrets(shared, clientSec); err != nil {
			logger.Error().Err(err).Msg("secrets")
			os.Exit(2)
		}
	}
	if *enablePersist {
		if *dataDir == "" {
			logger.Error().Msg("--data-dir is required with --enable-persistence")
			os.Exit(2)
		}
		if err := srv.ConfigurePersistence(*dataDir); err != nil {
			logger.Error().Err(err).Msg("persistence")
			os.Exit(2)
		}

		srv.SetSnapshotInterval(time.Duration(*snapshotSecs) * time.Second)
		srv.SetMaxWALBytes(*maxWALBytes)
		srv.SetMaxWALEntries(*maxWALEntries)
	}
	if replicaCount > 1 {
		mode, err := repl.ParseMode(*consist)
		if err != nil {
			logger.Error().Msg("--consistency must be async|quorum")
			os.Exit(2)
		}

		addrs := make([]client.NodeAddr, 0, len(peerList))
		for _, p := range peerList {
			addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
		}

		pool := client.NewPool(addrs, time.Duration(*rpcTimeoutMs)*time.Millisecond)
		if *tls {
			ccfg, err := tlscfg.Client(*tlsCert, *tlsKey, *tlsCA)
			if err != nil {
				logger.Error().Err(err).Msg("pool tls")
				os.Exit(2)
			}
			pool.SetTLS(ccfg)
		}
		if shared != "" {
			pool.SetSecrets(client.Secrets{NodeID: *nodeID, Shared: shared})
		}

		mgr := repl.NewManager(repl.Config{Local: st, Self: *nodeID, Clock: nil, Transport: pool})
		if *maxHintBytes > 0 {
			mgr.SetMaxHintBytes(*maxHintBytes)
		}
		if err := srv.ConfigureReplication(replicaCount, mode, mgr); err != nil {
			logger.Error().Err(err).Msg("replication configuration")
			os.Exit(2)
		}
		if *enablePersist {
			j, _, err := ae.OpenJournal(filepath.Join(*dataDir, "hints.log"), mgr.HintQueue())
			if err != nil {
				logger.Error().Err(err).Msg("hint journal")
				os.Exit(2)
			}
			mgr.SetHintJournal(j)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", net.JoinHostPort(*bindHost, strconv.Itoa(*port)))
	if err != nil {
		logger.Error().Err(err).Msg("listen")
		os.Exit(1)
	}

	var msrv *http.Server
	var gw *grpcgw.Gateway
	if *metricsPort > 0 {
		ml, err := lc.Listen(ctx, "tcp", net.JoinHostPort(*metricsBind, strconv.Itoa(*metricsPort)))
		if err != nil {
			logger.Error().Err(err).Msg("metrics listen")
			os.Exit(1)
		}

		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.NewHandler(srv.Metrics(), srv.MetricGauges))
		// Standard pprof endpoints on the same localhost-only listener,
		// so the docs/profiling.md workflow runs against a live node.
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
	if *grpcPort > 0 {
		gl, err := lc.Listen(ctx, "tcp", net.JoinHostPort(*grpcBind, strconv.Itoa(*grpcPort)))
		if err != nil {
			logger.Error().Err(err).Msg("grpc listen")
			os.Exit(1)
		}

		gw = grpcgw.New(srv)
		gw.SetClientSecret(clientSec)
		if *tls {
			if err := gw.SetTLS(*tlsCert, *tlsKey); err != nil {
				logger.Error().Err(err).Msg("grpc tls")
				os.Exit(2)
			}
		}

		go func() {
			if err := gw.Serve(gl); err != nil {
				logger.Error().Err(err).Msg("grpc")
			}
		}()
	}

	// Start the TCP server only after all listeners and gateway security
	// settings are configured. Serve starts gossip after registering the
	// listener, so peers cannot see a partially initialized daemon.
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
}
