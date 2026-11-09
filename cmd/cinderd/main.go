// Command cinderd is the Cinder distributed cache server (Go port).
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
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/metrics"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

// version is the release tag, stamped by the build. Plain `go build`
// leaves it "dev"; use -ldflags "-X main.version=X" for releases, or
// rely on the build-info fallback below (module version + VCS revision
// when built from a clean tree).
var version = "dev"

// buildVersion reports the stamped version, falling back to the module
// build info (and finally "dev") when no tag was stamped.
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
		port          = flag.Int("port", 7000, "TCP listen port")
		nodeID        = flag.String("node-id", "node1", "stable node identifier")
		capacity      = flag.Uint64("capacity", 67_108_864, "store capacity in bytes")
		peers         = flag.String("peers", "", "comma-separated id@host:port cluster members")
		replFact      = flag.Int("replication-factor", 1, "replica count (1 = none)")
		consist       = flag.String("consistency", "async", "write consistency: async|quorum")
		gossipMs      = flag.Int("gossip-interval-ms", 1000, "gossip round interval")
		probeMs       = flag.Int("ping-interval-ms", 1000, "failure-detector probe interval")
		suspectMs     = flag.Int("suspect-timeout-ms", 3000, "suspect and dead escalation timeout")
		aeInterval    = flag.Int("anti-entropy-interval-ms", 30000, "anti-entropy round interval")
		aeBuckets     = flag.Int("anti-entropy-buckets", 256, "anti-entropy digest buckets")
		quarantineMs  = flag.Int("quarantine-interval-ms", 10000, "rejoin warming window before a node receives migrated keys (0 = disabled)")
		grpcPort      = flag.Int("grpc-port", 0, "gRPC gateway port (0 = disabled)")
		metricsPort   = flag.Int("metrics-port", 0, "Prometheus /metrics HTTP port (0 = disabled)")
		tls           = flag.Bool("tls", false, "enable TLS encryption")
		tlsCert       = flag.String("tls-cert", "", "path to TLS certificate chain (PEM)")
		tlsKey        = flag.String("tls-key", "", "path to TLS private key (PEM)")
		tlsCA         = flag.String("tls-ca", "", "path to CA certificate for peer verification (PEM)")
		sharedSecret  = flag.String("shared-secret", "", "shared secret for node authentication (or CINDER_SHARED_SECRET)")
		clientSecret  = flag.String("client-shared-secret", "", "shared secret for client authentication, requires --tls (or CINDER_CLIENT_SECRET)")
		enablePersist = flag.Bool("enable-persistence", false, "enable WAL + snapshots")
		dataDir       = flag.String("data-dir", "", "persistence directory (required with --enable-persistence)")
		snapshotSecs  = flag.Int("snapshot-interval-s", 60, "seconds between automatic snapshots (0 = disabled)")
		maxWALBytes   = flag.Uint64("max-wal-bytes", 64<<20, "active WAL bytes before out-of-cycle compact (0 = disabled)")
		maxWALEntries = flag.Uint64("max-wal-entries", 10000, "active WAL records before out-of-cycle compact (0 = disabled)")
		rpcTimeoutMs  = flag.Int("rpc-timeout-ms", 5000, "per-RPC deadline in milliseconds (0 = no timeout)")
		aeFetchBytes  = flag.Int("anti-entropy-fetch-bytes", 1<<20, "per-round anti-entropy repair fetch budget in bytes")
		maxHintBytes  = flag.Uint64("max-hint-bytes", 0, "hinted-handoff byte budget (0 = default 64MiB)")
		tombstoneMs   = flag.Int("tombstone-gc-ms", 0, "delete-marker GC grace in milliseconds (0 = default 5min)")
		evictPolicy   = flag.String("eviction-policy", "lru", "eviction policy: lru|lfu")
		logLevel      = flag.String("log-level", "info", "log level: trace|debug|info|warn|error")
		showVers      = flag.Bool("version", false, "print version and exit")
	)

	flag.Parse()
	if *showVers {
		fmt.Printf("cinderd (go) %s\n", buildVersion())
		return
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

	var st *store.Store
	switch *evictPolicy {
	case "lru":
		st = store.NewLRUStore(*capacity, nil)
	case "lfu":
		st = store.NewLFUStore(*capacity, nil)
	default:
		fmt.Fprintln(os.Stderr, "cinderd: --eviction-policy must be lru|lfu")
		os.Exit(2)
	}
	if *tombstoneMs < 0 {
		fmt.Fprintln(os.Stderr, "cinderd: --tombstone-gc-ms must be >= 0")
		os.Exit(2)
	}
	if *aeBuckets <= 0 {
		fmt.Fprintln(os.Stderr, "cinderd: --anti-entropy-buckets must be positive")
		os.Exit(2)
	}
	if *tombstoneMs > 0 {
		st.SetTombstoneGCInterval(time.Duration(*tombstoneMs) * time.Millisecond)
	}
	srv := tcp.NewServer(*nodeID, *port, st, nil)
	srv.SetLogger(&logger)
	logger.Info().Str("node", *nodeID).Int("port", *port).Msg("starting")
	if err := srv.ConfigureCluster(peerList); err != nil {
		logger.Error().Err(err).Msg("cluster")
		os.Exit(2)
	}

	srv.SetGossipInterval(time.Duration(*gossipMs) * time.Millisecond)
	srv.SetProbeInterval(time.Duration(*probeMs) * time.Millisecond)
	srv.SetSuspectTimeout(time.Duration(*suspectMs) * time.Millisecond)
	srv.SetAntiEntropyInterval(time.Duration(*aeInterval) * time.Millisecond)
	srv.ConfigureAntiEntropy(uint32(*aeBuckets), *aeFetchBytes)
	srv.ConfigureRebalance(*replFact, time.Duration(*quarantineMs)*time.Millisecond)
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

	shared := *sharedSecret
	if shared == "" {
		shared = os.Getenv("CINDER_SHARED_SECRET")
	}

	clientSec := *clientSecret
	if clientSec == "" {
		clientSec = os.Getenv("CINDER_CLIENT_SECRET")
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
	if *replFact > 1 {
		var mode repl.Mode
		switch *consist {
		case "async":
			mode = repl.Async
		case "quorum":
			mode = repl.Quorum
		default:
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

		mgr := repl.NewManager(st, *nodeID, nil, pool)
		if *maxHintBytes > 0 {
			mgr.SetMaxHintBytes(*maxHintBytes)
		}
		srv.ConfigureReplication(*replFact, mode, mgr)
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
	l, err := lc.Listen(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(*port)))
	if err != nil {
		logger.Error().Err(err).Msg("listen")
		os.Exit(1)
	}

	go func() {
		if err := srv.Serve(l); err != nil {
			logger.Error().Err(err).Msg("serve")
		}
	}()
	// Auxiliary servers close explicitly at shutdown (no defers: setup
	// failures os.Exit, which would skip deferred calls).
	var msrv *http.Server
	var gw *grpcgw.Gateway
	// Prometheus endpoint, mirroring C++ metrics_port (0 = disabled).
	// Plain HTTP like the gRPC gateway pre-TLS: scrape behind the same
	// firewall rules as the admin surface.
	if *metricsPort > 0 {
		ml, err := lc.Listen(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(*metricsPort)))
		if err != nil {
			logger.Error().Err(err).Msg("metrics listen")
			os.Exit(1)
		}
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.NewHandler(srv.Metrics(), srv.MetricGauges))
		// Standard pprof endpoints on the same localhost-only listener,
		// so the docs/profiling.md workflow runs against a live node.
		// Same firewall posture as /metrics above.
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
		gl, err := lc.Listen(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(*grpcPort)))
		if err != nil {
			logger.Error().Err(err).Msg("grpc listen")
			os.Exit(1)
		}

		gw = grpcgw.New(srv)
		gw.SetClientSecret(clientSec)
		if clientSec != "" {
			// Gateway TLS follows the client secret (not --tls alone),
			// mirroring C++: an authenticated gateway never serves
			// plaintext. (ConfigureSecrets above already failed closed
			// when --tls itself is off, so certs are present here.)
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

	<-ctx.Done()
	stop()
	logger.Info().Msg("shutting down")
	if msrv != nil {
		_ = msrv.Close()
	}
	if gw != nil {
		gw.Stop()
	}
	srv.Shutdown()
}
