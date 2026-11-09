// Command cinderd is the Cinder distributed cache server (Go port).
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kraytos17/cinder/internal/ae"
	"github.com/kraytos17/cinder/internal/client"
	"github.com/kraytos17/cinder/internal/grpcgw"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
	"github.com/kraytos17/cinder/internal/tlscfg"
)

var version = "dev"

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
		tls           = flag.Bool("tls", false, "enable TLS encryption")
		tlsCert       = flag.String("tls-cert", "", "path to TLS certificate chain (PEM)")
		tlsKey        = flag.String("tls-key", "", "path to TLS private key (PEM)")
		tlsCA         = flag.String("tls-ca", "", "path to CA certificate for peer verification (PEM)")
		sharedSecret  = flag.String("shared-secret", "", "shared secret for node authentication (or CINDER_SHARED_SECRET)")
		clientSecret  = flag.String("client-shared-secret", "", "shared secret for client authentication, requires --tls (or CINDER_CLIENT_SECRET)")
		enablePersist = flag.Bool("enable-persistence", false, "enable WAL + snapshots")
		dataDir       = flag.String("data-dir", "", "persistence directory (required with --enable-persistence)")
		showVers      = flag.Bool("version", false, "print version and exit")
	)

	flag.Parse()
	if *showVers {
		fmt.Printf("cinderd (go) %s\n", version)
		return
	}

	peerList, err := tcp.ParsePeers(*peers)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	st := store.NewLRUStore(*capacity, nil)
	srv := tcp.NewServer(*nodeID, *port, st, nil)
	if err := srv.ConfigureCluster(peerList); err != nil {
		fmt.Fprintln(os.Stderr, "cinderd:", err)
		os.Exit(2)
	}

	srv.SetGossipInterval(time.Duration(*gossipMs) * time.Millisecond)
	srv.SetProbeInterval(time.Duration(*probeMs) * time.Millisecond)
	srv.SetSuspectTimeout(time.Duration(*suspectMs) * time.Millisecond)
	srv.SetAntiEntropyInterval(time.Duration(*aeInterval) * time.Millisecond)
	srv.ConfigureAntiEntropy(uint32(*aeBuckets), 0)
	srv.ConfigureRebalance(*replFact, time.Duration(*quarantineMs)*time.Millisecond)
	if *tls {
		if *tlsCert == "" || *tlsKey == "" {
			fmt.Fprintln(os.Stderr, "cinderd: --tls requires --tls-cert and --tls-key")
			os.Exit(2)
		}
		if err := srv.ConfigureTLS(*tlsCert, *tlsKey, *tlsCA); err != nil {
			fmt.Fprintln(os.Stderr, "cinderd: tls:", err)
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
			fmt.Fprintln(os.Stderr, "cinderd:", err)
			os.Exit(2)
		}
	}
	if *enablePersist {
		if *dataDir == "" {
			fmt.Fprintln(os.Stderr, "cinderd: --data-dir is required with --enable-persistence")
			os.Exit(2)
		}
		if err := srv.ConfigurePersistence(*dataDir); err != nil {
			fmt.Fprintln(os.Stderr, "cinderd:", err)
			os.Exit(2)
		}
	}
	if *replFact > 1 {
		var mode repl.Mode
		switch *consist {
		case "async":
			mode = repl.Async
		case "quorum":
			mode = repl.Quorum
		default:
			fmt.Fprintln(os.Stderr, "cinderd: --consistency must be async|quorum")
			os.Exit(2)
		}

		addrs := make([]client.NodeAddr, 0, len(peerList))
		for _, p := range peerList {
			addrs = append(addrs, client.NodeAddr{ID: p.ID, Host: p.Host, Port: p.Port})
		}

		pool := client.NewPool(addrs, 5*time.Second)
		if *tls {
			ccfg, err := tlscfg.Client(*tlsCert, *tlsKey, *tlsCA)
			if err != nil {
				fmt.Fprintln(os.Stderr, "cinderd: tls:", err)
				os.Exit(2)
			}
			pool.SetTLS(ccfg)
		}
		if shared != "" {
			pool.SetSecrets(client.Secrets{NodeID: *nodeID, Shared: shared})
		}
		mgr := repl.NewManager(st, *nodeID, nil, pool)
		srv.ConfigureReplication(*replFact, mode, mgr)
		if *enablePersist {
			j, _, err := ae.OpenJournal(filepath.Join(*dataDir, "hints.log"), mgr.HintQueue())
			if err != nil {
				fmt.Fprintln(os.Stderr, "cinderd: hint journal:", err)
				os.Exit(2)
			}
			mgr.SetHintJournal(j)
		}
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cinderd: listen:", err)
		os.Exit(1)
	}

	go srv.Serve(l)
	if *grpcPort > 0 {
		gl, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *grpcPort))
		if err != nil {
			fmt.Fprintln(os.Stderr, "cinderd: grpc listen:", err)
			os.Exit(1)
		}
		gw := grpcgw.New(srv)
		gw.SetClientSecret(clientSec)
		defer gw.Stop()
		go func() {
			if err := gw.Serve(gl); err != nil {
				fmt.Fprintln(os.Stderr, "cinderd: grpc:", err)
			}
		}()
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Shutdown()
}
