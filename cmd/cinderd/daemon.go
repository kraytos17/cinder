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
		if err := srv.SetFsyncPolicy(cfg.FsyncPolicy); err != nil {
			return fmt.Errorf("persistence fsync: %w", err)
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
	go func() { //nolint:contextcheck // Serve is a blocking listener call with no context parameter by design; its lifecycle is controlled via ShutdownContext below, and the transitively-created shutdown-deadline context has no meaningful parent here.
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
	shutdownTimeout := time.Duration(cfg.ShutdownTimeoutMs) * time.Millisecond
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	if msrv != nil {
		if err := msrv.Shutdown(shutdownCtx); err != nil {
			logger.Error().Err(err).Msg("metrics shutdown")
		}
	}
	if gw != nil {
		if err := gw.Shutdown(shutdownCtx); err != nil {
			logger.Error().Err(err).Msg("grpc shutdown")
		}
	}
	if err := srv.ShutdownContext(shutdownCtx); err != nil {
		logger.Error().Err(err).Msg("server shutdown did not drain before deadline")
	}
	return nil
}
