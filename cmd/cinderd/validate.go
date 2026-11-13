package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/urfave/cli/v3"

	"github.com/kraytos17/cinder/internal/hashring"
	"github.com/kraytos17/cinder/internal/log"
	"github.com/kraytos17/cinder/internal/net/tcp"
	"github.com/kraytos17/cinder/internal/persist"
	"github.com/kraytos17/cinder/internal/repl"
	"github.com/kraytos17/cinder/internal/store"
)

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
	if cfg.ShutdownTimeoutMs < 0 {
		errs = append(errs, errors.New("--shutdown-timeout-ms must be >= 0"))
	}
	if _, _, err := persist.ParseFsyncPolicy(cfg.FsyncPolicy); err != nil {
		errs = append(errs, err)
	}
	if cfg.FsyncPolicy != "" && cfg.FsyncPolicy != "none" && !cfg.EnablePersistence {
		errs = append(errs, errors.New("--fsync requires --enable-persistence"))
	}
	return out, errs
}
