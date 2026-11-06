#pragma once

#include <chrono>
#include <CLI/CLI.hpp>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "cinder/common/config.hpp"
#include "cinder/node/cache_node_server.hpp"
#include "common_args.hpp"

namespace cinder::tools {

    // CLI-side carriers for cinderd knobs that cannot bind straight into
    // CacheNodeServerOptions: int shadows for duration fields (CLI11 parses
    // ints, resolveDaemonOptions converts them once), string enums, peers and
    // the config path. Every other flag binds directly into the options
    // struct; only this struct repeats the documented CLI defaults.
    struct DaemonCliArgs {
        int ping_interval_ms = 1000;
        int suspect_timeout_ms = 3000;
        int gossip_interval_ms = 1000;
        int quarantine_interval_ms = 10'000;
        int rpc_timeout_ms = 5000;
        int anti_entropy_interval_ms = 30'000;
        int anti_entropy_buckets = 256;
        int anti_entropy_full_rehash_interval_ms = 300'000;
        int snapshot_interval_s = 60;
        int tombstone_gc_ms = 300'000;
        std::string peers;
        std::string consistency = "async";
        std::string log_level = "info";
        std::string config_path;
#ifdef CINDER_ENABLE_GRPC
        uint16_t grpc_port = 0;
#endif
    };

    inline void seedDaemonCliArgs(const CacheNodeServerOptions& options, DaemonCliArgs& args) {
        args.ping_interval_ms = static_cast<int>(options.ping_interval.count());
        args.suspect_timeout_ms = static_cast<int>(options.suspect_timeout.count());
        args.gossip_interval_ms = static_cast<int>(options.gossip_interval.count());
        args.quarantine_interval_ms = static_cast<int>(options.quarantine_interval.count());
        args.rpc_timeout_ms = static_cast<int>(options.rpc_timeout.count());
        args.anti_entropy_interval_ms = static_cast<int>(options.anti_entropy.interval.count());
        args.anti_entropy_buckets = static_cast<int>(options.anti_entropy.buckets);
        args.anti_entropy_full_rehash_interval_ms =
            static_cast<int>(options.anti_entropy.full_rehash_interval.count());

        args.snapshot_interval_s = static_cast<int>(options.persistence.snapshot_interval_s);
        args.tombstone_gc_ms = static_cast<int>(options.tombstone_gc_interval.count());
    }

    inline void addDaemonOptions(
        CLI::App& app,
        CacheNodeServerOptions& options,
        DaemonCliArgs& args
    ) {
        seedDaemonCliArgs(options, args);
        bindFlag(app, "-C,--config", args.config_path, "Path to YAML config file");
        bindFlag(app, "-p,--port", options.port, "Port to listen on");
        bindFlag(
            app,
            "--metrics-port",
            options.metrics_port,
            "Prometheus metrics port (0 = disabled)"
        );
        bindFlag(
            app,
            "--listen-fd",
            options.listen_fd,
            "Pre-bound listen socket fd (test harness socket passing)"
        );
        bindFlag(
            app,
            "--metrics-listen-fd",
            options.metrics_listen_fd,
            "Pre-bound metrics socket fd (test harness socket passing)"
        );

        bindFlag(app, "-c,--capacity", options.capacity, "Per-node capacity in bytes");
        bindFlag(app, "-n,--node-id", options.node_id, "This node's ID");
        bindFlag(app, "--peers", args.peers, "Peer nodes (comma-separated id@host:port)");
        bindFlag(
            app,
            "-r,--replication-factor",
            options.replica_factor,
            "Replication factor (1 = none)"
        );

        bindFlag(app, "--consistency", args.consistency, "Write consistency: async|quorum");
        bindFlag(
            app,
            "--ping-interval",
            args.ping_interval_ms,
            "Failure-detector ping interval (ms)"
        );
        bindFlag(
            app,
            "--suspect-timeout",
            args.suspect_timeout_ms,
            "Suspect timeout before Dead (ms)"
        );
        bindFlag(
            app,
            "--gossip-interval",
            args.gossip_interval_ms,
            "Gossip dissemination interval (ms)"
        );
        bindFlag(
            app,
            "--quarantine-interval",
            args.quarantine_interval_ms,
            "Re-join quarantine before receiving migrated keys (ms, 0 = off)"
        );

        bindFlag(app, "--log-level", args.log_level, "Log level: trace|debug|info|warn|error");
        bindFlag(
            app,
            "--enable-persistence",
            options.persistence_enabled,
            "Enable WAL + snapshot persistence"
        );

        bindFlag(app, "--data-dir", options.data_dir, "Directory for WAL and snapshot files");
        bindFlag(
            app,
            "--snapshot-interval",
            args.snapshot_interval_s,
            "Snapshot interval (seconds)"
        );
        bindFlag(
            app,
            "--max-wal-entries",
            options.persistence.max_wal_entries,
            "Max WAL entries before forced compaction (0 = disabled)"
        );
        bindFlag(
            app,
            "--max-wal-bytes",
            options.persistence.max_wal_bytes,
            "Max WAL bytes before forced compaction (0 = disabled)"
        );
        bindFlag(
            app,
            "--max-wal-segment-bytes",
            options.persistence.max_wal_segment_bytes,
            "Seal the active WAL past this size (0 = disabled)"
        );
        bindFlag(
            app,
            "--max-sealed-segments",
            options.persistence.max_sealed_segments,
            "Force compaction past this many sealed segments (0 = disabled)"
        );
        bindFlag(
            app,
            "--wal-fsync",
            options.persistence.wal_fsync,
            "fdatasync on every WAL flush (durable, slower)"
        );
        bindFlag(
            app,
            "--io-threads",
            options.io_threads,
            "Worker threads running the event loop (0 = auto: min(4, hw))"
        );

        bindFlag(app, "--tls", options.tls_enabled, "Enable TLS encryption");
        bindFlag(app, "--tls-cert", options.tls_cert_file, "Path to TLS certificate chain (PEM)");
        bindFlag(app, "--tls-key", options.tls_key_file, "Path to TLS private key (PEM)");
        bindFlag(
            app,
            "--tls-ca",
            options.tls_ca_file,
            "Path to CA certificate for peer verification (PEM)"
        );
        bindFlag(app, "--eviction-policy", options.eviction_policy, "Eviction policy: lru|lfu")
            ->check(CLI::IsMember({"lru", "lfu"}));

        bindFlag(
            app,
            "--shared-secret",
            options.shared_secret,
            "Shared secret for node authentication"
        );
        bindFlag(
            app,
            "--client-shared-secret",
            options.client_shared_secret,
            "Shared secret for client authentication (requires TLS)"
        );
        bindFlag(
            app,
            "--rpc-timeout",
            args.rpc_timeout_ms,
            "Per-RPC deadline in milliseconds (0 = no timeout)"
        );
        bindFlag(
            app,
            "--anti-entropy-interval",
            args.anti_entropy_interval_ms,
            "Anti-entropy repair interval in milliseconds (0 = disabled)"
        );
        bindFlag(
            app,
            "--anti-entropy-buckets",
            args.anti_entropy_buckets,
            "Anti-entropy hash bucket count"
        );
        bindFlag(
            app,
            "--anti-entropy-fetch-bytes",
            options.anti_entropy.fetch_bytes,
            "Per-round anti-entropy repair fetch budget in bytes"
        );
        bindFlag(
            app,
            "--anti-entropy-full-rehash-interval",
            args.anti_entropy_full_rehash_interval_ms,
            "Anti-entropy full-rehash backstop interval in milliseconds"
        );
        bindFlag(
            app,
            "--max-hint-bytes",
            options.max_hint_bytes,
            "Hinted-handoff byte budget (0 = config/default)"
        );
        bindFlag(
            app,
            "--tombstone-gc-ms",
            args.tombstone_gc_ms,
            "Delete-marker GC grace in milliseconds (0 = config/default)"
        );
#ifdef CINDER_ENABLE_GRPC
        bindFlag(app, "--grpc-port", args.grpc_port, "gRPC gateway port (0 = disabled)");
#endif
    }

    // Combine CLI values and YAML config into effective server options.
    // Durations arrive as int shadows on DaemonCliArgs and convert here once;
    // YAML write-through happens via applyIfUnset. The flag argument must be a
    // single registered option name (CLI11 lookup rejects composite forms).
    inline void resolveDaemonOptions(
        CLI::App& app,
        CacheNodeServerOptions& options,
        const DaemonCliArgs& args,
        const Config& cfg
    ) {
        using std::chrono::milliseconds;

        options.mode =
            args.consistency == "quorum" ? ConsistencyMode::Quorum : ConsistencyMode::Async;

        options.ping_interval = milliseconds(args.ping_interval_ms);
        options.suspect_timeout = milliseconds(args.suspect_timeout_ms);
        options.gossip_interval = milliseconds(args.gossip_interval_ms);
        options.quarantine_interval = milliseconds(args.quarantine_interval_ms);
        options.rpc_timeout = milliseconds(args.rpc_timeout_ms);
        options.anti_entropy.interval = milliseconds(args.anti_entropy_interval_ms);
        options.anti_entropy.buckets = static_cast<uint32_t>(args.anti_entropy_buckets);
        options.anti_entropy.full_rehash_interval =
            milliseconds(args.anti_entropy_full_rehash_interval_ms);

        options.persistence.snapshot_interval_s = static_cast<size_t>(args.snapshot_interval_s);
        options.tombstone_gc_interval = milliseconds(args.tombstone_gc_ms);
        options.tls_enabled = options.tls_enabled || cfg.tls.enabled;
        options.persistence_enabled = options.persistence_enabled || cfg.persistence_enabled;
        options.config = cfg;
        options.config_path = args.config_path;
        if (options.data_dir.empty()) {
            options.data_dir = cfg.data_dir;
        }
        if (options.metrics_port == 0) {
            options.metrics_port = cfg.metrics_port;
        }
        if (!args.config_path.empty()) {
            // YAML fills every knob the command line did not set explicitly.
            applyIfUnset(app, "--port", options.port, cfg.port);
            applyIfUnset(app, "--capacity", options.capacity, cfg.capacity);
            applyIfUnset(app, "--node-id", options.node_id, cfg.node_id);
            applyIfUnset(app, "--replication-factor", options.replica_factor, cfg.replica_factor);
            applyIfUnset(
                app,
                "--rpc-timeout",
                options.rpc_timeout,
                milliseconds(cfg.rpc_timeout_ms)
            );
            applyIfUnset(
                app,
                "--snapshot-interval",
                options.persistence.snapshot_interval_s,
                cfg.persistence.snapshot_interval_s
            );
            applyIfUnset(
                app,
                "--max-wal-entries",
                options.persistence.max_wal_entries,
                cfg.persistence.max_wal_entries
            );
            applyIfUnset(
                app,
                "--max-wal-bytes",
                options.persistence.max_wal_bytes,
                cfg.persistence.max_wal_bytes
            );
            applyIfUnset(
                app,
                "--max-wal-segment-bytes",
                options.persistence.max_wal_segment_bytes,
                cfg.persistence.max_wal_segment_bytes
            );
            applyIfUnset(
                app,
                "--max-sealed-segments",
                options.persistence.max_sealed_segments,
                cfg.persistence.max_sealed_segments
            );
            applyIfUnset(
                app,
                "--wal-fsync",
                options.persistence.wal_fsync,
                cfg.persistence.wal_fsync
            );
            applyIfUnset(
                app,
                "--anti-entropy-interval",
                options.anti_entropy.interval,
                cfg.anti_entropy.interval
            );
            applyIfUnset(
                app,
                "--anti-entropy-buckets",
                options.anti_entropy.buckets,
                cfg.anti_entropy.buckets
            );
            applyIfUnset(
                app,
                "--anti-entropy-fetch-bytes",
                options.anti_entropy.fetch_bytes,
                cfg.anti_entropy.fetch_bytes
            );
            applyIfUnset(
                app,
                "--anti-entropy-full-rehash-interval",
                options.anti_entropy.full_rehash_interval,
                cfg.anti_entropy.full_rehash_interval
            );
            applyIfUnset(
                app,
                "--tombstone-gc-ms",
                options.tombstone_gc_interval,
                milliseconds(cfg.tombstone_gc_ms)
            );

            applyIfUnset(app, "--max-hint-bytes", options.max_hint_bytes, cfg.max_hint_bytes);
            applyIfUnset(app, "--tls-cert", options.tls_cert_file, cfg.tls.cert_file);
            applyIfUnset(app, "--tls-key", options.tls_key_file, cfg.tls.key_file);
            applyIfUnset(app, "--tls-ca", options.tls_ca_file, cfg.tls.ca_file);
            applyIfUnset(app, "--shared-secret", options.shared_secret, cfg.auth.shared_secret);
            applyIfUnset(
                app,
                "--client-shared-secret",
                options.client_shared_secret,
                cfg.auth.client_shared_secret
            );
            applyIfUnset(app, "--log-level", options.config.log_level, cfg.log_level);
        } else if (options.max_hint_bytes == 0) {
            // No config file: Config carries the hinted-handoff budget
            // default
            options.max_hint_bytes = cfg.max_hint_bytes;
        }

        if (!args.peers.empty()) {
            Config peer_cfg;
            parsePeersString(args.peers, peer_cfg);
            options.peers = std::move(peer_cfg.peers);
        } else {
            options.peers = cfg.peers;
        }
    }
} // namespace cinder::tools
