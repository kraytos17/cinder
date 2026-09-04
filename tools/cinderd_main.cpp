#include <CLI/CLI.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "cinder/common/config.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/node/cache_node_server.hpp"

using std::chrono::milliseconds;

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder distributed cache server"};

    std::string config_path;
    app.add_option("-C,--config", config_path, "Path to YAML config file");

    uint16_t port = 7'000;
    size_t capacity = 67'108'864;
    std::string node_id = "node1";
    std::string peers;
    int replica_factor = 1;

    std::string consistency = "async";
    int ping_interval_ms = 1'000;
    int suspect_timeout_ms = 3'000;
    int gossip_interval_ms = 1'000;
    int quarantine_interval_ms = 10'000;
    std::string log_level = "info";
    bool persistence_enabled = false;
    std::string data_dir;
    int snapshot_interval_s = 60;
    int io_threads = 0;
    bool tls_enabled = false;
    std::string tls_cert_file;
    std::string tls_key_file;
    std::string tls_ca_file;
    std::string eviction_policy = "lru";
    int rpc_timeout_ms = 5'000;
    int anti_entropy_interval_ms = 30'000;
    int anti_entropy_buckets = 256;

    app.add_option("-p,--port", port, "Port to listen on");
    app.add_option("-c,--capacity", capacity, "Per-node capacity in bytes");
    app.add_option("-n,--node-id", node_id, "This node's ID");
    app.add_option("--peers", peers, "Peer nodes (comma-separated id@host:port)");
    app.add_option("-r,--replication-factor", replica_factor, "Replication factor (1 = none)");
    app.add_option("--consistency", consistency, "Write consistency: async|quorum");
    app.add_option("--ping-interval", ping_interval_ms, "Failure-detector ping interval (ms)");
    app.add_option("--suspect-timeout", suspect_timeout_ms, "Suspect timeout before Dead (ms)");
    app.add_option("--gossip-interval", gossip_interval_ms, "Gossip dissemination interval (ms)");
    app.add_option("--quarantine-interval",
        quarantine_interval_ms,
        "Re-join quarantine before receiving migrated keys (ms, 0 = off)");

    app.add_option("--log-level", log_level, "Log level: trace|debug|info|warn|error");
    app.add_flag("--enable-persistence", persistence_enabled, "Enable WAL + snapshot persistence");
    app.add_option("--data-dir", data_dir, "Directory for WAL and snapshot files");
    app.add_option("--snapshot-interval", snapshot_interval_s, "Snapshot interval (seconds)");
    app.add_option(
        "--io-threads", io_threads, "Worker threads running the event loop (0 = auto: min(4, hw))");

    app.add_flag("--tls", tls_enabled, "Enable TLS encryption");
    app.add_option("--tls-cert", tls_cert_file, "Path to TLS certificate chain (PEM)");
    app.add_option("--tls-key", tls_key_file, "Path to TLS private key (PEM)");
    app.add_option("--tls-ca", tls_ca_file, "Path to CA certificate for peer verification (PEM)");
    app.add_option("--eviction-policy", eviction_policy, "Eviction policy: lru|lfu")
        ->check(CLI::IsMember({"lru", "lfu"}));

    app.add_option(
        "--rpc-timeout", rpc_timeout_ms, "Per-RPC deadline in milliseconds (0 = no timeout)");
    app.add_option("--anti-entropy-interval",
        anti_entropy_interval_ms,
        "Anti-entropy repair interval in milliseconds (0 = disabled)");
    app.add_option(
        "--anti-entropy-buckets", anti_entropy_buckets, "Anti-entropy hash bucket count");

    CLI11_PARSE(app, argc, argv);

    cinder::Config cfg;
    if (!config_path.empty()) {
        auto result = cinder::loadConfig(config_path);
        if (!result.has_value()) {
            cinder::Logger::init("cinderd", cinder::LogLevel::Error);
            cinder::Logger::error("{}", result.error().message());
            return 1;
        }
        cfg = std::move(result.value());
    }

    // Only override if the CLI flag was explicitly set by the user.
    // Since CLI11 always writes to the variable, we detect "was it set"
    // by checking if the config file already populated it. For simplicity,
    // CLI flags always win when provided on the command line.
    if (!config_path.empty()) {
        // Config file loaded — CLI flags override via fallthrough below.
    }

    cinder::Logger::init("cinderd", cinder::logLevelFromString(log_level));
    cinder::Logger::info("starting cinderd on port {}", port);

    cinder::CacheNodeServerOptions options;
    options.node_id = node_id;
    options.port = port;
    options.capacity = capacity;
    options.replica_factor = replica_factor;
    options.mode =
        consistency == "quorum" ? cinder::ConsistencyMode::Quorum : cinder::ConsistencyMode::Async;

    options.ping_interval = milliseconds(ping_interval_ms);
    options.suspect_timeout = milliseconds(suspect_timeout_ms);
    options.gossip_interval = milliseconds(gossip_interval_ms);
    options.quarantine_interval = milliseconds(quarantine_interval_ms);
    options.persistence_enabled = persistence_enabled || cfg.persistence_enabled;
    options.data_dir = data_dir.empty() ? cfg.data_dir : data_dir;
    options.snapshot_interval_s = snapshot_interval_s;
    options.io_threads = io_threads;
    options.tls_enabled = tls_enabled || cfg.tls.enabled;
    options.tls_cert_file = tls_cert_file.empty() ? cfg.tls.cert_file : tls_cert_file;
    options.tls_key_file = tls_key_file.empty() ? cfg.tls.key_file : tls_key_file;
    options.tls_ca_file = tls_ca_file.empty() ? cfg.tls.ca_file : tls_ca_file;
    options.eviction_policy = eviction_policy;
    options.rpc_timeout = milliseconds(rpc_timeout_ms);
    options.anti_entropy_interval = milliseconds(anti_entropy_interval_ms);
    options.anti_entropy_buckets = static_cast<uint32_t>(anti_entropy_buckets);
    if (!config_path.empty()) {
        // Config-file values are the base; explicit CLI flags override them.
        if (app.count("--anti-entropy-interval") == 0) {
            options.anti_entropy_interval = milliseconds(cfg.anti_entropy_interval_ms);
        }
        if (app.count("--anti-entropy-buckets") == 0) {
            options.anti_entropy_buckets = static_cast<uint32_t>(cfg.anti_entropy_buckets);
        }
    }

    options.config = cfg;
    options.config_path = config_path;
    options.metrics_port = cfg.metrics_port;
    if (!peers.empty()) {
        cinder::Config peer_cfg;
        cinder::parsePeersString(peers, peer_cfg);
        options.peers = std::move(peer_cfg.peers);
    } else if (!cfg.peers.empty()) {
        options.peers = std::move(cfg.peers);
    }

    cinder::CacheNodeServer server(std::move(options));
    auto result = server.start();
    if (!result.has_value()) {
        cinder::Logger::error("failed to start server");
        return 1;
    }

    cinder::Logger::info(
        "listening on port {} (replication factor {}, {})", port, replica_factor, consistency);
    server.run();
    cinder::Logger::info("stopped");
    return 0;
}
