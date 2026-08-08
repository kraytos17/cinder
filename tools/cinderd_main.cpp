#include <CLI/CLI.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "cinder/common/logger.hpp"
#include "cinder/node/cache_node_server.hpp"

using std::chrono::milliseconds;

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder distributed cache server"};

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

    CLI11_PARSE(app, argc, argv);

    cinder::Logger::init("cinderd", cinder::LogLevel::Info);
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

    if (!peers.empty()) {
        size_t start = 0;
        while (true) {
            auto end = peers.find(',', start);
            auto peer = peers.substr(start, end - start);
            cinder::ClusterConfig::NodeConfig peer_config;
            if (parsePeer(peer, peer_config)) {
                options.peers.push_back(peer_config);
            } else {
                cinder::Logger::warn("skipping malformed peer: {}", peer);
            }
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
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
