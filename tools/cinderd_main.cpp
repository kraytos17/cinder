#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <string>

#include "cinder/common/logger.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/store/lru_store.hpp"

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder distributed cache server"};

    uint16_t port = 7'000;
    size_t capacity = 67'108'864;
    std::string node_id = "node1";
    std::string peers;

    app.add_option("-p,--port", port, "Port to listen on");
    app.add_option("-c,--capacity", capacity, "Per-node capacity in bytes");
    app.add_option("-n,--node-id", node_id, "This node's ID");
    app.add_option("--peers", peers, "Peer node IDs (comma-separated)");

    CLI11_PARSE(app, argc, argv);

    cinder::Logger::init("cinderd", cinder::LogLevel::Info);
    cinder::Logger::info("starting cinderd on port " + std::to_string(port));

    cinder::ConsistentHashRing ring(150);
    ring.add_node(node_id);
    if (!peers.empty()) {
        size_t start = 0;
        while (true) {
            auto end = peers.find(',', start);
            auto peer = peers.substr(start, end - start);
            if (peer.contains('@')) {
                ring.add_node(peer);
            }
            if (end == std::string::npos)
                break;
            start = end + 1;
        }
    }

    asio::io_context io;
    cinder::LruStore store(capacity);
    cinder::net::TcpServer server(io, port, store, ring, node_id);

    auto result = server.start();
    if (!result.has_value()) {
        cinder::Logger::error("failed to start server");
        return 1;
    }

    cinder::Logger::info("listening on port " + std::to_string(port));
    asio::signal_set signals(io, SIGINT, SIGTERM);
    signals.async_wait([&](std::error_code, int) {
        cinder::Logger::info("shutting down...");
        server.shutdown();
        io.stop();
    });

    io.run();
    cinder::Logger::info("stopped");
    return 0;
}
