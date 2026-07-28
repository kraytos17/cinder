#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <cstdlib>
#include <string>

#include "cinder/common/logger.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/store/lru_store.hpp"

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder distributed cache server"};

    uint16_t port = 7'000;
    size_t capacity = 67'108'864;
    std::string eviction = "lru";

    app.add_option("-p,--port", port, "Port to listen on");
    app.add_option("-c,--capacity", capacity, "Per-node capacity in bytes");
    app.add_option("-e,--eviction", eviction, "Eviction policy: lru|lfu|ttl");

    CLI11_PARSE(app, argc, argv);
    cinder::Logger::init("cinderd", cinder::LogLevel::Info);
    cinder::Logger::info("starting cinderd on port " + std::to_string(port));

    asio::io_context io;
    cinder::LruStore store(capacity);
    cinder::net::TcpServer server(io, port, store);

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
