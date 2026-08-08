#include <asio.hpp>
#include <chrono>
#include <CLI/CLI.hpp>
#include <iostream>
#include <print>
#include <string>

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/status.hpp"

using asio::io_context;
using std::chrono::milliseconds;

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder cache client"};

    std::string host = "127.0.0.1";
    uint16_t port = 7'000;
    app.add_option("--host", host, "Server host");
    app.add_option("-p,--port", port, "Server port");

    std::string cmd;
    std::string key;
    std::string value;
    int ttl_ms = 0;

    app.add_option("command", cmd, "get|set|del|ping")->required();
    app.add_option("key", key, "Key");
    app.add_option("value", value, "Value (set only)");
    app.add_option("--ttl", ttl_ms, "TTL in ms (set only)");

    CLI11_PARSE(app, argc, argv);

    cinder::ClusterConfig config;
    config.nodes.push_back({"server", host, port});

    cinder::net::Request req;
    if (cmd == "get") {
        req.opcode = cinder::net::Opcode::Get;
        req.key = key;
    } else if (cmd == "set") {
        req.opcode = cinder::net::Opcode::Set;
        req.key = key;
        req.value = value;
        if (ttl_ms > 0) {
            req.ttl = milliseconds(ttl_ms);
        }
    } else if (cmd == "del") {
        req.opcode = cinder::net::Opcode::Del;
        req.key = key;
    } else if (cmd == "ping") {
        req.opcode = cinder::net::Opcode::Ping;
    } else {
        std::println(std::cerr, "unknown command: {}", cmd);
        return 1;
    }

    io_context io;
    cinder::ConnectionPool pool(config, io);
    auto res = pool.send("server", req);
    if (!res.has_value()) {
        std::println(std::cerr, "request failed: {}", res.error().message());
        return 1;
    }

    auto& response = res.value();
    if (cmd == "ping") {
        std::println("pong");
    } else if (response.value.has_value()) {
        std::println("{}", response.value.value());
    } else {
        std::println("{}", cinder::toString(response.status));
    }
    return 0;
}
