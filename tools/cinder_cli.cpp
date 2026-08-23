#include <asio.hpp>
#include <chrono>
#include <CLI/CLI.hpp>
#include <print>
#include <string>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/status.hpp"

using asio::io_context;
using std::chrono::milliseconds;

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder cache client"};

    std::string host = "127.0.0.1";
    uint16_t port = 7'000;
    bool verbose = false;
    bool tls_enabled = false;
    std::string tls_cert_file;
    std::string tls_key_file;
    std::string tls_ca_file;

    app.add_option("--host", host, "Server host");
    app.add_option("-p,--port", port, "Server port");
    app.add_flag("-v,--verbose", verbose, "Enable verbose (debug) logging");
    app.add_flag("--tls", tls_enabled, "Enable TLS encryption");
    app.add_option("--tls-cert", tls_cert_file, "Path to TLS certificate chain (PEM)");
    app.add_option("--tls-key", tls_key_file, "Path to TLS private key (PEM)");
    app.add_option("--tls-ca", tls_ca_file, "Path to CA certificate for peer verification (PEM)");

    std::string cmd;
    std::string key;
    std::string value;
    int ttl_ms = 0;

    app.add_option("command", cmd, "get|set|del|ping")->required();
    app.add_option("key", key, "Key");
    app.add_option("value", value, "Value (set only)");
    app.add_option("--ttl", ttl_ms, "TTL in ms (set only)");

    CLI11_PARSE(app, argc, argv);

    cinder::Logger::init("cinder-cli",
        verbose ? cinder::LogLevel::Debug : cinder::LogLevel::Warn,
        cinder::LogSink::Stderr);

    cinder::ClusterConfig config;
    config.nodes.push_back({"server", host, port});

#ifdef CINDER_ENABLE_TLS
    std::optional<asio::ssl::context> ssl_ctx;
    if (tls_enabled) {
        ssl_ctx.emplace(asio::ssl::context(asio::ssl::context::tlsv12_client));
        if (!tls_cert_file.empty()) {
            ssl_ctx->use_certificate_chain_file(tls_cert_file);
        }
        if (!tls_key_file.empty()) {
            ssl_ctx->use_private_key_file(tls_key_file, asio::ssl::context::pem);
        }
        if (!tls_ca_file.empty()) {
            ssl_ctx->load_verify_file(tls_ca_file);
            ssl_ctx->set_verify_mode(asio::ssl::verify_peer);
        } else {
            ssl_ctx->set_verify_mode(asio::ssl::verify_none);
        }
    }
#endif

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
        cinder::Logger::error("unknown command: {}", cmd);
        return 1;
    }

    io_context io;
    cinder::ConnectionPool pool(config,
        io
#ifdef CINDER_ENABLE_TLS
        ,
        ssl_ctx ? &*ssl_ctx : nullptr
#endif
    );

    cinder::Logger::debug("sending {} to {}:{}", cmd, host, port);
    auto res = pool.send("server", req);
    if (!res.has_value()) {
        cinder::Logger::error("request failed: {}", res.error().message());
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
