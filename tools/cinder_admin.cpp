#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <print>
#include <string>
#include <thread>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/status.hpp"
#include "cinder/net/protocol.hpp"

using asio::io_context;
using std::chrono::milliseconds;

auto
main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder admin CLI"};

    std::string host = "127.0.0.1";
    uint16_t port = 7'000;
    bool verbose = false;
    bool tls_enabled = false;
    std::string tls_cert_file;
    std::string tls_key_file;
    std::string tls_ca_file;

    app.add_option("--host", host, "Server host");
    app.add_option("-p,--port", port, "Server port");
    app.add_flag("-v,--verbose", verbose, "Enable verbose logging");
    app.add_flag("--tls", tls_enabled, "Enable TLS");
    app.add_option("--tls-cert", tls_cert_file, "TLS cert chain (PEM)");
    app.add_option("--tls-key", tls_key_file, "TLS private key (PEM)");
    app.add_option("--tls-ca", tls_ca_file, "CA cert for verification (PEM)");

    std::string cmd;
    app.add_option("command", cmd, "info|cluster|ring|compact|config-reload|shutdown")->required();

    CLI11_PARSE(app, argc, argv);

    cinder::Logger::init("cinder-admin",
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
    if (cmd == "info") {
        req.opcode = cinder::net::Opcode::AdminInfo;
    } else if (cmd == "cluster") {
        req.opcode = cinder::net::Opcode::AdminCluster;
    } else if (cmd == "ring") {
        req.opcode = cinder::net::Opcode::AdminRing;
    } else if (cmd == "compact") {
        req.opcode = cinder::net::Opcode::AdminCompact;
    } else if (cmd == "config-reload") {
        req.opcode = cinder::net::Opcode::AdminConfigReload;
    } else if (cmd == "shutdown") {
        req.opcode = cinder::net::Opcode::AdminShutdown;
    } else {
        cinder::Logger::error("unknown command: {}", cmd);
        std::println(stderr, "unknown command: {}", cmd);
        std::println(
            stderr, "available commands: info, cluster, ring, compact, config-reload, shutdown");
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

    std::jthread io_thread([&io](std::stop_token) {
        auto work = asio::make_work_guard(io);
        io.run();
    });

    cinder::Logger::debug("sending {} to {}:{}", cmd, host, port);
    auto res = pool.send("server", req);

    io.stop();
    if (!res.has_value()) {
        cinder::Logger::error("request failed: {}", res.error().message());
        std::println(stderr, "error: {}", res.error().message());
        return 1;
    }

    if (res->value.has_value()) {
        std::println("{}", res->value.value());
    } else if (res->status == cinder::Errc::OK) {
        std::println("OK");
    } else {
        std::println("{}", cinder::toString(res->status));
    }
    return 0;
}
