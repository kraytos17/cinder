#include <asio.hpp>
#include <chrono>
#include <CLI/CLI.hpp>
#include <print>
#include <string>
#include <thread>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"

using asio::io_context;
using std::chrono::milliseconds;

// Batch get: one Get per key over the shared pool. Prints `key: value` per
// line (or `key: <status>` on a miss/error) and exits nonzero if any key
// failed, so `mget` is usable in scripts.
static auto runMultiGet(cinder::ConnectionPool& pool, const std::vector<std::string>& keys) -> int {
    bool failed = false;
    for (const auto& k : keys) {
        cinder::net::Request req;
        req.opcode = cinder::net::Opcode::Get;
        req.key = k;
        auto res = pool.send("server", req);
        if (!res.has_value()) {
            std::println("{}: {}", k, res.error().message());
            failed = true;
        } else if (res.value().value.has_value()) {
            std::println("{}: {}", k, res.value().value.value());
        } else {
            std::println("{}: {}", k, cinder::toString(res.value().status));
            failed = true;
        }
    }
    return failed ? 1 : 0;
}

auto main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder cache client"};

    std::string host = "127.0.0.1";
    uint16_t port = 7000;
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
    std::vector<std::string> extra_keys;
    int ttl_ms = 0;

    app.add_option("command", cmd, "get|set|del|ping|info|cluster|ring|mget")->required();
    app.add_option("key", key, "Key (first key for mget)");
    app.add_option("value", value, "Value (set only)");
    app.add_option("extra_keys", extra_keys, "Additional keys (mget only)")->take_all();
    app.add_option("--ttl", ttl_ms, "TTL in ms (set only)");

    CLI11_PARSE(app, argc, argv);
    cinder::initLogger(
        "cinder-cli",
        verbose ? cinder::LogLevel::Debug : cinder::LogLevel::Warn,
        cinder::LogSink::Stderr
    );

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

    io_context io;
    cinder::ConnectionPool pool(
        config,
        io,
        {},
        {}
#ifdef CINDER_ENABLE_TLS
        ,
        ssl_ctx ? &*ssl_ctx : nullptr
#endif
    );

    std::jthread io_thread([&io](std::stop_token) {
        auto work = asio::make_work_guard(io);
        io.run();
    });

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
    } else if (cmd == "info") {
        req.opcode = cinder::net::Opcode::AdminInfo;
    } else if (cmd == "cluster") {
        req.opcode = cinder::net::Opcode::AdminCluster;
    } else if (cmd == "ring") {
        req.opcode = cinder::net::Opcode::AdminRing;
    } else if (cmd == "mget") {
        // Keys arrive through every positional slot: `key`, then `value`
        // (documented "set only", reused here as the second key), then the
        // take_all `extra_keys`. Single-key commands are unaffected.
        std::vector<std::string> keys;
        if (!key.empty()) {
            keys.push_back(key);
        }
        if (!value.empty()) {
            keys.push_back(value);
        }

        keys.insert(keys.end(), extra_keys.begin(), extra_keys.end());
        if (keys.empty()) {
            CINDER_ERROR("mget requires at least one key");
            io.stop();
            return 1;
        }

        int rc = runMultiGet(pool, keys);
        io.stop();
        return rc;
    } else {
        CINDER_ERROR("unknown command", {"cmd", cmd});
        return 1;
    }

    CINDER_DEBUG("sending", {"cmd", cmd}, {"host", host}, {"port", port});
    auto res = pool.send("server", req);
    io.stop();
    if (!res.has_value()) {
        CINDER_ERROR("request failed", {"reason", res.error().message()});
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
