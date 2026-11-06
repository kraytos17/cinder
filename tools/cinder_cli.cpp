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
#include "common_args.hpp"

using asio::io_context;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;

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

// Batch set: one Set per key/value pair over the shared pool. Prints
// `key: OK` per pair (or `key: <status>` on failure) and exits nonzero if
// any pair failed, so `mset` is usable in scripts.
static auto runMultiSet(
    cinder::ConnectionPool& pool,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values,
    int ttl_ms
) -> int {
    bool failed = false;
    for (size_t i = 0; i < keys.size(); ++i) {
        cinder::net::Request req;
        req.opcode = cinder::net::Opcode::Set;
        req.key = keys[i];
        req.value = values[i];
        if (ttl_ms > 0) {
            req.ttl = milliseconds(ttl_ms);
        }

        auto res = pool.send("server", req);
        if (!res.has_value()) {
            std::println("{}: {}", keys[i], res.error().message());
            failed = true;
        } else if (res.value().status == cinder::Errc::OK) {
            std::println("{}: OK", keys[i]);
        } else {
            std::println("{}: {}", keys[i], cinder::toString(res.value().status));
            failed = true;
        }
    }
    return failed ? 1 : 0;
}

auto main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder cache client"};

    cinder::tools::ClientEndpoint endpoint;
    cinder::tools::addClientEndpointOptions(app, endpoint);

    std::string cmd;
    std::string key;
    std::string value;
    std::vector<std::string> extra_keys;
    int ttl_ms = 0;

    cinder::tools::bindFlag(
        app,
        "command",
        cmd,
        "get|set|del|ping|info|cluster|ring|mget|mset|get-versioned"
    )
        ->required();

    cinder::tools::bindFlag(app, "key", key, "Key (first key for mget, k1 for mset)");
    cinder::tools::bindFlag(app, "value", value, "Value (set only, v1 for mset)");
    cinder::tools::bindFlag(
        app,
        "extra_keys",
        extra_keys,
        "Additional keys (mget) or k2/v2/... (mset)"
    )
        ->take_all();

    cinder::tools::bindFlag(app, "--ttl", ttl_ms, "TTL in ms (set/mset only)");

    CLI11_PARSE(app, argc, argv);
    cinder::initLogger(
        "cinder-cli",
        endpoint.verbose ? cinder::LogLevel::Debug : cinder::LogLevel::Warn,
        cinder::LogSink::Stderr
    );

    // One root span per invocation: command/host/port as attributes (never
    // keys, values, or the client secret). pool.send children join it via
    // thread-local context, so the server joins the CLI trace. The finish
    // helper reports the exit outcome at every return below.
    cinder::tools::CliSpan cli_span("cinder-cli", cmd, endpoint);
    cinder::ClusterConfig config;

    config.nodes.push_back({"server", endpoint.host, endpoint.port});
    config.client_shared_secret = cinder::tools::resolveClientSecret(endpoint.client_secret);

#ifdef CINDER_ENABLE_TLS
    auto ssl_ctx = cinder::tools::makeClientSslContext(endpoint);
#endif

    io_context io;
    cinder::ConnectionPool pool(
        config,
        io,
        {},
        {},
        config.client_shared_secret
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
    } else if (cmd == "get-versioned") {
        req.opcode = cinder::net::Opcode::GetVersioned;
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
            return cli_span.finish(cinder::SpanStatus::Error, 1);
        }

        int rc = runMultiGet(pool, keys);
        io.stop();
        return cli_span.finish(rc == 0 ? cinder::SpanStatus::Ok : cinder::SpanStatus::Error, rc);
    } else if (cmd == "mset") {
        // Pairs arrive through every positional slot: `key` (k1), then
        // `value` (v1), then the take_all `extra_keys` (k2, v2, ...).
        // Single-key commands are unaffected.
        std::vector<std::string> args;
        if (!key.empty()) {
            args.push_back(key);
        }
        if (!value.empty()) {
            args.push_back(value);
        }

        args.insert(args.end(), extra_keys.begin(), extra_keys.end());
        if (args.empty() || args.size() % 2 != 0) {
            CINDER_ERROR("mset requires key/value pairs");
            io.stop();
            return cli_span.finish(cinder::SpanStatus::Error, 1);
        }

        std::vector<std::string> mset_keys;
        std::vector<std::string> mset_values;
        for (size_t i = 0; i < args.size(); i += 2) {
            mset_keys.push_back(args[i]);
            mset_values.push_back(args[i + 1]);
        }

        int mset_rc = runMultiSet(pool, mset_keys, mset_values, ttl_ms);
        io.stop();
        return cli_span.finish(
            mset_rc == 0 ? cinder::SpanStatus::Ok : cinder::SpanStatus::Error,
            mset_rc
        );
    } else {
        CINDER_ERROR("unknown command", {"cmd", cmd});
        io.stop();
        return cli_span.finish(cinder::SpanStatus::Error, 1);
    }

    CINDER_DEBUG("sending", {"cmd", cmd}, {"host", endpoint.host}, {"port", endpoint.port});
    auto res = pool.send("server", req);
    io.stop();
    if (!res.has_value()) {
        CINDER_ERROR("request failed", {"reason", res.error().message()});
        return cli_span.finish(cinder::SpanStatus::Error, 1);
    }

    auto& response = res.value();
    if (response.status == cinder::Errc::PermissionDenied) {
        // Auth failure is a hard error (nonzero exit) so scripts and tests
        // can distinguish it from data. Other statuses keep the legacy
        // print-to-stdout behavior.
        CINDER_ERROR("permission denied", {"cmd", cmd});
        std::println(stderr, "error: permission denied");
        return cli_span.finish(cinder::SpanStatus::Error, 1);
    }
    if (cmd == "ping") {
        std::println("pong");
    } else if (response.tombstone) {
        // Deleted but the marker survives (tombstone): report the delete
        // with its LWW identity rather than a bare status.
        std::println("tombstone");
        std::println("version: {}", response.version);
        std::println("writer: {}", response.writer_node_hash);
    } else if (response.value.has_value()) {
        std::println("{}", response.value.value());
        if (cmd == "get-versioned") {
            // LWW metadata rides on GetVersioned responses only. Writer is
            // the node hash in decimal; expiry (when set) as epoch seconds.
            std::println("version: {}", response.version);
            std::println("writer: {}", response.writer_node_hash);
            if (response.expires_at.has_value()) {
                const auto secs =
                    duration_cast<seconds>(response.expires_at->time_since_epoch()).count();
                std::println("expires_at: {}", secs);
            }
        }
    } else {
        std::println("{}", cinder::toString(response.status));
    }
    return cli_span.finish(cinder::SpanStatus::Ok, 0);
}
