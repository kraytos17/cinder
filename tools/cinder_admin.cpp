#include <asio.hpp>
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
#include "cinder/net/protocol.hpp"
#include "common_args.hpp"

using asio::io_context;
using std::chrono::milliseconds;

auto main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder admin CLI"};

    cinder::tools::ClientEndpoint endpoint;
    cinder::tools::addClientEndpointOptions(app, endpoint);
    std::string cmd;
    cinder::tools::bindFlag(app, "command", cmd, "info|cluster|ring|compact|config-reload|shutdown")
        ->required();

    CLI11_PARSE(app, argc, argv);
    cinder::initLogger(
        "cinder-admin",
        endpoint.verbose ? cinder::LogLevel::Debug : cinder::LogLevel::Warn,
        cinder::LogSink::Stderr
    );

    cinder::tools::CliSpan admin_span("cinder-admin", cmd, endpoint);
    cinder::ClusterConfig config;
    config.nodes.push_back({"server", endpoint.host, endpoint.port});
    config.client_shared_secret = cinder::tools::resolveClientSecret(endpoint.client_secret);

#ifdef CINDER_ENABLE_TLS
    auto ssl_ctx = cinder::tools::makeClientSslContext(endpoint);
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
        CINDER_ERROR("unknown command", {"cmd", cmd});
        std::println(stderr, "unknown command: {}", cmd);
        std::println(
            stderr,
            "available commands: info, cluster, ring, compact, config-reload, shutdown"
        );
        return admin_span.finish(cinder::SpanStatus::Error, 1);
    }

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

    CINDER_DEBUG("sending", {"cmd", cmd}, {"host", endpoint.host}, {"port", endpoint.port});
    auto res = pool.send("server", req);
    io.stop();
    if (!res.has_value()) {
        CINDER_ERROR("request failed", {"reason", res.error().message()});
        std::println(stderr, "error: {}", res.error().message());
        return admin_span.finish(cinder::SpanStatus::Error, 1);
    }
    if (res->status == cinder::Errc::PermissionDenied) {
        CINDER_ERROR("permission denied", {"cmd", cmd});
        std::println(stderr, "error: permission denied");
        return admin_span.finish(cinder::SpanStatus::Error, 1);
    }
    if (res->value.has_value()) {
        std::println("{}", res->value.value());
    } else if (res->status == cinder::Errc::OK) {
        std::println("OK");
    } else {
        std::println("{}", cinder::toString(res->status));
    }
    return admin_span.finish(cinder::SpanStatus::Ok, 0);
}
