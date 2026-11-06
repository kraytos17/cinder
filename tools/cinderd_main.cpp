#include <CLI/CLI.hpp>
#include <string>

#include "cinder/common/config.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/node/cache_node_server.hpp"
#include "daemon_args.hpp"

#ifdef CINDER_ENABLE_GRPC
#include <memory>

#include "cinder/net/grpc_gateway.hpp"
#endif

auto main(int argc, char* argv[]) -> int {
    CLI::App app{"Cinder distributed cache server"};
    cinder::CacheNodeServerOptions options;

    // CLI defaults for fields whose runtime defaults in CacheNodeServerOptions
    // are zero/empty on purpose (the server never assumes a port or id).
    options.port = 7000;
    options.node_id = "node1";
    options.capacity = 67'108'864;

    cinder::tools::DaemonCliArgs args;
    cinder::tools::addDaemonOptions(app, options, args);

    CLI11_PARSE(app, argc, argv);
    cinder::Config cfg;
    if (!args.config_path.empty()) {
        auto result = cinder::loadConfig(args.config_path);
        if (!result.has_value()) {
            cinder::initLogger("cinderd", cinder::LogLevel::Error);
            CINDER_ERROR("config error", {"reason", result.error().message()});
            return 1;
        }
        cfg = std::move(result.value());
    }

    cinder::initLogger("cinderd", cinder::logLevelFromString(args.log_level));
    cinder::tools::resolveDaemonOptions(app, options, args, cfg);
    CINDER_INFO("starting cinderd", {"port", options.port});

    // Snapshot the gateway's TLS inputs before options is moved into the
    // server below; reading them afterwards would be use-after-move. Only
    // needed when the gRPC gateway exists (its only consumer is #ifdef'd).
#ifdef CINDER_ENABLE_GRPC
    const std::string grpc_tls_cert_file = options.tls_cert_file;
    const std::string grpc_tls_key_file = options.tls_key_file;
#endif
    const uint16_t listen_port = options.port;
    const int replica_factor = options.replica_factor;
    cinder::CacheNodeServer server(std::move(options));
    auto result = server.start();
    if (!result.has_value()) {
        CINDER_ERROR("failed to start server", {"reason", result.error().message()});
        return 1;
    }

#ifdef CINDER_ENABLE_GRPC
    std::unique_ptr<cinder::grpc::GrpcGateway> grpc_gateway;
    auto actual_grpc_port = args.grpc_port > 0 ? args.grpc_port : cfg.grpc_port;
    if (actual_grpc_port > 0) {
        if (!server.clientAuthSecret().empty()) {
            // The gateway enforces the same client bearer over gRPC metadata
            // and requires TLS for it; without certificates startup already
            // failed in server.start(), this is the defense-in-depth check.
            if (grpc_tls_cert_file.empty() || grpc_tls_key_file.empty()) {
                CINDER_ERROR("client auth with gRPC requires TLS certificate and key");
                return 1;
            }
        }

        auto handle = server.gatewayHandle();
        grpc_gateway = std::make_unique<cinder::grpc::GrpcGateway>(
            std::move(handle),
            actual_grpc_port,
            server.clientAuthSecret(),
            grpc_tls_cert_file,
            grpc_tls_key_file
        );
        grpc_gateway->start();
    }
#endif

    CINDER_INFO(
        "listening",
        {"port", listen_port},
        {"replica_factor", replica_factor},
        {"consistency", args.consistency}
    );
    server.run();

#ifdef CINDER_ENABLE_GRPC
    if (grpc_gateway) {
        grpc_gateway->shutdown();
    }
#endif

    CINDER_INFO("stopped");
    return 0;
}
