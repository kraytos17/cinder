#pragma once

#include <asio.hpp>
#include <CLI/CLI.hpp>
#include <concepts>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cinder/common/tracing.hpp"

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#include <optional>
#endif

namespace cinder::tools {

    // Resolve the client authentication secret for CLI tools: an explicit
    // `--client-secret` flag wins, otherwise the `CINDER_CLIENT_SECRET`
    // environment variable, otherwise empty (client auth disabled).
    inline auto resolveClientSecret(const std::string& flag_value) -> std::string {
        if (!flag_value.empty()) {
            return flag_value;
        }
        // NOLINTNEXTLINE(concurrency-mt-unsafe): single-threaded startup
        if (const char* env = std::getenv("CINDER_CLIENT_SECRET")) {
            return env;
        }
        return {};
    }

    template <typename T>
    concept CliBindable = std::is_arithmetic_v<T>
        || std::same_as<T, std::string>
        || std::same_as<T, std::vector<std::string>>;

    // Bind flags/options uniformly. bool is the only type that maps to
    // CLI11's flag API; all other supported types are value-taking options.
    // Works for positionals too (a name without dashes is a positional).
    template <CliBindable T>
    auto bindFlag(CLI::App& app, std::string_view names, T& value, std::string_view help)
        -> CLI::Option* {
        if constexpr (std::same_as<T, bool>) {
            return app.add_flag(std::string(names), value, std::string(help));
        } else {
            return app.add_option(std::string(names), value, std::string(help));
        }
    }

    // Flag-wins, then YAML: centralizes CLI11's explicit-option check. The
    // flag argument must be one of the option's registered names (CLI11
    // lookup rejects composite forms like "-p,--port").
    template <typename T>
    void applyIfUnset(CLI::App& app, const char* flag, T& value, const T& config_value) {
        if (app.count(flag) == 0) {
            value = config_value;
        }
    }

    // Host/port/TLS/auth inputs shared by every client tool. Registered once
    // via bindFlag so all tools parse the same flag set.
    struct ClientEndpoint {
        std::string host = "127.0.0.1";
        uint16_t port = 7000;
        bool verbose = false;
        bool tls_enabled = false;
        std::string tls_cert_file;
        std::string tls_key_file;
        std::string tls_ca_file;
        std::string client_secret;
    };

    inline void addClientEndpointOptions(CLI::App& app, ClientEndpoint& endpoint) {
        bindFlag(app, "--host", endpoint.host, "Server host");
        bindFlag(app, "-p,--port", endpoint.port, "Server port");
        bindFlag(app, "-v,--verbose", endpoint.verbose, "Enable verbose (debug) logging");
        bindFlag(app, "--tls", endpoint.tls_enabled, "Enable TLS encryption");
        bindFlag(app, "--tls-cert", endpoint.tls_cert_file, "Path to TLS certificate chain (PEM)");
        bindFlag(app, "--tls-key", endpoint.tls_key_file, "Path to TLS private key (PEM)");
        bindFlag(
            app,
            "--tls-ca",
            endpoint.tls_ca_file,
            "Path to CA certificate for peer verification (PEM)"
        );
        bindFlag(
            app,
            "--client-secret",
            endpoint.client_secret,
            "Shared secret for client authentication (or CINDER_CLIENT_SECRET)"
        );
    }

#ifdef CINDER_ENABLE_TLS
    inline auto makeClientSslContext(const ClientEndpoint& endpoint)
        -> std::optional<asio::ssl::context> {
        if (!endpoint.tls_enabled) {
            return std::nullopt;
        }

        std::optional<asio::ssl::context> context(std::in_place, asio::ssl::context::tlsv12_client);
        if (!endpoint.tls_cert_file.empty()) {
            context->use_certificate_chain_file(endpoint.tls_cert_file);
        }
        if (!endpoint.tls_key_file.empty()) {
            context->use_private_key_file(endpoint.tls_key_file, asio::ssl::context::pem);
        }
        if (!endpoint.tls_ca_file.empty()) {
            context->load_verify_file(endpoint.tls_ca_file);
            context->set_verify_mode(asio::ssl::verify_peer);
        } else {
            context->set_verify_mode(asio::ssl::verify_none);
        }
        return context;
    }
#endif

    // One root span per invocation: command/host/port as attributes (never
    // keys, values, or the client secret). pool.send children join it via
    // thread-local context; finish reports the exit outcome at every return.
    struct CliSpan {
        CliSpan(std::string_view tool, const std::string& command, const ClientEndpoint& endpoint) :
            span("cli.command") {
            span.setAttribute({"tool", tool});
            span.setAttribute({"cmd", command});
            span.setAttribute({"host", endpoint.host});
            span.setAttribute({"port", endpoint.port});
        }

        auto finish(cinder::SpanStatus status, int code) -> int {
            span.setStatus(status);
            span.setAttribute({"exit_code", code});
            return code;
        }

        cinder::Span span;
    };
} // namespace cinder::tools
