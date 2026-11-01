#ifdef CINDER_ENABLE_GRPC

#include "cinder/net/grpc_gateway.hpp"

#include <fstream>
#include <sstream>
#include <string>

#include "cinder/common/tracing.hpp"

namespace cinder::grpc {
    namespace {
        auto readPemFile(const std::string& path) -> std::string {
            std::ifstream file(path);
            if (!file.is_open()) {
                return {};
            }

            std::ostringstream out;
            out << file.rdbuf();
            return out.str();
        }
    } // namespace

    // Defined in cache_service_impl.cpp — creates the gRPC service implementation.
    extern auto createCacheService(
        CacheNodeServer::GatewayHandle handle,
        std::string client_shared_secret = {}
    ) -> std::unique_ptr<grpc::Service>;

    GrpcGateway::GrpcGateway(
        CacheNodeServer::GatewayHandle handle,
        uint16_t port,
        std::string client_shared_secret,
        std::string tls_cert_file,
        std::string tls_key_file
    ) :
        handle_(std::move(handle)),
        port_(port),
        client_shared_secret_(std::move(client_shared_secret)),
        tls_cert_file_(std::move(tls_cert_file)),
        tls_key_file_(std::move(tls_key_file)) {}

    GrpcGateway::~GrpcGateway() {
        shutdown();
    }

    void GrpcGateway::start() {
        Span span("grpc.gateway.start");
        service_ = createCacheService(handle_, client_shared_secret_);
        grpc::ServerBuilder builder;
        std::shared_ptr<grpc::ServerCredentials> creds;
        if (!client_shared_secret_.empty()) {
            // Client auth is replayable — refuse to serve it over plaintext.
            if (tls_cert_file_.empty() || tls_key_file_.empty()) {
                CINDER_ERROR("gRPC client auth requires TLS certificate and key");
                return;
            }

            grpc::SslServerCredentialsOptions opts;
            opts.pem_key_cert_pairs.push_back(
                {readPemFile(tls_key_file_), readPemFile(tls_cert_file_)}
            );
            creds = grpc::SslServerCredentials(opts);
        } else {
            creds = grpc::InsecureServerCredentials();
        }

        builder.AddListeningPort("0.0.0.0:" + std::to_string(port_), creds);
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        CINDER_INFO("gRPC gateway listening on port", {"port", port_});
    }

    void GrpcGateway::shutdown() {
        Span span("grpc.gateway.shutdown");
        if (server_) {
            CINDER_INFO("gRPC gateway shutting down");
            server_->Shutdown();
            server_->Wait();
            server_.reset();
        }
    }
} // namespace cinder::grpc
#endif
