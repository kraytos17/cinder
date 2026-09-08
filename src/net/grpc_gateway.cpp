#ifdef CINDER_ENABLE_GRPC

#include "cinder/net/grpc_gateway.hpp"

#include "cinder/common/logger.hpp"

namespace cinder::grpc {

// Defined in cache_service_impl.cpp — creates the gRPC service implementation.
extern auto
createCacheService(CacheNodeServer::GatewayHandle handle) -> std::unique_ptr<grpc::Service>;

GrpcGateway::GrpcGateway(CacheNodeServer::GatewayHandle handle, uint16_t port)
    : handle_(std::move(handle)),
      port_(port) {}

GrpcGateway::~GrpcGateway() {
    shutdown();
}

void
GrpcGateway::start() {
    service_ = createCacheService(handle_);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:" + std::to_string(port_), grpc::InsecureServerCredentials());

    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    Logger::info("gRPC gateway listening on port {}", port_);
}

void
GrpcGateway::shutdown() {
    if (server_) {
        Logger::info("gRPC gateway shutting down");
        server_->Shutdown();
        server_->Wait();
        server_.reset();
    }
}
} // namespace cinder::grpc
#endif
