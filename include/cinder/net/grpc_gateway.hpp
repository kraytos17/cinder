#pragma once
#ifdef CINDER_ENABLE_GRPC

#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

#include "cinder/node/cache_node_server.hpp"

namespace cinder::grpc {

// gRPC gateway that translates protobuf RPCs into internal cache operations.
// Runs on a separate port with gRPC's own thread pool, sharing the same
// CacheStore, ConsistentHashRing, ReplicationManager, and MembershipTable
// as the TCP server.
class GrpcGateway {
  public:

    GrpcGateway(CacheNodeServer::GatewayHandle handle, uint16_t port);
    ~GrpcGateway();

    GrpcGateway(const GrpcGateway&) = delete;
    auto operator=(const GrpcGateway&) -> GrpcGateway& = delete;
    GrpcGateway(GrpcGateway&&) = delete;
    auto operator=(GrpcGateway&&) -> GrpcGateway& = delete;

    void start();
    void shutdown();

  private:

    CacheNodeServer::GatewayHandle handle_;
    std::unique_ptr<grpc::Service> service_;
    std::unique_ptr<grpc::Server> server_;
    uint16_t port_;
};

} // namespace cinder::grpc
#endif
