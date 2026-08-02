#pragma once

#include <asio.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_connection.hpp"

namespace cinder {
class ReplicationManager;
class GossipManager;
} // namespace cinder

namespace cinder::net {

class TcpServer {
  public:

    TcpServer(asio::io_context& io, uint16_t port, CacheStore& store,
        const ConsistentHashRing& ring, std::string node_id, ReplicationManager* repl = nullptr,
        int replica_factor = 1, ConsistencyMode mode = ConsistencyMode::Async,
        GossipManager* gossip = nullptr);
    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    auto operator=(const TcpServer&) -> TcpServer& = delete;
    TcpServer(TcpServer&&) = delete;
    auto operator=(TcpServer&&) -> TcpServer& = delete;

    auto start() -> Result<void>;
    void shutdown();

  private:

    void doAccept();

    asio::ip::tcp::acceptor acceptor_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    std::string node_id_;
    ReplicationManager* repl_;
    int replica_factor_;
    ConsistencyMode mode_;
    GossipManager* gossip_;
    std::vector<std::shared_ptr<TcpConnection>> connections_;
};
} // namespace cinder::net
