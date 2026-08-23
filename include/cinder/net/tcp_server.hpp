#pragma once

#include <asio.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_connection.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {
class ReplicationManager;
class GossipManager;
} // namespace cinder

namespace cinder::net {

class TcpServer {
  public:

    TcpServer(io_context& io, uint16_t port, CacheStore& store, const ConsistentHashRing& ring,
        std::string node_id, Clock& clock, ReplicationManager* repl = nullptr,
        int replica_factor = 1, ConsistencyMode mode = ConsistencyMode::Async,
        GossipManager* gossip = nullptr
#ifdef CINDER_ENABLE_TLS
        ,
        asio::ssl::context* ssl_ctx = nullptr
#endif
    );

    ~TcpServer();

    TcpServer(const TcpServer&) = delete;
    auto operator=(const TcpServer&) -> TcpServer& = delete;
    TcpServer(TcpServer&&) = delete;
    auto operator=(TcpServer&&) -> TcpServer& = delete;

    auto start() -> Result<void>;
    void shutdown();

  private:

    void doAccept();

    tcp::acceptor acceptor_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    Clock& clock_;
    std::string node_id_;
    ReplicationManager* repl_;
    int replica_factor_;
    ConsistencyMode mode_;
    GossipManager* gossip_;
#ifdef CINDER_ENABLE_TLS
    asio::ssl::context* ssl_ctx_ = nullptr;
#endif
    std::vector<std::shared_ptr<TcpConnection>> connections_;
};
} // namespace cinder::net
