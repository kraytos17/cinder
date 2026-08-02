#pragma once

#include <asio.hpp>
#include <cstdint>
#include <string>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/net/tcp_transport.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {

// Options for constructing a CacheNodeServer.
struct CacheNodeServerOptions {
    std::string node_id;
    uint16_t port = 0;
    size_t capacity = 0;
    std::vector<ClusterConfig::NodeConfig> peers;
    int replica_factor = 1;
    ConsistencyMode mode = ConsistencyMode::Async;
};

// Parse a single "id@host:port" peer string into a NodeConfig. Returns false
// on malformed input (no '@' separator or missing port).
auto
parsePeer(const std::string& peer, ClusterConfig::NodeConfig& out) -> bool;

// Assembles a single cache node: store + ring + replication transport +
// TCP server, plus periodic hinted-handoff replay and signal-driven shutdown.
// This is the object a `cinderd` process hosts.
class CacheNodeServer {
  public:

    explicit CacheNodeServer(CacheNodeServerOptions options);
    ~CacheNodeServer() = default;

    CacheNodeServer(const CacheNodeServer&) = delete;
    auto operator=(const CacheNodeServer&) -> CacheNodeServer& = delete;
    CacheNodeServer(CacheNodeServer&&) = delete;
    auto operator=(CacheNodeServer&&) -> CacheNodeServer& = delete;

    auto start() -> Result<void>;
    void run();
    void shutdown();

  private:

    void scheduleReplay();

    asio::io_context io_;
    LruStore store_;
    RealClock clock_;
    ConsistentHashRing ring_;
    TcpTransport transport_;
    ReplicationManager repl_;
    net::TcpServer server_;
    asio::steady_timer replay_timer_;
    asio::signal_set signals_;
};
} // namespace cinder
