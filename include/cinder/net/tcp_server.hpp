#pragma once

#include <asio.hpp>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/cluster/clock.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_connection.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {
class ReplicationManager;
class GossipManager;
class AntiEntropyManager;
} // namespace cinder

namespace cinder::net {

struct AdminCallbacks {
    std::function<std::string()> info_getter;
    std::function<std::string()> cluster_getter;
    std::function<std::string()> ring_getter;
    std::function<void()> compact_trigger;
    std::function<void()> config_reload_trigger;
    std::function<void()> shutdown_trigger;
};

class TcpServer {
  public:

    TcpServer(io_context& io, uint16_t port, CacheStore& store, const ConsistentHashRing& ring,
        std::string node_id, Clock& clock, ReplicationManager* repl = nullptr,
        int replica_factor = 1, ConsistencyMode mode = ConsistencyMode::Async,
        GossipManager* gossip = nullptr, uint16_t metrics_port = 0,
        MetricsCollector* metrics = nullptr, std::function<std::string()> config_getter = nullptr,
        AntiEntropyManager* anti_entropy = nullptr
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

    void setAdminCallbacks(AdminCallbacks cb) { admin_callbacks_ = std::move(cb); }

    // Hard cap on concurrent client connections. Beyond this the acceptor
    // rejects new sockets instead of buffering unbounded file descriptors.
    static constexpr size_t K_MAX_CONNECTIONS = 10'000;

  private:

    void doAccept();
    void doAcceptMetrics();

    asio::strand<io_context::executor_type> strand_;
    bool stopping_ = false;
    tcp::acceptor acceptor_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    Clock& clock_;
    std::string node_id_;
    ReplicationManager* repl_;
    int replica_factor_;
    ConsistencyMode mode_;
    GossipManager* gossip_;
    AntiEntropyManager* anti_entropy_ = nullptr;
    MetricsCollector* metrics_ = nullptr;
#ifdef CINDER_ENABLE_TLS
    asio::ssl::context* ssl_ctx_ = nullptr;
#endif
    std::vector<std::shared_ptr<TcpConnection>> connections_;
    std::unique_ptr<tcp::acceptor> metrics_acceptor_;
    std::function<std::string()> config_getter_;
    std::atomic<size_t> active_connections_{0};
    AdminCallbacks admin_callbacks_;
};
} // namespace cinder::net
