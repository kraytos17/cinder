#pragma once

#include <asio.hpp>
#include <charconv>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#include <optional>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/failure_detector.hpp"
#include "cinder/cluster/gossip.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/common/config.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/net/tcp_transport.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/node/shard_manager.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/persistence.hpp"

using asio::io_context;
using asio::signal_set;
using asio::steady_timer;
using std::chrono::milliseconds;

namespace cinder {

// Options for constructing a CacheNodeServer.
struct CacheNodeServerOptions {
    std::string node_id;
    uint16_t port = 0;
    size_t capacity = 0;
    std::vector<ClusterConfig::NodeConfig> peers;
    int replica_factor = 1;
    ConsistencyMode mode = ConsistencyMode::Async;
    milliseconds ping_interval{1'000};
    milliseconds suspect_timeout{3'000};
    milliseconds gossip_interval{1'000};
    milliseconds quarantine_interval{10'000};
    // Persistence
    std::string data_dir;
    bool persistence_enabled = false;
    size_t snapshot_interval_s = 60;
    size_t max_wal_entries = 10'000;
    // Concurrency: worker threads running the io_context. 0 = auto
    // (min(4, hardware_concurrency)); 1 = legacy single-threaded reactor.
    int io_threads = 0;
    // TLS
    bool tls_enabled = false;
    std::string tls_cert_file;
    std::string tls_key_file;
    std::string tls_ca_file;
    std::string eviction_policy = "lru";
    // RPC deadline — max time for a single peer RPC before cancellation.
    milliseconds rpc_timeout{5'000};
    // Metrics HTTP endpoint (Prometheus /metrics). 0 = disabled.
    uint16_t metrics_port = 0;
    // Path to YAML config file (empty = no hot-reload).
    std::string config_path;
    // Loaded config for runtime reference and hot-reload.
    Config config;
};

// Parse a single "id@host:port" peer string into a NodeConfig. Returns false
// on malformed input (no '@' separator, missing port, or a non-numeric or
// out-of-range port). constexpr so it can be exercised at compile time.
inline constexpr auto
parsePeer(const std::string& peer, ClusterConfig::NodeConfig& out) -> bool {
    auto at = peer.rfind('@');
    if (at == std::string::npos) {
        return false;
    }

    auto colon = peer.find(':', at);
    if (colon == std::string::npos) {
        return false;
    }
    if (at == 0 || colon == at + 1) {
        return false;
    }

    const char* begin = peer.data() + colon + 1;
    const char* end = peer.data() + peer.size();
    uint32_t port = 0;
    auto [ptr, ec] = std::from_chars(begin, end, port);
    if (ec != std::errc{} || ptr != end || port > std::numeric_limits<uint16_t>::max()) {
        return false;
    }

    out.id = peer.substr(0, at);
    out.host = peer.substr(at + 1, colon - at - 1);
    out.port = static_cast<uint16_t>(port);
    return true;
}

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
    void scheduleGossip();
    void scheduleProbe();
    void scheduleEvict();
    void scheduleCompact();
    void rebuildRing();
    void scheduleRebalance();
    void scheduleConfigReload();
    void applyConfig();

    io_context io_;
    NodeId node_id_;
    milliseconds ping_interval_{1'000};
    milliseconds quarantine_interval_{10'000};
    int io_threads_ = 0; // resolved from CacheNodeServerOptions::io_threads
#ifdef CINDER_ENABLE_TLS
    std::optional<asio::ssl::context> ssl_ctx_;
#endif
    RealClock clock_;
    std::unique_ptr<CacheStore> store_;
    PersistenceManager persistence_;
    ConsistentHashRing ring_;
    TcpTransport transport_;
    ReplicationManager repl_;
    MembershipTable table_;
    FailureDetector detector_;
    GossipManager gossip_;
    ShardManager shard_;
    net::TcpServer server_;
    steady_timer replay_timer_;
    steady_timer gossip_timer_;
    steady_timer probe_timer_;
    steady_timer evict_timer_;
    steady_timer quarantine_timer_;
    steady_timer compact_timer_;
    steady_timer config_reload_timer_;
    signal_set signals_;
    MetricsCollector metrics_;
    uint16_t metrics_port_ = 0;
    Config current_config_;
    std::string config_path_;
};
} // namespace cinder
