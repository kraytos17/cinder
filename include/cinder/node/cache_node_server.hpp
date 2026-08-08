#pragma once

#include <asio.hpp>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <system_error>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/failure_detector.hpp"
#include "cinder/cluster/gossip.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/net/tcp_transport.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/node/shard_manager.hpp"
#include "cinder/store/lru_store.hpp"

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
    void rebuildRing();
    void scheduleRebalance();

    io_context io_;
    NodeId node_id_;
    milliseconds ping_interval_{1'000};
    milliseconds quarantine_interval_{10'000};
    LruStore store_;
    RealClock clock_;
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
    signal_set signals_;
};
} // namespace cinder
