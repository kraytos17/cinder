#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

using namespace std::chrono;

// Coordinates writes to the primary + replicas. The write path assigns a
// monotonic version and a stable per-node writer hash so replica applies are
// idempotent and commutative (LWW).
//
//   Async  — local write + best-effort fan-out to replicas.
//   Quorum — local write + fan-out, but requires W = R/2+1 acknowledgements
//            (including the local write) to succeed; otherwise fails closed.
//
// Replicas that are unreachable during a write get a Hint queued (bounded +
// TTL) and are replayed once healthy by replayHints().
class ReplicationManager {
  public:

    ReplicationManager(CacheStore& local, NodeId self, Clock& clock, Transport& transport);
    ~ReplicationManager() = default;
    ReplicationManager(const ReplicationManager&) = delete;
    auto operator=(const ReplicationManager&) -> ReplicationManager& = delete;
    ReplicationManager(ReplicationManager&&) = delete;
    auto operator=(ReplicationManager&&) -> ReplicationManager& = delete;

    auto write(const std::string& key, std::string value, std::optional<milliseconds> ttl,
        const std::vector<NodeId>& replica_nodes, ConsistencyMode mode) -> Result<void>;

    // Retry queued hints against now-healthy replicas. Returns count replayed
    // (expired hints are dropped). Call periodically.
    auto replayHints() -> size_t;
    auto hintCount() const -> size_t;

  private:

    static constexpr size_t K_MAX_HINTS = 1'024;
    static constexpr milliseconds K_HINT_TTL{30'000};

    struct Hint {
        NodeId target;
        net::Request req;
        steady_clock::time_point expires_at;
    };

    void enqueueHint(const NodeId& target, const net::Request& req);

    CacheStore& local_;
    NodeId self_;
    Clock& clock_;
    Transport& transport_;
    std::atomic<Version> version_{1};

    mutable std::mutex hints_mutex_;
    std::deque<Hint> hints_;
};
} // namespace cinder
