#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
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

    using WriteCallback = std::move_only_function<void(Result<void>)>;
    using ReplayCallback = std::move_only_function<void(size_t)>;

    // Async write: commits locally, then fans out to replicas. `on_done` is
    // invoked on the io thread.
    //   Async  — immediately after local commit (fan-out is best-effort).
    //   Quorum — after W = R/2+1 acks (incl. local) are observed, or when all
    //            replicas answered with fewer than W (fails closed NotReady).
    void writeAsync(const std::string& key, std::string value, std::optional<milliseconds> ttl,
        const std::vector<NodeId>& replica_nodes, ConsistencyMode mode, WriteCallback on_done);

    // Retry queued hints against now-healthy replicas. Invokes `on_done` with
    // the count replayed (expired hints are dropped). Call periodically.
    void replayHints(ReplayCallback on_done);
    auto hintCount() const -> size_t;

  private:

    static constexpr size_t K_MAX_HINTS = 1'024;
    static constexpr milliseconds K_HINT_TTL{30'000};

    struct Hint {
        NodeId target;
        net::Request req;
        steady_clock::time_point expires_at;
    };

    // Shared per-write quorum accounting (owned by the in-flight sendAsync
    // callbacks; destroyed when the last one completes).
    struct QuorumState {
        std::shared_ptr<WriteCallback> done;
        size_t acks = 0;
        size_t pending = 0;
        bool done_flag = false;
    };

    // Shared per-replay accounting across hint sendAsync callbacks.
    struct ReplayState {
        std::shared_ptr<ReplayCallback> done;
        size_t pending = 0;
        size_t replayed = 0;
    };

    void enqueueHint(const NodeId& target, const net::Request& req);
    void removeHint(const Hint& hint);

    CacheStore& local_;
    NodeId self_;
    Clock& clock_;
    Transport& transport_;
    std::atomic<Version> version_{1};

    mutable std::mutex hints_mutex_;
    std::deque<Hint> hints_;
};
} // namespace cinder
