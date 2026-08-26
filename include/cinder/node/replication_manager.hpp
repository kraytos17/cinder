#pragma once

#include <atomic>
#include <chrono>
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
#include "cinder/node/hint_queue.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

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
    using ReadCallback = std::move_only_function<void(Result<VersionedEntry>)>;
    using ReplayCallback = std::move_only_function<void(size_t)>;

    // Async write: commits locally, then fans out to replicas. `on_done` is
    // invoked on the io thread.
    //   Async  — immediately after local commit (fan-out is best-effort).
    //   Quorum — after W = R/2+1 acks (incl. local) are observed, or when all
    //            replicas answered with fewer than W (fails closed NotReady).
    void writeAsync(const std::string& key, std::string value, std::optional<milliseconds> ttl,
        const std::vector<NodeId>& replica_nodes, ConsistencyMode mode, WriteCallback on_done);

    // Quorum read: reads from local store + fans out GetVersioned to replicas.
    // Returns the entry with the highest version (LWW). Stale replicas are
    // repaired in the background via a best-effort Replicate write-back.
    void readAsync(const std::string& key, const std::vector<NodeId>& replica_nodes, size_t R,
        ReadCallback on_done);

    // Retry queued hints against now-healthy replicas. Invokes `on_done` with
    // the count replayed (expired hints are dropped). Call periodically.
    void replayHints(ReplayCallback on_done);
    auto hintCount() const -> size_t;

  private:

    static constexpr size_t K_MAX_HINTS = 1'024;
    static constexpr milliseconds K_HINT_TTL{30'000};

    // Shared per-write quorum accounting (owned by the in-flight sendAsync
    // callbacks; destroyed when the last one completes). Completion callbacks
    // may run concurrently on different io-pool threads, so `mu` guards the
    // counters and the single-shot `decided` transition; callbacks are invoked
    // with `mu` released.
    struct QuorumState {
        std::mutex mu;
        std::shared_ptr<WriteCallback> done;
        size_t acks = 0;
        size_t pending = 0;
        bool decided = false;
    };

    // Shared per-read quorum accounting. `best_*` forms a compound LWW maximum
    // updated by concurrent replica responses, so it is mutex-guarded rather
    // than atomic. Same single-shot `decided` contract as QuorumState.
    struct ReadQuorumState {
        std::mutex mu;
        std::shared_ptr<ReadCallback> done;
        std::vector<NodeId> replicas;
        Version best_version = 0;
        uint64_t best_writer_hash = 0;
        std::optional<VersionedEntry> best_entry;
        size_t acks = 0;
        size_t pending = 0;
        bool decided = false;
        bool needs_repair = false;
    };

    // Shared per-replay accounting across hint sendAsync callbacks.
    struct ReplayState {
        std::mutex mu;
        std::shared_ptr<ReplayCallback> done;
        size_t pending = 0;
        size_t replayed = 0;
        bool decided = false;
    };

    void enqueueHint(const NodeId& target, const net::Request& req);
    void sendRepairFanOut(
        const std::string& key, const std::vector<NodeId>& targets, const VersionedEntry& winner);

    CacheStore& local_;
    NodeId self_;
    Clock& clock_;
    Transport& transport_;
    HintQueue hints_;
    // Single-consumer guard for replayHints(): overlapping timer ticks (or
    // pool threads) must not run two replays concurrently.
    std::atomic<bool> replaying_{false};
};
} // namespace cinder
