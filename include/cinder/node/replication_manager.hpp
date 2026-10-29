#pragma once

#include <ankerl/unordered_dense.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
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

        // Callback contract for all three callbacks below: each fires exactly
        // once, but on no particular thread. When every completion is already
        // available the callback runs inline on the calling thread (so fully
        // synchronous transports complete synchronously); otherwise it runs on
        // Asio's system thread pool once the outstanding completions drain.
        // Callbacks must therefore be thread-safe and non-blocking, and must
        // re-post (e.g. asio::post to a strand) before touching strand-bound
        // state. Reduction itself is serialized — see fanOutQuorum.
        using WriteCallback = std::move_only_function<void(Result<void>)>;
        using ReadCallback = std::move_only_function<void(Result<VersionedEntry>)>;
        using ReplayCallback = std::move_only_function<void(size_t)>;

        // Async write: commits locally, then fans out to replicas.
        //   Async  — `on_done` runs inline after local commit (fan-out is
        //            best-effort).
        //   Quorum — `on_done` runs after W = R/2+1 acks (incl. local) are
        //            observed, when quorum becomes unreachable (fails closed
        //            NotReady without waiting out stragglers), or when all
        //            replicas answer. RPC deadlines are enforced by Transport;
        //            there is no separate fan-out deadline.
        void writeAsync(
            const std::string& key,
            std::string value,
            std::optional<milliseconds> ttl,
            const std::vector<NodeId>& replica_nodes,
            ConsistencyMode mode,
            WriteCallback on_done,
            uint64_t trace_id = 0,
            uint64_t span_id = 0
        );

        // Quorum read: reads from local store + fans out GetVersioned to replicas.
        // Returns the entry with the highest version (LWW). Stale replicas are
        // repaired in the background via a best-effort Replicate write-back.
        //
        // Duplicate suppression: concurrent reads for the same key share one
        // fan-out (singleflight). The first caller becomes the leader and runs
        // the quorum reduce; later arrivals attach as waiters and receive the
        // leader's LWW result (copies for all but the last waiter, which is
        // moved). Waiters never block — completion is async. The leader's
        // replica set and R win; any quorum answer is a valid LWW result.
        void readAsync(
            const std::string& key,
            const std::vector<NodeId>& replica_nodes,
            size_t R,
            ReadCallback on_done,
            uint64_t trace_id = 0,
            uint64_t span_id = 0
        );

        // Retry queued hints against now-healthy replicas. Invokes `on_done` with
        // the count replayed (expired hints are dropped). Call periodically.
        void replayHints(ReplayCallback on_done);
        auto hintCount() const -> size_t;

        void setMetrics(MetricsCollector* m) {
            metrics_ = m;
        }

    private:

        static constexpr size_t K_MAX_HINTS = 1024;
        static constexpr milliseconds K_HINT_TTL{30'000};

        void enqueueHint(const NodeId& target, const net::Request& req);
        void sendRepairFanOut(
            const std::string& key,
            const std::vector<NodeId>& targets,
            const VersionedEntry& winner
        );

        // One in-flight coalesced read. Waiters share the leader's LWW result;
        // the table holds one entry per distinct in-flight key.
        struct ReadFlight {
            std::vector<ReadCallback> waiters;
        };

        // Complete a coalesced read: erase-then-notify so a waiter that
        // synchronously starts a new read for the same key begins a fresh
        // flight instead of deadlocking on itself.
        void finishReadFlight(
            const std::string& key,
            const std::shared_ptr<ReadFlight>& flight,
            Result<VersionedEntry> result
        );

        CacheStore& local_;
        NodeId self_;
        Clock& clock_;
        Transport& transport_;
        HintQueue hints_;
        // Single-consumer guard for replayHints(): overlapping timer ticks (or
        // pool threads) must not run two replays concurrently.
        std::atomic<bool> replaying_{false};
        // Coalesced reads: key -> in-flight flight. Leaf-level mutex, held
        // only for map insert/find/erase — never across I/O or store locks.
        std::mutex read_flights_mutex_;
        ankerl::unordered_dense::map<std::string, std::shared_ptr<ReadFlight>> read_flights_;
        MetricsCollector* metrics_ = nullptr;
    };
} // namespace cinder
