#pragma once

#include <ankerl/unordered_dense.h>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/common/types.hpp"
#include "cinder/node/hint_journal.hpp"
#include "cinder/node/hint_queue.hpp"
#include "cinder/node/read_quorum.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

    using std::chrono::duration_cast;
    using std::chrono::milliseconds;
    using std::chrono::steady_clock;
    using std::chrono::system_clock;

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

        // Bound on hints replayed per replayHints call (FIFO from the queue
        // head). A full 1024-slot backlog drains in four 1-second ticks
        // instead of one 1024-wide fan-out, bounding per-tick sends, channel
        // memory, and lock hold time.
        static constexpr size_t K_REPLAY_BATCH = 256;

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

        // Async delete: tombstone locally, then fan out to replicas.
        //   Async  — `on_done` runs inline after local commit (fan-out is
        //            best-effort).
        //   Quorum — `on_done` runs after W = R/2+1 acks (incl. local) are
        //            observed, mirroring writeAsync. A quorum delete returns
        //            NotFound to no one — success means a quorum holds the
        //            tombstone, so later reads observe the delete.
        void deleteAsync(
            const std::string& key,
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
        // Bounded: at most K_REPLAY_BATCH hints per call, FIFO; the remainder
        // stays queued for the next call.
        void replayHints(ReplayCallback on_done);
        auto hintCount() const -> size_t;

        // Cap on HintQueue estimated bytes (drop-oldest past it). Applied
        // before serving; restart-only.
        void setMaxHintBytes(size_t max_bytes) {
            hints_.setMaxBytes(max_bytes);
        }

        auto hintsDropped() const -> uint64_t {
            return hints_.dropped();
        }

        // Durable hints: journal path is set at startup (data directory);
        // recovery runs explicitly before serving. No-op when unset.
        void setHintJournal(std::string path) {
            hint_journal_path_ = std::move(path);
        }

        auto recoverHints() -> uint64_t;
        void syncHints();

        void setMetrics(MetricsCollector* m) {
            metrics_ = m;
            hints_.setMetrics(
                m == nullptr ? nullptr : &m->replicationMetrics().hint_queue_entries,
                m == nullptr ? nullptr : &m->replicationMetrics().hint_queue_estimated_bytes,
                m == nullptr ? nullptr : &m->replicationMetrics().hint_queue_estimated_bytes_peak
            );
        }

    private:

        static constexpr size_t K_MAX_HINTS = 1024;
        static constexpr milliseconds K_HINT_TTL{30'000};

        static auto sysHintExpiryMs() -> uint64_t {
            return static_cast<uint64_t>(
                       duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                   )
                + static_cast<uint64_t>(K_HINT_TTL.count());
        }

        void enqueueHint(const NodeId& target, const net::Request& req);
        // Shared write/delete fan-out after the local commit: best-effort
        // async fan-out with hint-on-failure, or quorum reduce to W. The
        // request carries the tombstone bit for deletes; spans and metrics
        // follow it. `req` is taken by value (moved into async callbacks).
        void fanOutWrite(
            const std::string& key,
            net::Request req,
            const std::vector<NodeId>& replica_nodes,
            ConsistencyMode mode,
            WriteCallback on_done
        );
        // Repair fan-out parented to the caller (the read quorum that found
        // divergence), so the repair is a child span rather than a fresh root.
        void sendRepairFanOut(
            const std::string& key,
            const std::vector<NodeId>& targets,
            const VersionedEntry& winner,
            SpanContext parent
        );

        // Read-flight state lives in ReadFlightRegistry (see read_quorum.hpp);
        // the manager keeps only the registry.
        CacheStore& local_;
        NodeId self_;
        Clock& clock_;
        Transport& transport_;
        HintQueue hints_;
        std::string hint_journal_path_;
        std::unique_ptr<HintJournal> journal_;
        // Single-consumer guard for replayHints(): overlapping timer ticks (or
        // pool threads) must not run two replays concurrently.
        std::atomic<bool> replaying_{false};
        // Coalesced reads: the registry owns the in-flight table (keyed by
        // key + read quorum). Leaf-level mutex, held only for map
        // insert/find/erase — never across I/O or store locks.
        ReadFlightRegistry<ReadCallback, Result<VersionedEntry>> read_flights_;
        MetricsCollector* metrics_ = nullptr;
    };
} // namespace cinder
