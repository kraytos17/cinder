#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

    class MetricsCollector;
    class PersistenceManager;

    class CacheStore {
    public:

        explicit CacheStore(Clock* clock = nullptr) :
            clock_(clock) {}

        virtual ~CacheStore() = default;

        CacheStore(const CacheStore&) = delete;
        auto operator=(const CacheStore&) -> CacheStore& = delete;
        CacheStore(CacheStore&&) = delete;
        auto operator=(CacheStore&&) -> CacheStore& = delete;

        virtual auto put(
            const std::string& key,
            std::string value,
            std::optional<milliseconds> ttl = std::nullopt
        ) -> Result<void> = 0;
        virtual auto get(const std::string& key) -> std::optional<std::string> = 0;
        virtual auto remove(const std::string& key) -> bool = 0;
        [[nodiscard]]
        virtual auto size() const -> size_t = 0;
        virtual auto evictExpired() -> size_t = 0;

        // Versioned write path — used by replication. Applies idempotently:
        // accepts iff incoming.version > local.version, or == with
        // writer_node_hash >= local.writer_node_hash (tiebreak).
        virtual auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> = 0;
        virtual auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> = 0;

        // Versioned delete: writes a tombstone marker (empty value + tombstone
        // bit) that carries (version, writer) and wins or loses LWW exactly
        // like a put. Unlike remove(), the marker survives in the index until
        // tombstone GC so replicas and anti-entropy observe the delete.
        // Stale deletes lose to newer writes — delete-vs-write resolves by
        // the same (version, writer) order as write-vs-write.
        virtual auto removeVersioned(const std::string& key, Version version, uint64_t writer)
            -> Result<void> = 0;

        // Mint a monotonic local version for a locally-originated write. The store
        // is the single version authority regardless of which write path is used
        // (direct put() or ReplicationManager). Thread-safe (store mutex).
        virtual auto mintVersion() -> Version = 0;

        // Push-based visitor: the visitor is invoked outside the store lock over
        // a point-in-time snapshot, so it may safely call store mutators
        // (e.g. remove) without self-deadlocking.
        virtual void forEach(
            std::move_only_function<void(const std::string& key, const VersionedEntry&)>
        ) const = 0;

        // Snapshot accessor: point-in-time copy of all present entries,
        // including tombstones (deleted-but-not-yet-GC'd markers), expired
        // entries skipped. Tombstones must travel with the data — snapshots,
        // migration, and anti-entropy all need them to avoid resurrecting
        // deleted keys. Filter with entry.isTombstone() for live-only views.
        // Prefer this over forEach when the consumer needs filtering, early
        // exit, or accumulation — a plain range-for replaces the callback.
        // Same contract as forEach: the store may be mutated freely while
        // iterating the returned vector.
        virtual auto liveEntries() const -> std::vector<std::pair<std::string, VersionedEntry>> = 0;

        // Mutation observer for incremental digest maintenance (anti-entropy).
        // Invoked synchronously for every accepted write, remove, and eviction
        // while the caller holds the store's shard lock, so the sink MUST be
        // leaf-only: never call back into the store (deadlock), never block.
        // Removal passes removed=true; the XOR toggle downstream is
        // self-inverse, so add and remove converge on the same operation.
        using MutationSink = std::move_only_function<
            void(std::string_view key, uint64_t version_and_ttl, uint64_t writer, bool removed)
        >;

        // Register the mutation sink. Default no-op (stores without AE).
        virtual void setMutationSink(MutationSink /*sink*/) {}

        // Set the persistence manager for WAL writes. Default no-op for stores
        // that don't support persistence (e.g. simulation stores).
        virtual void setPersistence(PersistenceManager* /*unused*/) {}

        // Set the metrics collector for observability. Default no-op.
        virtual void setMetrics(MetricsCollector* /*unused*/) {}

        // Change the capacity at runtime. Default no-op.
        virtual void setCapacity(size_t /*unused*/) {}

        // Frequency hint for snapshot persistence (LFU policy state).
        // Default 0 (LRU has no frequencies). Recovered via restoreFreq so
        // a restart preserves hot-set information instead of resetting it.
        virtual auto entryFreq(const std::string& /*key*/) const -> size_t {
            return 0;
        }

        // Restore frequency state after a snapshot replay; default no-op.
        // Never demotes: a newer access (higher freq) always wins.
        virtual void restoreFreq(const std::string& /*key*/, size_t /*freq*/) {}

        // Tombstone GC grace: how long a delete marker survives before the
        // TTL-wheel reaps it. Must exceed the worst-case partition healing
        // time (hint TTL + several anti-entropy rounds); premature GC
        // resurrects the key from any replica that missed the delete.
        // Default no-op (stores without tombstone support).
        virtual void setTombstoneGcInterval(milliseconds /*unused*/) {}

    protected:

        // Injected clock for deterministic sims; nullptr → real steady_clock.
        auto now() const -> steady_clock::time_point {
            return clock_ ? clock_->now() : steady_clock::now();
        }

        // Wall-clock milliseconds for a steady expiry, honoring the injected
        // clock so WAL bytes are deterministic under SimClock.
        auto expiryToSystemMs(steady_clock::time_point t) const -> uint64_t {
            return toSystemMsOrDefault(clock_, t);
        }

    private:

        Clock* clock_ = nullptr;
    };
} // namespace cinder
