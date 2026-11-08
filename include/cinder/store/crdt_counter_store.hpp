#pragma once

// Sharded CRDT counter store: key -> PNCounter with merge-based writes.
//
// This store is intentionally separate from EvictionStoreBase/LruStore.
// Those paths apply LWW (version, writer) order to whole values, which
// would drop concurrent increments — the exact update loss CRDT counters
// exist to prevent. Here every write path merges: local increments merge
// into the resident counter, and replicated state merges component-wise,
// so concurrent updates on any replica survive regardless of arrival order
// or duplication.
//
// Deliberate non-goals for this slice (documented, not accidental):
//   - No TTL wheel: counters are state, not cache entries; expiry would
//     silently discard increments. There is no expiry path at all.
//   - No capacity eviction: over-budget writes fail with
//     CapacityExceeded instead of evicting a neighbor. Eviction here
//     would be silent data loss, not cache pressure relief. Lowering the
//     capacity below current use only rejects future growth; existing
//     state is never touched.
//   - No tombstones: remove() erases locally, and a merge from a peer
//     that still holds the key resurrects it. Distributed delete needs a
//     tombstone/gc design; until then remove() is an operator-local escape
//     hatch, not a replicated delete.
//   - Not wired anywhere yet: no server, replication, anti-entropy,
//     WAL, or snapshot integration. liveCounters() is the snapshot
//     accessor those paths will consume. Counter bytes must never flow
//     through the LWW putVersioned path.
//
// Threading mirrors the eviction stores: keys stripe over shards (one per
// 64 KiB, clamped to [1, 16]), one shared_mutex per shard, copy-out reads.

#include <algorithm>
#include <btree/map.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/crdt_counter.hpp"

namespace cinder {

    class CrdtCounterStore {
    public:

        explicit CrdtCounterStore(size_t capacity_bytes) :
            capacity_bytes_(capacity_bytes),
            shards_(shardCountFor(capacity_bytes)) {}

        CrdtCounterStore(const CrdtCounterStore&) = delete;
        auto operator=(const CrdtCounterStore&) -> CrdtCounterStore& = delete;
        CrdtCounterStore(CrdtCounterStore&&) = delete;
        auto operator=(CrdtCounterStore&&) -> CrdtCounterStore& = delete;

        // Add delta to this key's counter, attributed to actor. Creates the
        // key when absent. Empty actors and component overflow fail without
        // mutating anything; over-budget growth fails with CapacityExceeded.
        auto increment(const std::string& key, const NodeId& actor, uint64_t delta = 1)
            -> Result<void> {
            return adjust(key, actor, delta, true);
        }

        auto decrement(const std::string& key, const NodeId& actor, uint64_t delta = 1)
            -> Result<void> {
            return adjust(key, actor, delta, false);
        }

        // Merge replicated state into the resident counter (idempotent:
        // replaying the same state is a no-op). Over-budget merges fail
        // with CapacityExceeded and leave the resident untouched, so a
        // retried merge converges instead of partially applying.
        auto merge(const std::string& key, const crdt::PNCounter& remote) -> Result<void> {
            if (remote.empty()) {
                return ok();
            }

            auto& sh = shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);
            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                const size_t need = counterBytes(key, remote);
                if (need > shardCapacity() || sh.current_bytes > shardCapacity() - need) {
                    return err(Error(Errc::CapacityExceeded, "counter exceeds capacity"));
                }

                sh.current_bytes += need;
                sh.index.emplace(key, remote);
                return ok();
            }

            crdt::PNCounter merged = it->second;
            merged.merge(remote);
            const size_t old_size = counterBytes(key, it->second);
            const size_t new_size = counterBytes(key, merged);
            // Merge only adds components, so the size never shrinks; the
            // subtraction below cannot underflow.
            const size_t growth = new_size - old_size;
            if (growth
                > 0
                && (growth > shardCapacity() || sh.current_bytes > shardCapacity() - growth)) {
                return err(Error(Errc::CapacityExceeded, "counter exceeds capacity"));
            }

            sh.current_bytes += growth;
            it->second = std::move(merged);
            return ok();
        }

        // Current value, or nullopt when the key is absent. No tombstone or
        // expiry filtering: counters have neither.
        [[nodiscard]]
        auto value(const std::string& key) const -> std::optional<int64_t> {
            const auto& sh = shards_[shardFor(key)];
            std::shared_lock lock(sh.mutex);
            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                return std::nullopt;
            }
            return it->second.value();
        }

        // Full state copy for replication sends and snapshots.
        [[nodiscard]]
        auto get(const std::string& key) const -> std::optional<crdt::PNCounter> {
            const auto& sh = shards_[shardFor(key)];
            std::shared_lock lock(sh.mutex);
            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                return std::nullopt;
            }
            return it->second;
        }

        // Local-only erase (see the tombstone note above). Returns whether
        // anything was removed.
        auto remove(const std::string& key) -> bool {
            auto& sh = shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);
            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                return false;
            }

            sh.current_bytes -= counterBytes(key, it->second);
            sh.index.erase(it);
            return true;
        }

        [[nodiscard]]
        auto size() const -> size_t {
            size_t total = 0;
            for (const auto& sh : shards_) {
                std::shared_lock lock(sh.mutex);
                total += static_cast<size_t>(sh.index.size());
            }
            return total;
        }

        // Change the budget. Never evicts: shards already over the new
        // slice only reject growth until removals bring them back under.
        void setCapacity(size_t new_capacity) {
            capacity_bytes_.store(new_capacity, std::memory_order_relaxed);
        }

        // Point-in-time copy of every counter, for snapshots and migration.
        // The caller may mutate the store freely while iterating the result.
        [[nodiscard]]
        auto liveCounters() const -> std::vector<std::pair<std::string, crdt::PNCounter>> {
            std::vector<std::pair<std::string, crdt::PNCounter>> items;
            for (const auto& sh : shards_) {
                std::shared_lock lock(sh.mutex);
                for (const auto& entry : sh.index) {
                    items.emplace_back(entry.first, entry.second);
                }
            }
            return items;
        }

    private:

        // Shared increment/decrement body; positive selects the P side.
        auto adjust(const std::string& key, const NodeId& actor, uint64_t delta, bool positive)
            -> Result<void> {
            if (delta == 0) {
                return ok();
            }

            auto& sh = shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);
            auto apply = [&](crdt::PNCounter& counter) -> Result<void> {
                return positive ? counter.increment(actor, delta) : counter.decrement(actor, delta);
            };

            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                crdt::PNCounter fresh;
                if (auto applied = apply(fresh); !applied.has_value()) {
                    return applied;
                }

                const size_t need = counterBytes(key, fresh);
                if (need > shardCapacity() || sh.current_bytes > shardCapacity() - need) {
                    return err(Error(Errc::CapacityExceeded, "counter exceeds capacity"));
                }

                sh.current_bytes += need;
                sh.index.emplace(key, std::move(fresh));
                return ok();
            }

            crdt::PNCounter& resident = it->second;
            // P and N are independent G-Counters: adding the first positive
            // component for an actor does not mean its negative component
            // already exists (and vice versa). Charge each newly-created
            // side separately, matching counterBytes().
            const bool side_missing = positive ? resident.positive().component(actor) == 0
                                               : resident.negative().component(actor) == 0;
            const size_t growth = side_missing ? addSat(actor.size(), 2 * sizeof(uint64_t)) : 0;
            if (growth
                > 0
                && (growth > shardCapacity() || sh.current_bytes > shardCapacity() - growth)) {
                return err(Error(Errc::CapacityExceeded, "counter exceeds capacity"));
            }
            if (auto applied = apply(resident); !applied.has_value()) {
                return applied;
            }

            sh.current_bytes += growth;
            return ok();
        }

        struct Shard {
            mutable std::shared_mutex mutex;
            btree::map<std::string, crdt::PNCounter> index;
            size_t current_bytes = 0;
        };

        static constexpr size_t K_MAX_SHARDS = 16;
        static constexpr size_t K_MIN_SHARD_BYTES = 65'536;

        [[nodiscard]]
        static auto shardCountFor(size_t capacity_bytes) -> size_t {
            size_t n = capacity_bytes / K_MIN_SHARD_BYTES;
            n = std::max<size_t>(n, 1);
            n = std::min(n, K_MAX_SHARDS);
            return n;
        }

        // Saturating add for capacity accounting: actor-controlled lengths
        // must not wrap the budget check.
        [[nodiscard]]
        static auto addSat(size_t a, size_t b) -> size_t {
            if (b > std::numeric_limits<size_t>::max() - a) {
                return std::numeric_limits<size_t>::max();
            }
            return a + b;
        }

        // Billable bytes: key plus every actor id plus its two component
        // slots. Deterministic and cheap; recomputed on merge rather than
        // cached per write.
        [[nodiscard]]
        static auto counterBytes(const std::string& key, const crdt::PNCounter& counter) -> size_t {
            size_t total = key.size();
            const crdt::GCounter* sides[2] = {&counter.positive(), &counter.negative()};
            for (const crdt::GCounter* side : sides) {
                for (const auto& entry : side->components()) {
                    total = addSat(total, entry.first.size());
                    total = addSat(total, 2 * sizeof(uint64_t));
                }
            }
            return total;
        }

        [[nodiscard]]
        auto shardFor(std::string_view key) const -> size_t {
            return std::hash<std::string_view>{}(key) % shards_.size();
        }

        [[nodiscard]]
        auto shardCapacity() const -> size_t {
            return capacity_bytes_.load(std::memory_order_relaxed) / shards_.size();
        }

        std::atomic<size_t> capacity_bytes_;
        std::vector<Shard> shards_;
    };
} // namespace cinder
