#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/slab_allocator.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/persistence.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {
    // Policy-templated base for LRU/LFU eviction stores, striped over shards.
    //
    // Sharding: keys route to `shards_[hash(key) % N]` and every mutating op
    // holds only that shard's mutex, so puts/gets on disjoint keys proceed in
    // parallel. `N` is fixed at construction from capacity
    // (`shardCountFor`: one shard per 64 KiB, clamped to [1, 16]), so tiny
    // test stores stay single-shard and behave exactly like the legacy
    // single-mutex store, while production capacities stripe.
    //
    // Consequences of striping (documented, not accidental):
    //   - Eviction is per-shard LRU/LFU against a capacity slice
    //     (`capacity / N`), not global LRU. Hot single keys can evict early
    //     while other shards sit empty. Standard striped-cache tradeoff.
    //   - A single entry larger than its home shard's slice is rejected with
    //     CapacityExceeded even when it would fit the global capacity.
    //   - The Lamport version clock is a global atomic (fetch_add mint +
    //     CAS-max observe), so cross-shard monotonicity holds; same-key
    //     ordering additionally holds because a key always routes to one shard.
    //
    // Derived classes must provide (accessed via CRTP):
    //   struct Shard {
    //       mutable std::shared_mutex mutex;
    //       std::list<Node, SlabAllocator<Node>> list;
    //       IndexMap index; // dense map<string, ListIt>, no held iterators
    //                       // across map mutations (see putVersioned)
    //       TtlWheel<Node> wheel;
    //       size_t current_bytes = 0;
    //       steady_clock::time_point last_evict;
    //       // ... plus policy state (e.g. LFU freq buckets)
    //   };
    //   std::vector<Shard> shards_; // sized in the derived ctor init list
    //
    //   static auto nodeKey(const Node&) -> const std::string&
    //   static auto nodeEntry(Node&) -> VersionedEntry&
    //   static auto nodeEntry(const Node&) -> const VersionedEntry&
    //   static auto nodeSize(const Node&) -> size_t
    //
    // Policy hooks (all shard-local; caller holds sh.mutex exclusively
    // unless noted):
    //   void applyExisting(Shard& sh, ListIt it, VersionedEntry entry)
    //   ListIt insertNew(Shard& sh, const std::string& key, VersionedEntry entry)
    //   void onAccess(Shard& sh, ListIt it)
    //   void onEvictExpired(Shard& sh, ListIt it)
    //   size_t evictOne(Shard& sh) // evicts one entry, returns bytes freed
    //                              // (0 when the shard is already empty)
    //
    template <typename Derived, typename Node> class EvictionStoreBase : public CacheStore {
    public:

        using ListIt = std::list<Node, SlabAllocator<Node>>::iterator;

        explicit EvictionStoreBase(size_t capacity_bytes, Clock* clock = nullptr) :
            CacheStore(clock),
            capacity_bytes_(capacity_bytes),
            next_version_(static_cast<Version>(now().time_since_epoch().count())) {}

        auto put(
            const std::string& key,
            std::string value,
            std::optional<milliseconds> ttl = std::nullopt
        ) -> Result<void> final {
            Derived& self = d();
            VersionedEntry entry;

            entry.value = std::move(value);
            // Atomic fetch_add: uniqueness across threads without the shard
            // lock. Same-key ordering still holds (one shard per key); the
            // Lamport bump in putVersioned() advances past observed versions.
            entry.setVersion(self.mintVersion());
            if (ttl.has_value()) {
                entry.expires_at = now() + *ttl;
                entry.setHasTtl(true);
            }
            return putVersioned(key, std::move(entry));
        }

        auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> final {
            Derived& self = d();
            auto& sh = self.shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);

            auto it = sh.index.find(key);
            if (it != sh.index.end()) {
                {
                    // Idempotent apply: reject stale/equal-lower writes (replay-safe).
                    // Scoped: `it`/`it->second` must not survive evictIfNeeded() below.
                    // The dense index may relocate elements on ANY erase, invalidating
                    // all iterators/references — unlike std::unordered_map, which only
                    // invalidates the erased element. No map mutation happens inside
                    // this block (applyExisting touches list/freq structures only).
                    auto& entry0 = Derived::nodeEntry(*it->second);
                    if (entry.version() < entry0.version()) {
                        return ok();
                    }
                    if (entry.version()
                        == entry0.version()
                        && entry.writer_node_hash
                        < entry0.writer_node_hash) {
                        return ok();
                    }
                }

                // Lamport bump: advance past any observed version so
                // this node wins LWW if it later coordinates the same key.
                observeVersion(entry.version() + 1);
                const size_t old_size = Derived::nodeSize(*it->second);
                sh.current_bytes -= old_size;

                total_bytes_.fetch_sub(old_size, std::memory_order_relaxed);
                self.applyExisting(sh, it->second, std::move(entry));
                const size_t new_size = Derived::nodeSize(*it->second);

                sh.current_bytes += new_size;
                total_bytes_.fetch_add(new_size, std::memory_order_relaxed);
                evictIfNeeded(sh);
                // evictIfNeeded may have evicted this very node (LFU evicts the
                // min-frequency entry, which can be the just-written one when
                // it is uniquely minimal). Re-derive the iterator: the one above
                // is dead after the erasures regardless of whether our key
                // survived, so updateWheel must use the fresh one.
                it = sh.index.find(key);
                if (it == sh.index.end()) {
                    refreshMetrics();
                    return ok();
                }

                updateWheel(sh, it->second);
                writeWal(sh, key);
                if (metrics_) {
                    metrics_->shardMetrics().live.writes.fetchAdd(1, std::memory_order_relaxed);
                    refreshMetrics();
                }
                return ok();
            }

            size_t shard_cap = shardCapacity();
            size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
            if (entry_size > shard_cap) {
                return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
            }

            observeVersion(entry.version() + 1);
            auto new_it = self.insertNew(sh, key, std::move(entry));
            sh.current_bytes += entry_size;
            total_bytes_.fetch_add(entry_size, std::memory_order_relaxed);
            total_entries_.fetch_add(1, std::memory_order_relaxed);
            evictIfNeeded(sh);
            // Same self-eviction hazard as above: the new node starts at freq 1
            // (the minimum), so under capacity pressure evictIfNeeded can take
            // it right back. Re-lookup before touching the iterator.
            if (sh.index.find(key) == sh.index.end()) {
                refreshMetrics();
                return ok();
            }

            updateWheel(sh, new_it);
            writeWal(sh, key);
            if (metrics_) {
                metrics_->shardMetrics().live.writes.fetchAdd(1, std::memory_order_relaxed);
                refreshMetrics();
            }
            return ok();
        }

        auto get(const std::string& key) -> std::optional<std::string> final {
            Derived& self = d();
            auto& sh = self.shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);

            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                if (metrics_) {
                    metrics_->shardMetrics().live.misses.fetchAdd(1, std::memory_order_relaxed);
                }
                return std::nullopt;
            }

            auto& node = it->second;
            if (Derived::nodeEntry(*node).hasTtl()
                && Derived::nodeEntry(*node).expires_at
                <= now()) {
                eraseNode(sh, it);
                if (metrics_) {
                    metrics_->shardMetrics().cap.expires_on_read.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                    refreshMetrics();
                }
                return std::nullopt;
            }

            self.onAccess(sh, node);
            if (metrics_) {
                metrics_->shardMetrics().live.hits.fetchAdd(1, std::memory_order_relaxed);
            }
            return Derived::nodeEntry(*node).value;
        }

        auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> final {
            Derived& self = d();
            auto& sh = self.shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);

            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                if (metrics_) {
                    metrics_->shardMetrics().live.misses.fetchAdd(1, std::memory_order_relaxed);
                }
                return std::nullopt;
            }

            auto& node = it->second;
            if (Derived::nodeEntry(*node).hasTtl()
                && Derived::nodeEntry(*node).expires_at
                <= now()) {
                eraseNode(sh, it);
                if (metrics_) {
                    metrics_->shardMetrics().cap.expires_on_read.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                    refreshMetrics();
                }
                return std::nullopt;
            }
            if (metrics_) {
                metrics_->shardMetrics().live.hits.fetchAdd(1, std::memory_order_relaxed);
            }
            return Derived::nodeEntry(*node);
        }

        auto remove(const std::string& key) -> bool final {
            Derived& self = d();
            auto& sh = self.shards_[shardFor(key)];
            std::scoped_lock lock(sh.mutex);
            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                return false;
            }

            // WAL append before erase
            if (persistence_) {
                WalEntry we;
                we.op = WalEntry::Op::Del;
                we.key = key;
                we.version = Derived::nodeEntry(*it->second).version();
                we.writer_node_hash = Derived::nodeEntry(*it->second).writer_node_hash;
                we.has_ttl = false;
                we.expires_at_ms = 0;
                persistence_->onWrite(we);
            }

            eraseNode(sh, it);
            if (metrics_) {
                metrics_->shardMetrics().live.deletes.fetch_add(1, std::memory_order_relaxed);
                refreshMetrics();
            }
            return true;
        }

        [[nodiscard]]
        auto size() const -> size_t final {
            const Derived& self = d();
            size_t total = 0;
            for (const auto& sh : self.shards_) {
                std::shared_lock lock(sh.mutex);
                total += sh.index.size();
            }
            return total;
        }

        auto evictExpired() -> size_t final {
            Derived& self = d();
            auto current = now();

            size_t evicted = 0;
            for (auto& sh : self.shards_) {
                std::scoped_lock lock(sh.mutex);
                // Advance the wheel to catch up with elapsed wall time (always >= 1 tick so
                // a freshly-inserted sub-second TTL fires on the very next call).
                auto elapsed = current - sh.last_evict;
                auto elapsed_ticks = std::chrono::floor<std::chrono::seconds>(elapsed).count();
                size_t ticks = elapsed_ticks < 1 ? 1 : static_cast<size_t>(elapsed_ticks);

                sh.last_evict = current;
                for (size_t i = 0; i < ticks; ++i) {
                    sh.wheel.tick([&](Node& node) {
                        auto& entry = Derived::nodeEntry(node);
                        if (!entry.hasTtl() || entry.expires_at > current) {
                            // Fired early (wheel wrap, sub-second drift): re-schedule so the
                            // key is still reaped at its true expiry.
                            sh.wheel.insert(&node, expiryTicks(entry.expires_at));
                            return;
                        }

                        auto kit = sh.index.find(Derived::nodeKey(node));
                        if (kit == sh.index.end()) {
                            return; // already removed
                        }

                        eraseNode(sh, kit);
                        ++evicted;
                        if (metrics_) {
                            metrics_->shardMetrics().cap.evictions_ttl.fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                        }
                    });
                }
            }
            if (metrics_ && evicted > 0) {
                refreshMetrics();
            }
            return evicted;
        }

        auto mintVersion() -> Version final {
            return next_version_.fetch_add(1, std::memory_order_relaxed);
        }

        auto liveEntries() const -> std::vector<std::pair<std::string, VersionedEntry>> final {
            const Derived& self = d();
            std::vector<std::pair<std::string, VersionedEntry>> items;
            {
                auto snap_time = now();
                for (const auto& sh : self.shards_) {
                    std::shared_lock lock(sh.mutex);
                    for (const auto& node : sh.list) {
                        if (Derived::nodeEntry(node).hasTtl()
                            && Derived::nodeEntry(node).expires_at
                            <= snap_time) {
                            continue; // expired — skip
                        }
                        items.emplace_back(Derived::nodeKey(node), Derived::nodeEntry(node));
                    }
                }
            }
            return items;
        }

        void forEach(
            std::move_only_function<void(const std::string&, const VersionedEntry&)> fn
        ) const final {
            for (const auto& [key, entry] : liveEntries()) {
                fn(key, entry);
            }
        }

        void setPersistence(PersistenceManager* pm) final {
            persistence_ = pm;
        }

        void setMetrics(MetricsCollector* m) final {
            metrics_ = m;
            if (m) {
                m->shardMetrics().cap.capacity_bytes.store(
                    capacity_bytes_.load(std::memory_order_relaxed)
                );
            }
        }

        void setCapacity(size_t new_capacity) final {
            Derived& self = d();
            // Atomic: read by evictIfNeeded() under each shard mutex, so a
            // plain member would race a concurrent put(). Shard count stays
            // fixed (sized at construction); only the per-shard slice changes.
            capacity_bytes_.store(new_capacity, std::memory_order_relaxed);
            if (metrics_) {
                metrics_->shardMetrics().cap.capacity_bytes.store(new_capacity);
            }
            for (auto& sh : self.shards_) {
                std::scoped_lock lock(sh.mutex);
                evictIfNeeded(sh);
            }
            refreshMetrics();
        }

    protected:

        static constexpr size_t K_MAX_SHARDS = 16;
        // One shard per this many capacity bytes; tiny stores stay
        // single-shard (bit-identical eviction behavior), production
        // capacities stripe over up to K_MAX_SHARDS.
        static constexpr size_t K_MIN_SHARD_BYTES = 65'536;

        static auto shardCountFor(size_t capacity_bytes) -> size_t {
            size_t n = capacity_bytes / K_MIN_SHARD_BYTES;
            n = std::max<size_t>(n, 1);
            n = std::min(n, K_MAX_SHARDS);
            return n;
        }

        // Wheel slot for an absolute expiry: at least 1 tick ahead of the current
        // cursor, so sub-second TTLs are reaped on the very next evictExpired.
        [[nodiscard]]
        auto expiryTicks(steady_clock::time_point expires_at) const -> size_t {
            auto remaining = expires_at - now();
            if (remaining <= std::chrono::seconds(0)) {
                return 1;
            }

            auto ticks = std::chrono::ceil<std::chrono::seconds>(remaining).count();
            return static_cast<size_t>(ticks) < 1 ? 1 : static_cast<size_t>(ticks);
        }

        // Global capacity; per-shard slice is capacity / shard count.
        std::atomic<size_t> capacity_bytes_;
        // Global Lamport clock: fetch_add mint + CAS-max observe. Same-key
        // ordering additionally holds because a key routes to one shard.
        std::atomic<Version> next_version_;
        // Authoritative for metrics only; per-shard bytes drive eviction.
        std::atomic<size_t> total_bytes_{0};
        std::atomic<size_t> total_entries_{0};
        PersistenceManager* persistence_ = nullptr;
        MetricsCollector* metrics_ = nullptr;

        auto shardFor(std::string_view key) const -> size_t {
            const Derived& self = d();
            return std::hash<std::string_view>{}(key) % self.shards_.size();
        }

        [[nodiscard]]
        auto shardCapacity() const -> size_t {
            const Derived& self = d();
            return capacity_bytes_.load(std::memory_order_relaxed) / self.shards_.size();
        }

    private:

        template <typename Self>
        [[nodiscard]]
        auto d(this Self&& self) -> std::conditional_t<
            std::is_const_v<std::remove_reference_t<Self>>,
            const Derived&,
            Derived&
        > {
            using Ret = std::conditional_t<
                std::is_const_v<std::remove_reference_t<Self>>,
                const Derived&,
                Derived&
            >;
            return static_cast<Ret>(self);
        }

        // Advance the global Lamport clock past an observed version.
        void observeVersion(Version v) {
            Version cur = next_version_.load(std::memory_order_relaxed);
            while (v
                   > cur
                   && !next_version_.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
            }
        }

        // Erase index/list/wheel for an expired/removed node. Caller holds
        // sh.mutex exclusively. Updates shard bytes and global counters.
        template <typename S, typename IndexIt> void eraseNode(S& sh, IndexIt it) {
            Derived& self = d();
            auto list_it = it->second;
            size_t freed = Derived::nodeSize(*list_it);
            self.onEvictExpired(sh, list_it);
            sh.wheel.remove(&(*list_it));
            sh.list.erase(list_it);
            sh.index.erase(it);
            sh.current_bytes -= freed;
            total_bytes_.fetch_sub(freed, std::memory_order_relaxed);
            total_entries_.fetch_sub(1, std::memory_order_relaxed);
        }

        template <typename S, typename It> void updateWheel(S& sh, It it) {
            auto& entry = Derived::nodeEntry(*it);
            if (entry.hasTtl()) {
                sh.wheel.insert(&(*it), expiryTicks(entry.expires_at));
            } else {
                sh.wheel.remove(&(*it));
            }
        }

        template <typename S> void writeWal(S& sh, const std::string& key) {
            if (!persistence_) {
                return;
            }

            auto it = sh.index.find(key);
            if (it == sh.index.end()) {
                return;
            }

            auto& node = it->second;
            auto& entry = Derived::nodeEntry(*node);
            WalEntry we;

            we.op = WalEntry::Op::Set;
            we.key = key;
            we.value = entry.value;
            we.version = entry.version();
            we.writer_node_hash = entry.writer_node_hash;
            we.has_ttl = entry.hasTtl();
            if (entry.hasTtl()) {
                we.expires_at_ms = expiryToSystemMs(entry.expires_at);
            } else {
                we.expires_at_ms = 0;
            }
            // Thread-safe across shards: onWrite only takes the WAL queue
            // mutex (lock order shard > queue), never the persistence mutex.
            persistence_->onWrite(we);
        }

        // Caller holds sh.mutex exclusively. Evicts from this shard only;
        // freed == 0 (policy found nothing despite a non-empty list) breaks
        // the loop so a wedged policy cannot spin under the lock.
        template <typename S> void evictIfNeeded(S& sh) {
            Derived& self = d();
            size_t cap = shardCapacity();
            size_t evicted = 0;
            while (sh.current_bytes > cap && !sh.list.empty()) {
                size_t freed = self.evictOne(sh);
                if (freed == 0) {
                    break;
                }

                sh.current_bytes -= freed;
                total_bytes_.fetch_sub(freed, std::memory_order_relaxed);
                total_entries_.fetch_sub(1, std::memory_order_relaxed);
                ++evicted;
            }
            if (metrics_ && evicted > 0) {
                metrics_->shardMetrics().cap.evictions_capacity.fetch_add(
                    evicted,
                    std::memory_order_relaxed
                );
                refreshMetrics();
            }
        }

        // Publish the atomic globals to metrics. Lock-free (plain atomic
        // loads); per-op staleness is fine for observability.
        void refreshMetrics() {
            if (metrics_) {
                metrics_->shardMetrics().live.current_bytes.store(
                    total_bytes_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed
                );
                metrics_->shardMetrics().live.current_entries.store(
                    total_entries_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed
                );
            }
        }
    };
} // namespace cinder
