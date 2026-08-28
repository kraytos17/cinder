#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <functional>
#include <generator>
#include <list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/slab_allocator.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/persistence.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {
// Policy-templated base for LRU/LFU eviction stores.
//
// Derived classes must provide as members (accessed via CRTP):
//   struct Node { ... };
//   using ListIt = std::list<Node, SlabAllocator<Node>>::iterator;
//   mutable std::shared_mutex mutex_;
//   std::list<Node, SlabAllocator<Node>> list_;
//   std::unordered_map<std::string, ListIt> index_;
//   TtlWheel wheel_;
//
//   static auto nodeKey(const Node&) -> const std::string&
//   static auto nodeEntry(Node&) -> VersionedEntry&
//   static auto nodeEntry(const Node&) -> const VersionedEntry&
//   static auto nodeSize(const Node&) -> size_t
//
// Policy hooks:
//   void applyExisting(ListIt it, VersionedEntry entry)
//   ListIt insertNew(const std::string& key, VersionedEntry entry)
//   void onAccess(ListIt it)
//   void onEvictExpired(ListIt it)
//   void evictOne()
//
template <typename Derived, typename Node> class EvictionStoreBase : public CacheStore {
  public:

    using ListIt = std::list<Node, SlabAllocator<Node>>::iterator;

    explicit EvictionStoreBase(size_t capacity_bytes, Clock* clock = nullptr)
        : CacheStore(clock),
          capacity_bytes_(capacity_bytes),
          last_evict_(now()),
          next_version_(static_cast<Version>(now().time_since_epoch().count())) {}

    auto put(const std::string& key, std::string value,
        std::optional<milliseconds> ttl = std::nullopt) -> Result<void> final {
        Derived& self = d();
        VersionedEntry entry;

        entry.value = std::move(value);
        // Mint under the lock — an unlocked ++ here races the Lamport bump in
        // putVersioned() and can hand duplicate versions to concurrent writers.
        entry.version = self.mintVersion();
        if (ttl.has_value()) {
            entry.expires_at = now() + *ttl;
            entry.has_ttl = true;
        }
        return putVersioned(key, std::move(entry));
    }

    auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);

        auto it = self.index_.find(key);
        if (it != self.index_.end()) {
            auto& node = it->second;
            // Idempotent apply: reject stale/equal-lower writes (replay-safe).
            if (entry.version < Derived::nodeEntry(*node).version) {
                return ok();
            }
            if (entry.version == Derived::nodeEntry(*node).version
                && entry.writer_node_hash < Derived::nodeEntry(*node).writer_node_hash) {
                return ok();
            }

            // Lamport bump: advance past any observed version so
            // this node wins LWW if it later coordinates the same key.
            next_version_ = std::max(next_version_, entry.version + 1);
            current_bytes_ -= Derived::nodeSize(*node);
            self.applyExisting(node, std::move(entry));
            current_bytes_ += Derived::nodeSize(*node);
            evictIfNeeded();
            updateWheel(node);
            writeWal(key);
            if (metrics_) {
                metrics_->shardMetrics().writes.fetch_add(1, std::memory_order_relaxed);
                metrics_->shardMetrics().current_bytes.store(
                    current_bytes_, std::memory_order_relaxed);
                metrics_->shardMetrics().current_entries.store(
                    self.index_.size(), std::memory_order_relaxed);
            }
            return ok();
        }

        size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
        if (entry_size > capacity_bytes_) {
            return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
        }

        next_version_ = std::max(next_version_, entry.version + 1);
        auto new_it = self.insertNew(key, std::move(entry));
        current_bytes_ += entry_size;
        evictIfNeeded();
        updateWheel(new_it);
        writeWal(key);
        if (metrics_) {
            metrics_->shardMetrics().writes.fetch_add(1, std::memory_order_relaxed);
            metrics_->shardMetrics().current_bytes.store(current_bytes_, std::memory_order_relaxed);
            metrics_->shardMetrics().current_entries.store(
                self.index_.size(), std::memory_order_relaxed);
        }
        return ok();
    }

    auto get(const std::string& key) -> std::optional<std::string> final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);

        auto it = self.index_.find(key);
        if (it == self.index_.end()) {
            if (metrics_) {
                metrics_->shardMetrics().misses.fetch_add(1, std::memory_order_relaxed);
            }
            return std::nullopt;
        }

        auto& node = it->second;
        if (Derived::nodeEntry(*node).has_ttl && Derived::nodeEntry(*node).expires_at <= now()) {
            current_bytes_ -= Derived::nodeSize(*node);
            self.onEvictExpired(node);
            self.wheel_.remove(Derived::nodeKey(*node));
            self.list_.erase(node);
            self.index_.erase(it);
            if (metrics_) {
                metrics_->shardMetrics().expires_on_read.fetch_add(1, std::memory_order_relaxed);
            }
            return std::nullopt;
        }

        self.onAccess(node);
        if (metrics_) {
            metrics_->shardMetrics().hits.fetch_add(1, std::memory_order_relaxed);
        }
        return Derived::nodeEntry(*node).value;
    }

    auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);

        auto it = self.index_.find(key);
        if (it == self.index_.end()) {
            if (metrics_) {
                metrics_->shardMetrics().misses.fetch_add(1, std::memory_order_relaxed);
            }
            return std::nullopt;
        }

        auto& node = it->second;
        if (Derived::nodeEntry(*node).has_ttl && Derived::nodeEntry(*node).expires_at <= now()) {
            current_bytes_ -= Derived::nodeSize(*node);
            self.onEvictExpired(node);
            self.wheel_.remove(Derived::nodeKey(*node));
            self.list_.erase(node);
            self.index_.erase(it);
            if (metrics_) {
                metrics_->shardMetrics().expires_on_read.fetch_add(1, std::memory_order_relaxed);
            }
            return std::nullopt;
        }
        if (metrics_) {
            metrics_->shardMetrics().hits.fetch_add(1, std::memory_order_relaxed);
        }
        return Derived::nodeEntry(*node);
    }

    auto remove(const std::string& key) -> bool final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);
        auto it = self.index_.find(key);
        if (it == self.index_.end()) {
            return false;
        }

        auto& node = it->second;
        // WAL append before erase
        if (persistence_) {
            WalEntry we;
            we.op = WalEntry::Op::Del;
            we.key = key;
            we.version = Derived::nodeEntry(*node).version;
            we.writer_node_hash = Derived::nodeEntry(*node).writer_node_hash;
            we.has_ttl = false;
            we.expires_at_ms = 0;
            persistence_->onWrite(we);
        }

        current_bytes_ -= Derived::nodeSize(*node);
        self.onEvictExpired(node);
        self.wheel_.remove(Derived::nodeKey(*node));
        self.list_.erase(node);
        self.index_.erase(it);
        if (metrics_) {
            metrics_->shardMetrics().deletes.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    }

    [[nodiscard]] auto size() const -> size_t final {
        const Derived& self = d();
        std::shared_lock lock(self.mutex_);
        return self.index_.size();
    }

    auto evictExpired() -> size_t final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);

        auto current = now();
        // Advance the wheel to catch up with elapsed wall time (always >= 1 tick so
        // a freshly-inserted sub-second TTL fires on the very next call).
        auto elapsed = current - last_evict_;
        auto elapsed_ticks = std::chrono::floor<std::chrono::seconds>(elapsed).count();
        size_t ticks = elapsed_ticks < 1 ? 1 : static_cast<size_t>(elapsed_ticks);

        last_evict_ = current;
        size_t evicted = 0;
        for (size_t i = 0; i < ticks; ++i) {
            self.wheel_.tick([&](const std::string& key) {
                auto kit = self.index_.find(key);
                if (kit == self.index_.end()) {
                    return; // already removed
                }

                auto& node = kit->second;
                if (Derived::nodeEntry(*node).has_ttl
                    && Derived::nodeEntry(*node).expires_at <= current) {
                    current_bytes_ -= Derived::nodeSize(*node);
                    auto list_it = node;
                    self.onEvictExpired(list_it);
                    self.index_.erase(kit);
                    self.list_.erase(list_it);
                    ++evicted;
                    if (metrics_) {
                        metrics_->shardMetrics().evictions_ttl.fetch_add(
                            1, std::memory_order_relaxed);
                    }
                } else {
                    // Fired early (wheel wrap, sub-second drift): re-schedule so the
                    // key is still reaped at its true expiry.
                    self.wheel_.insert(
                        Derived::nodeKey(*node), expiryTicks(Derived::nodeEntry(*node).expires_at));
                }
            });
        }
        if (metrics_ && evicted > 0) {
            metrics_->shardMetrics().current_bytes.store(current_bytes_, std::memory_order_relaxed);
            metrics_->shardMetrics().current_entries.store(
                self.index_.size(), std::memory_order_relaxed);
        }
        return evicted;
    }

    auto mintVersion() -> Version final {
        Derived& self = d();
        std::scoped_lock lock(self.mutex_);
        return next_version_++;
    }

    auto liveEntries() const
        -> std::generator<std::pair<const std::string&, const VersionedEntry&>> override {
        const Derived& self = d();
        std::vector<std::pair<std::string, VersionedEntry>> items;
        {
            std::shared_lock lock(self.mutex_);
            items.reserve(self.list_.size());
            auto snap_time = now();
            for (const auto& node : self.list_) {
                if (Derived::nodeEntry(node).has_ttl
                    && Derived::nodeEntry(node).expires_at <= snap_time) {
                    continue; // expired — skip
                }
                items.emplace_back(Derived::nodeKey(node), Derived::nodeEntry(node));
            }
        }

        // Yield from snapshot — lock is released.
        for (const auto& [key, entry] : items) {
            co_yield std::pair<const std::string&, const VersionedEntry&>{key, entry};
        }
    }

    void forEach(
        std::move_only_function<void(const std::string&, const VersionedEntry&)> fn) const final {
        const Derived& self = d();
        std::vector<std::pair<std::string, VersionedEntry>> items;
        {
            std::shared_lock lock(self.mutex_);
            items.reserve(self.list_.size());
            auto snap_time = now();
            for (const auto& node : self.list_) {
                if (Derived::nodeEntry(node).has_ttl
                    && Derived::nodeEntry(node).expires_at <= snap_time) {
                    continue; // expired — skip
                }
                items.emplace_back(Derived::nodeKey(node), Derived::nodeEntry(node));
            }
        }

        for (const auto& [key, entry] : items) {
            fn(key, entry);
        }
    }

    void setPersistence(PersistenceManager* pm) final { persistence_ = pm; }

    void setMetrics(MetricsCollector* m) final {
        metrics_ = m;
        if (m) {
            m->shardMetrics().capacity_bytes.store(capacity_bytes_);
        }
    }

  protected:

    // Wheel slot for an absolute expiry: at least 1 tick ahead of the current
    // cursor, so sub-second TTLs are reaped on the very next evictExpired.
    [[nodiscard]] auto expiryTicks(steady_clock::time_point expires_at) const -> size_t {
        auto remaining = expires_at - now();
        if (remaining <= std::chrono::seconds(0)) {
            return 1;
        }

        auto ticks = std::chrono::ceil<std::chrono::seconds>(remaining).count();
        return static_cast<size_t>(ticks) < 1 ? 1 : static_cast<size_t>(ticks);
    }

    size_t capacity_bytes_;
    size_t current_bytes_ = 0;
    steady_clock::time_point last_evict_;
    Version next_version_;
    PersistenceManager* persistence_ = nullptr;
    MetricsCollector* metrics_ = nullptr;

  private:

    [[nodiscard]] auto d() const -> const Derived& { return *static_cast<const Derived*>(this); }

    [[nodiscard]] auto d() -> Derived& { return *static_cast<Derived*>(this); }

    void updateWheel(ListIt it) {
        Derived& self = d();
        auto& entry = Derived::nodeEntry(*it);
        if (entry.has_ttl) {
            self.wheel_.insert(Derived::nodeKey(*it), expiryTicks(entry.expires_at));
        } else {
            self.wheel_.remove(Derived::nodeKey(*it));
        }
    }

    void writeWal(const std::string& key) {
        if (!persistence_) {
            return;
        }

        Derived& self = d();
        auto it = self.index_.find(key);
        if (it == self.index_.end()) {
            return;
        }

        auto& node = it->second;
        auto& entry = Derived::nodeEntry(*node);
        WalEntry we;

        we.op = WalEntry::Op::Set;
        we.key = key;
        we.value = entry.value;
        we.version = entry.version;
        we.writer_node_hash = entry.writer_node_hash;
        we.has_ttl = entry.has_ttl;
        if (entry.has_ttl) {
            we.expires_at_ms = expiryToSystemMs(entry.expires_at);
        } else {
            we.expires_at_ms = 0;
        }
        persistence_->onWrite(we);
    }

    void evictIfNeeded() {
        Derived& self = d();
        auto entries_before = self.index_.size();
        while (current_bytes_ > capacity_bytes_ && !self.list_.empty()) {
            Logger::trace(
                "cinder store: evicting to fit bytes={}/{}", current_bytes_, capacity_bytes_);
            self.evictOne();
        }
        if (metrics_) {
            auto entries_after = self.index_.size();
            if (entries_after < entries_before) {
                metrics_->shardMetrics().evictions_capacity.fetch_add(
                    entries_before - entries_after, std::memory_order_relaxed);
            }

            metrics_->shardMetrics().current_bytes.store(current_bytes_, std::memory_order_relaxed);
            metrics_->shardMetrics().current_entries.store(
                entries_after, std::memory_order_relaxed);
        }
    }
};
} // namespace cinder
