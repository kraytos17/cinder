#pragma once

#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/slab_allocator.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"
#include "cinder/store/persistence.hpp"
#include "cinder/store/ttl_wheel.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

class LruStore : public CacheStore {
  public:

    explicit LruStore(size_t capacity_bytes, Clock* clock = nullptr);

    auto put(const std::string& key, std::string value,
        std::optional<milliseconds> ttl = std::nullopt) -> Result<void> override;
    auto get(const std::string& key) -> std::optional<std::string> override;
    auto remove(const std::string& key) -> bool override;
    auto size() const -> size_t override;
    auto evictExpired() -> size_t override;
    auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> override;
    auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> override;
    auto mintVersion() -> Version override;
    void forEach(
        std::move_only_function<void(const std::string&, const VersionedEntry&)> /*unused*/)
        const override;

    void setPersistence(PersistenceManager* pm) override { persistence_ = pm; }

  private:

    struct Node {
        std::string key;
        VersionedEntry entry;
    };

    using ListIt = std::list<Node, SlabAllocator<Node>>::iterator;

    void touch(ListIt it);
    void evictIfNeeded();
    void evictOne();

    // Wheel slot for an absolute expiry: at least 1 tick ahead of the current
    // cursor, so sub-second TTLs are reaped on the very next evictExpired.
    auto expiryTicks(steady_clock::time_point expires_at) const -> size_t;

    mutable std::mutex mutex_;
    std::list<Node, SlabAllocator<Node>> lru_list_;
    std::unordered_map<std::string, ListIt> index_;
    TtlWheel wheel_;
    steady_clock::time_point last_evict_;
    size_t capacity_bytes_;
    size_t current_bytes_ = 0;
    Version next_version_; // seeded from the clock in the ctor (restart-safe)
    PersistenceManager* persistence_ = nullptr;
};
} // namespace cinder
