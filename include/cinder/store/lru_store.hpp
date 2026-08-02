#pragma once

#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

class LruStore : public CacheStore {
  public:

    explicit LruStore(size_t capacity_bytes, Clock* clock = nullptr);

    auto put(const std::string& key, std::string value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void> override;
    auto get(const std::string& key) -> std::optional<std::string> override;
    auto remove(const std::string& key) -> bool override;
    auto size() const -> size_t override;
    auto evictExpired() -> size_t override;
    auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> override;
    auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> override;
    auto mintVersion() -> Version override;

  private:

    struct Node {
        std::string key;
        VersionedEntry entry;
    };

    using ListIt = std::list<Node>::iterator;

    void touch(ListIt it);
    void evictIfNeeded();
    void evictOne();

    mutable std::mutex mutex_;
    std::list<Node> lru_list_;
    std::unordered_map<std::string, ListIt> index_;
    size_t capacity_bytes_;
    size_t current_bytes_ = 0;
    Version next_version_; // seeded from the clock in the ctor (restart-safe)
};
} // namespace cinder
