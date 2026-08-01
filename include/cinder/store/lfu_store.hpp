#pragma once

#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder {

class LfuStore : public CacheStore {
  public:

    explicit LfuStore(size_t capacity_bytes);

    auto put(const std::string& key, std::string value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void> override;
    auto get(const std::string& key) -> std::optional<std::string> override;
    auto remove(const std::string& key) -> bool override;
    auto size() const -> size_t override;
    auto evictExpired() -> size_t override;

  private:

    struct Node {
        std::string key;
        CacheEntry entry;
        size_t freq = 1;
    };

    using ListIt = std::list<Node>::iterator;

    void incrementFreq(ListIt it);
    void removeFromFreqBucket(ListIt it);
    void removeFromFreqBucket(ListIt it, size_t freq);
    void evictIfNeeded();
    void evictOne();

    mutable std::mutex mutex_;
    std::list<Node> lfu_list_;
    std::unordered_map<std::string, ListIt> index_;
    std::unordered_map<size_t, std::list<ListIt>> freq_buckets_;
    size_t min_freq_ = 1;
    size_t capacity_bytes_;
    size_t current_bytes_ = 0;
};
} // namespace cinder
