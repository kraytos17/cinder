#pragma once

#include "cinder/common/types.hpp"
#include "cinder/store/cache_store.hpp"

#include <chrono>
#include <cstddef>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace cinder {

class LruStore final : public CacheStore {
public:
    explicit LruStore(size_t capacity_bytes);

    auto put(const std::string& key, std::string value,
             std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void> override;
    auto get(const std::string& key) -> std::optional<std::string> override;
    auto remove(const std::string& key) -> bool override;
    auto size() const -> size_t override;
    auto evict_expired() -> size_t override;

private:
    struct Node {
        std::string key;
        CacheEntry entry;
    };
    using ListIt = std::list<Node>::iterator;

    void touch(ListIt it);
    void evict_if_needed();
    void evict_one();

    mutable std::mutex mutex_;
    std::list<Node> lru_list_;
    std::unordered_map<std::string, ListIt> index_;
    size_t capacity_bytes_;
    size_t current_bytes_ = 0;
};

} // namespace cinder
