#include "cinder/store/lru_store.hpp"

#include <chrono>
#include <utility>

namespace cinder {

LruStore::LruStore(size_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {}

auto LruStore::put(const std::string& key, std::string value,
                   std::optional<std::chrono::milliseconds> ttl) -> Result<void> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it != index_.end()) {
        auto& node = it->second;
        current_bytes_ -= node->entry.value.size();
        node->entry.value = std::move(value);
        if (ttl.has_value()) {
            node->entry.expires_at = std::chrono::steady_clock::now() + *ttl;
            node->entry.has_ttl = true;
        } else {
            node->entry.has_ttl = false;
        }

        current_bytes_ += node->entry.value.size();
        touch(node);
        evict_if_needed();
        return ok();
    }

    CacheEntry entry;
    entry.value = std::move(value);
    if (ttl.has_value()) {
        entry.expires_at = std::chrono::steady_clock::now() + *ttl;
        entry.has_ttl = true;
    }

    size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
    if (entry_size > capacity_bytes_) {
        return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
    }

    lru_list_.push_front({.key=key, .entry=std::move(entry)});
    index_[key] = lru_list_.begin();
    current_bytes_ += entry_size;
    evict_if_needed();
    return ok();
}

auto LruStore::get(const std::string& key) -> std::optional<std::string> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl &&
        node->entry.expires_at <= std::chrono::steady_clock::now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        lru_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }

    touch(node);
    return node->entry.value;
}

auto LruStore::remove(const std::string& key) -> bool {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return false;
    }

    auto& node = it->second;
    current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
    lru_list_.erase(node);
    index_.erase(it);
    return true;
}

auto LruStore::size() const -> size_t {
    std::scoped_lock lock(mutex_);
    return index_.size();
}

auto LruStore::evict_expired() -> size_t {
    std::scoped_lock lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    size_t evicted = 0;
    for (auto it = lru_list_.begin(); it != lru_list_.end();) {
        if (it->entry.has_ttl && it->entry.expires_at <= now) {
            current_bytes_ -= it->key.size() + it->entry.value.size() + sizeof(Node);
            index_.erase(it->key);
            it = lru_list_.erase(it);
            ++evicted;
        } else {
            ++it;
        }
    }
    return evicted;
}

void LruStore::touch(ListIt it) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

void LruStore::evict_if_needed() {
    while (current_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
        evict_one();
    }
}

void LruStore::evict_one() {
    auto& node = lru_list_.back();
    current_bytes_ -= node.key.size() + node.entry.value.size() + sizeof(Node);
    index_.erase(node.key);
    lru_list_.pop_back();
}
} // namespace cinder
