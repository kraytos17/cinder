#include "cinder/store/lru_store.hpp"

#include <chrono>
#include <utility>

namespace cinder {

LruStore::LruStore(size_t capacity_bytes, Clock* clock)
    : CacheStore(clock),
      capacity_bytes_(capacity_bytes) {}

auto
LruStore::put(const std::string& key, std::string value,
    std::optional<std::chrono::milliseconds> ttl) -> Result<void> {
    VersionedEntry entry;
    entry.value = std::move(value);
    entry.version = next_version_++;
    if (ttl.has_value()) {
        entry.expires_at = now() + *ttl;
        entry.has_ttl = true;
    }
    return putVersioned(key, std::move(entry));
}

auto
LruStore::putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it != index_.end()) {
        auto& node = it->second;
        // Idempotent apply: reject stale/equal-lower writes (replay-safe).
        if (entry.version < node->entry.version) {
            return ok();
        }
        if (entry.version == node->entry.version
            && entry.writer_node_hash < node->entry.writer_node_hash) {
            return ok();
        }

        current_bytes_ -= node->entry.value.size();
        node->entry = std::move(entry);
        current_bytes_ += node->entry.value.size();
        touch(node);
        evictIfNeeded();
        return ok();
    }

    size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
    if (entry_size > capacity_bytes_) {
        return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
    }

    lru_list_.push_front({.key = key, .entry = std::move(entry)});
    index_[key] = lru_list_.begin();
    current_bytes_ += entry_size;
    evictIfNeeded();
    return ok();
}

auto
LruStore::get(const std::string& key) -> std::optional<std::string> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        lru_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }

    touch(node);
    return node->entry.value;
}

auto
LruStore::getVersioned(const std::string& key) -> std::optional<VersionedEntry> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        lru_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }
    return node->entry;
}

auto
LruStore::remove(const std::string& key) -> bool {
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

auto
LruStore::size() const -> size_t {
    std::scoped_lock lock(mutex_);
    return index_.size();
}

auto
LruStore::evictExpired() -> size_t {
    std::scoped_lock lock(mutex_);

    auto current = now();
    size_t evicted = 0;
    for (auto it = lru_list_.begin(); it != lru_list_.end();) {
        if (it->entry.has_ttl && it->entry.expires_at <= current) {
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

void
LruStore::touch(ListIt it) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

void
LruStore::evictIfNeeded() {
    while (current_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
        evictOne();
    }
}

void
LruStore::evictOne() {
    auto& node = lru_list_.back();
    current_bytes_ -= node.key.size() + node.entry.value.size() + sizeof(Node);
    index_.erase(node.key);
    lru_list_.pop_back();
}
} // namespace cinder
