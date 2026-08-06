#include "cinder/store/lfu_store.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

namespace cinder {

LfuStore::LfuStore(size_t capacity_bytes, Clock* clock)
    : CacheStore(clock),
      capacity_bytes_(capacity_bytes),
      next_version_(static_cast<Version>(now().time_since_epoch().count())) {}

auto
LfuStore::put(const std::string& key, std::string value,
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
LfuStore::putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> {
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

        // Lamport bump: advance past any observed (incl. replicated) version so
        // this node wins LWW if it later coordinates the same key.
        next_version_ = std::max(next_version_, entry.version + 1);

        current_bytes_ -= node->entry.value.size();
        node->entry = std::move(entry);
        current_bytes_ += node->entry.value.size();
        incrementFreq(node);
        evictIfNeeded();
        return ok();
    }

    size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
    if (entry_size > capacity_bytes_) {
        return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
    }

    next_version_ = std::max(next_version_, entry.version + 1);

    lfu_list_.push_front({.key = key, .entry = std::move(entry), .freq = 1});
    index_[key] = lfu_list_.begin();
    freq_buckets_[1].push_front(lfu_list_.begin());
    min_freq_ = 1;
    current_bytes_ += entry_size;
    evictIfNeeded();
    return ok();
}

auto
LfuStore::mintVersion() -> Version {
    std::scoped_lock lock(mutex_);
    return next_version_++;
}

void
LfuStore::forEach(
    std::move_only_function<void(const std::string&, const VersionedEntry&)> fn) const {
    // Snapshot under the lock, then invoke the visitor outside it — the visitor
    // may safely call store mutators without self-deadlocking.
    std::vector<std::pair<std::string, VersionedEntry>> items;
    {
        std::scoped_lock lock(mutex_);
        items.reserve(lfu_list_.size());
        auto current = now();
        for (const auto& node : lfu_list_) {
            if (node.entry.has_ttl && node.entry.expires_at <= current) {
                continue; // expired — skip
            }
            items.emplace_back(node.key, node.entry);
        }
    }
    for (const auto& [key, entry] : items) {
        fn(key, entry);
    }
}

auto
LfuStore::get(const std::string& key) -> std::optional<std::string> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        removeFromFreqBucket(node);
        lfu_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }

    incrementFreq(node);
    return node->entry.value;
}

auto
LfuStore::getVersioned(const std::string& key) -> std::optional<VersionedEntry> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        removeFromFreqBucket(node);
        lfu_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }
    return node->entry;
}

auto
LfuStore::remove(const std::string& key) -> bool {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return false;
    }

    auto& node = it->second;
    current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
    removeFromFreqBucket(node);
    lfu_list_.erase(node);
    index_.erase(it);
    return true;
}

auto
LfuStore::size() const -> size_t {
    std::scoped_lock lock(mutex_);
    return index_.size();
}

auto
LfuStore::evictExpired() -> size_t {
    std::scoped_lock lock(mutex_);

    auto current = now();
    size_t evicted = 0;
    for (auto it = lfu_list_.begin(); it != lfu_list_.end();) {
        if (it->entry.has_ttl && it->entry.expires_at <= current) {
            current_bytes_ -= it->key.size() + it->entry.value.size() + sizeof(Node);
            removeFromFreqBucket(it);
            index_.erase(it->key);
            it = lfu_list_.erase(it);
            ++evicted;
        } else {
            ++it;
        }
    }
    return evicted;
}

void
LfuStore::incrementFreq(ListIt it) {
    size_t old_freq = it->freq;
    size_t new_freq = old_freq + 1;
    it->freq = new_freq;

    removeFromFreqBucket(it, old_freq);
    freq_buckets_[new_freq].push_front(it);
    if (old_freq == min_freq_ && freq_buckets_[old_freq].empty()) {
        ++min_freq_;
    }
}

void
LfuStore::removeFromFreqBucket(ListIt it) {
    removeFromFreqBucket(it, it->freq);
}

void
LfuStore::removeFromFreqBucket(ListIt it, size_t freq) {
    auto bucket_it = freq_buckets_.find(freq);
    if (bucket_it != freq_buckets_.end()) {
        bucket_it->second.remove(it);
        if (bucket_it->second.empty()) {
            freq_buckets_.erase(bucket_it);
            if (freq == min_freq_) {
                min_freq_ = freq_buckets_.empty() ? 1 : freq_buckets_.begin()->first;
            }
        }
    }
}

void
LfuStore::evictIfNeeded() {
    while (current_bytes_ > capacity_bytes_ && !lfu_list_.empty()) {
        evictOne();
    }
}

void
LfuStore::evictOne() {
    auto bucket_it = freq_buckets_.find(min_freq_);
    if (bucket_it == freq_buckets_.end() || bucket_it->second.empty()) {
        return;
    }

    auto node_it = bucket_it->second.back();
    bucket_it->second.pop_back();
    if (bucket_it->second.empty()) {
        freq_buckets_.erase(bucket_it);
        min_freq_ = freq_buckets_.empty() ? 1 : freq_buckets_.begin()->first;
    }

    current_bytes_ -= node_it->key.size() + node_it->entry.value.size() + sizeof(Node);
    index_.erase(node_it->key);
    lfu_list_.erase(node_it);
}
} // namespace cinder
