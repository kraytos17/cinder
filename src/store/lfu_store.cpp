#include "cinder/store/lfu_store.hpp"

#include <chrono>
#include <utility>

namespace cinder {

LfuStore::LfuStore(size_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {}

auto
LfuStore::put(const std::string& key, std::string value,
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
        increment_freq(node);
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

    lfu_list_.push_front({.key = key, .entry = std::move(entry), .freq = 1});
    index_[key] = lfu_list_.begin();
    freq_buckets_[1].push_front(lfu_list_.begin());
    min_freq_ = 1;
    current_bytes_ += entry_size;
    evict_if_needed();
    return ok();
}

auto
LfuStore::get(const std::string& key) -> std::optional<std::string> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= std::chrono::steady_clock::now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        remove_from_freq_bucket(node);
        lfu_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }

    increment_freq(node);
    return node->entry.value;
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
    remove_from_freq_bucket(node);
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
LfuStore::evict_expired() -> size_t {
    std::scoped_lock lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    size_t evicted = 0;
    for (auto it = lfu_list_.begin(); it != lfu_list_.end();) {
        if (it->entry.has_ttl && it->entry.expires_at <= now) {
            current_bytes_ -= it->key.size() + it->entry.value.size() + sizeof(Node);
            remove_from_freq_bucket(it);
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
LfuStore::increment_freq(ListIt it) {
    size_t old_freq = it->freq;
    size_t new_freq = old_freq + 1;
    it->freq = new_freq;

    remove_from_freq_bucket(it, old_freq);
    freq_buckets_[new_freq].push_front(it);
    if (old_freq == min_freq_ && freq_buckets_[old_freq].empty()) {
        ++min_freq_;
    }
}

void
LfuStore::remove_from_freq_bucket(ListIt it) {
    remove_from_freq_bucket(it, it->freq);
}

void
LfuStore::remove_from_freq_bucket(ListIt it, size_t freq) {
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
LfuStore::evict_if_needed() {
    while (current_bytes_ > capacity_bytes_ && !lfu_list_.empty()) {
        evict_one();
    }
}

void
LfuStore::evict_one() {
    auto bucket_it = freq_buckets_.find(min_freq_);
    if (bucket_it == freq_buckets_.end() || bucket_it->second.empty()) {
        return;
    }

    ListIt node_it = bucket_it->second.back();
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
