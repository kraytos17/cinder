#include "cinder/store/lfu_store.hpp"

#include <algorithm>
#include <utility>

namespace cinder {

LfuStore::LfuStore(size_t capacity_bytes, Clock* clock)
    : EvictionStoreBase(capacity_bytes, clock) {}

void
LfuStore::applyExisting(ListIt it, VersionedEntry entry) {
    it->entry = std::move(entry);
    incrementFreq(it);
}

auto
LfuStore::insertNew(const std::string& key, VersionedEntry entry) -> ListIt {
    list_.push_front({.freq = 1, .key = key, .entry = std::move(entry)});
    index_[key] = list_.begin();
    freq_buckets_[1].push_back(list_.begin());
    min_freq_ = 1;
    return list_.begin();
}

void
LfuStore::onAccess(ListIt it) {
    incrementFreq(it);
}

void
LfuStore::onEvictExpired(ListIt it) {
    removeFromFreqBucket(it);
}

void
LfuStore::evictOne() {
    auto bucket_it = freq_buckets_.find(min_freq_);
    if (bucket_it == freq_buckets_.end() || bucket_it->second.empty()) {
        return;
    }

    auto node_it = bucket_it->second.front();
    bucket_it->second.erase(bucket_it->second.begin());
    if (bucket_it->second.empty()) {
        freq_buckets_.erase(bucket_it);
        min_freq_ = freq_buckets_.empty() ? 1 : freq_buckets_.begin()->first;
    }

    current_bytes_ -= node_it->key.size() + node_it->entry.value.size() + sizeof(LfuNode);
    wheel_.remove(node_it->key);
    index_.erase(node_it->key);
    list_.erase(node_it);
}

void
LfuStore::incrementFreq(ListIt it) {
    size_t old_freq = it->freq;
    size_t new_freq = old_freq + 1;
    it->freq = new_freq;

    removeFromFreqBucket(it, old_freq);
    freq_buckets_[new_freq].push_back(it);
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
        auto& vec = bucket_it->second;
        vec.erase(std::remove(vec.begin(), vec.end(), it), vec.end());
        if (vec.empty()) {
            freq_buckets_.erase(bucket_it);
            if (freq == min_freq_) {
                min_freq_ = freq_buckets_.empty() ? 1 : freq_buckets_.begin()->first;
            }
        }
    }
}
} // namespace cinder
