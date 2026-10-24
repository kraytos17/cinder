#include "cinder/store/lfu_store.hpp"

#include <utility>

namespace cinder {

    LfuStore::LfuStore(size_t capacity_bytes, Clock* clock) :
        EvictionStoreBase(capacity_bytes, clock) {}

    void LfuStore::applyExisting(ListIt it, VersionedEntry entry) {
        it->entry = std::move(entry);
        incrementFreq(it);
    }

    auto LfuStore::insertNew(const std::string& key, VersionedEntry entry) -> ListIt {
        LfuNode n{};
        n.freq = 1;
        n.key = key;
        n.entry = std::move(entry);
        list_.push_front(std::move(n));
        index_[key] = list_.begin();
        list_.begin()->freq_index = freq_buckets_[1].size();
        freq_buckets_[1].push_back(list_.begin());
        min_freq_ = 1;
        return list_.begin();
    }

    void LfuStore::onAccess(ListIt it) {
        incrementFreq(it);
    }

    void LfuStore::onEvictExpired(ListIt it) {
        removeFromFreqBucket(it);
    }

    void LfuStore::evictOne() {
        // min_freq_ may lag if buckets drained without advancing it; resync
        // before evicting so we always make progress when the list is
        // non-empty. Without this, evictIfNeeded's while loop spins forever
        // holding the mutex when min_freq_ points at a missing bucket.
        advanceMinFreq();
        auto bucket_it = freq_buckets_.find(min_freq_);
        if (bucket_it == freq_buckets_.end() || bucket_it->second.empty()) {
            return;
        }

        auto& vec = bucket_it->second;
        // Swap front to back for O(1) removal while preserving eviction order
        // (oldest entry at min_freq is at front, pushed earliest).
        auto node_it = vec.front();
        if (vec.size() > 1) {
            auto back_it = vec.back();
            back_it->freq_index = 0;
            vec[0] = back_it;
        }

        vec.pop_back();
        if (vec.empty()) {
            freq_buckets_.erase(bucket_it);
            advanceMinFreq();
        }

        current_bytes_ -= node_it->key.size() + node_it->entry.value.size() + sizeof(LfuNode);
        wheel_.remove(&(*node_it));
        index_.erase(node_it->key);
        list_.erase(node_it);
    }

    void LfuStore::incrementFreq(ListIt it) {
        size_t old_freq = it->freq;
        size_t new_freq = old_freq + 1;
        it->freq = new_freq;

        removeFromFreqBucket(it, old_freq);
        it->freq_index = freq_buckets_[new_freq].size();
        freq_buckets_[new_freq].push_back(it);
        if (old_freq == min_freq_ && !freq_buckets_.contains(old_freq)) {
            advanceMinFreq();
        }
    }

    void LfuStore::removeFromFreqBucket(ListIt it) {
        removeFromFreqBucket(it, it->freq);
    }

    void LfuStore::removeFromFreqBucket(ListIt it, size_t freq) {
        auto bucket_it = freq_buckets_.find(freq);
        if (bucket_it == freq_buckets_.end()) {
            return;
        }

        auto& vec = bucket_it->second;
        auto idx = it->freq_index;
        if (idx != vec.size() - 1) {
            auto last = vec.back();
            last->freq_index = idx;
            vec[idx] = last;
        }

        vec.pop_back();
        if (vec.empty()) {
            freq_buckets_.erase(bucket_it);
            if (freq == min_freq_) {
                advanceMinFreq();
            }
        }
    }

    void LfuStore::advanceMinFreq() {
        // Frequencies only increase, so the minimum is non-decreasing; probe
        // upward past gaps left by drained buckets. freq_buckets_ is an
        // unordered_map, so begin()->first is arbitrary and must NOT be used.
        while (!freq_buckets_.empty() && !freq_buckets_.contains(min_freq_)) {
            ++min_freq_;
        }
        if (freq_buckets_.empty()) {
            min_freq_ = 1;
        }
    }
} // namespace cinder
