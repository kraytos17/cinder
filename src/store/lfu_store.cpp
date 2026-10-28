#include "cinder/store/lfu_store.hpp"

#include <utility>

namespace cinder {

    LfuStore::LfuStore(size_t capacity_bytes, Clock* clock) :
        EvictionStoreBase(capacity_bytes, clock),
        shards_(shardCountFor(capacity_bytes)) {
        // Same sizing rationale as LruStore: one upfront reservation per
        // shard from its capacity slice avoids rehash churn during warmup.
        // freq_buckets stays unreserved.
        size_t per_shard = capacity_bytes / shards_.size();
        for (auto& sh : shards_) {
            sh.index.reserve(per_shard / 256 + 64);
            sh.last_evict = now();
        }
    }

    void LfuStore::applyExisting(Shard& sh, ListIt it, VersionedEntry entry) {
        it->entry = std::move(entry);
        incrementFreq(sh, it);
    }

    auto LfuStore::insertNew(Shard& sh, const std::string& key, VersionedEntry entry) -> ListIt {
        LfuNode n{};
        n.freq = 1;
        n.key = key;
        n.entry = std::move(entry);
        sh.list.push_front(std::move(n));
        sh.index[key] = sh.list.begin();
        sh.list.begin()->freq_index = sh.freq_buckets[1].size();
        sh.freq_buckets[1].push_back(sh.list.begin());
        sh.min_freq = 1;
        return sh.list.begin();
    }

    void LfuStore::onAccess(Shard& sh, ListIt it) {
        incrementFreq(sh, it);
    }

    void LfuStore::onEvictExpired(Shard& sh, ListIt it) {
        removeFromFreqBucket(sh, it);
    }

    auto LfuStore::evictOne(Shard& sh) -> size_t {
        // min_freq may lag if buckets drained without advancing it; resync
        // before evicting so we always make progress when the list is
        // non-empty. Without this, evictIfNeeded's while loop spins forever
        // holding the mutex when min_freq points at a missing bucket.
        advanceMinFreq(sh);
        auto bucket_it = sh.freq_buckets.find(sh.min_freq);
        if (bucket_it == sh.freq_buckets.end() || bucket_it->second.empty()) {
            return 0;
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
            sh.freq_buckets.erase(bucket_it);
            advanceMinFreq(sh);
        }

        size_t freed = node_it->key.size() + node_it->entry.value.size() + sizeof(LfuNode);
        sh.wheel.remove(&(*node_it));
        sh.index.erase(node_it->key);
        sh.list.erase(node_it);
        return freed;
    }

    void LfuStore::incrementFreq(Shard& sh, ListIt it) {
        size_t old_freq = it->freq;
        size_t new_freq = old_freq + 1;
        it->freq = new_freq;

        removeFromFreqBucket(sh, it, old_freq);
        it->freq_index = sh.freq_buckets[new_freq].size();
        sh.freq_buckets[new_freq].push_back(it);
        if (old_freq == sh.min_freq && !sh.freq_buckets.contains(old_freq)) {
            advanceMinFreq(sh);
        }
    }

    void LfuStore::removeFromFreqBucket(Shard& sh, ListIt it) {
        removeFromFreqBucket(sh, it, it->freq);
    }

    void LfuStore::removeFromFreqBucket(Shard& sh, ListIt it, size_t freq) {
        auto bucket_it = sh.freq_buckets.find(freq);
        if (bucket_it == sh.freq_buckets.end()) {
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
            sh.freq_buckets.erase(bucket_it);
            if (freq == sh.min_freq) {
                advanceMinFreq(sh);
            }
        }
    }

    void LfuStore::advanceMinFreq(Shard& sh) {
        // Frequencies only increase, so the minimum is non-decreasing; probe
        // upward past gaps left by drained buckets. freq_buckets is an
        // unordered map, so begin()->first is arbitrary and must NOT be used.
        while (!sh.freq_buckets.empty() && !sh.freq_buckets.contains(sh.min_freq)) {
            ++sh.min_freq;
        }
        if (sh.freq_buckets.empty()) {
            sh.min_freq = 1;
        }
    }
} // namespace cinder
