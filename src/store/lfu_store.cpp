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
        return sh.list.begin();
    }

    void LfuStore::onAccess(Shard& sh, ListIt it) {
        incrementFreq(sh, it);
    }

    void LfuStore::onEvictExpired(Shard& sh, ListIt it) {
        removeFromFreqBucket(sh, it);
    }

    auto LfuStore::evictOne(Shard& sh) -> size_t {
        if (sh.freq_buckets.empty()) {
            return 0;
        }

        // Ordered frequency buckets make the eviction minimum structural.
        // NOTE (btree invalidation): any insert/erase invalidates ALL
        // iterators and mapped references into freq_buckets, so bucket_it
        // and vec below must not outlive this block; erase() is the last
        // map operation and nothing after it touches them.
        ListIt node_it;
        {
            auto bucket_it = sh.freq_buckets.begin();
            auto& vec = bucket_it->second;
            // Swap front to back for O(1) removal while preserving eviction order
            // (oldest entry at min_freq is at front, pushed earliest).
            node_it = vec.front();
            if (vec.size() > 1) {
                auto back_it = vec.back();
                back_it->freq_index = 0;
                vec[0] = back_it;
            }

            vec.pop_back();
            if (vec.empty()) {
                sh.freq_buckets.erase(bucket_it);
            }
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
    }

    void LfuStore::removeFromFreqBucket(Shard& sh, ListIt it) {
        removeFromFreqBucket(sh, it, it->freq);
    }

    void LfuStore::removeFromFreqBucket(Shard& sh, ListIt it, size_t freq) {
        // NOTE (btree invalidation): bucket_it and vec must not outlive any
        // freq_buckets mutation; erase() is the last map operation here and
        // nothing after it touches them.
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
        }
    }
} // namespace cinder
