#pragma once

#include <chrono>
#include <cstddef>
#include <list>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/slab_allocator.hpp"
#include "cinder/common/types.hpp"
#include "cinder/store/detail/eviction_store_base.hpp"
#include "cinder/store/ttl_wheel.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

struct LfuNode {
    size_t freq = 1;
    std::string key;
    VersionedEntry entry;
};

class LfuStore : public EvictionStoreBase<LfuStore, LfuNode> {
    friend class EvictionStoreBase<LfuStore, LfuNode>;

  public:

    using ListIt = std::list<LfuNode, SlabAllocator<LfuNode>>::iterator;

    explicit LfuStore(size_t capacity_bytes, Clock* clock = nullptr);

  private:

    static auto nodeKey(const LfuNode& n) -> const std::string& { return n.key; }

    static auto nodeEntry(LfuNode& n) -> VersionedEntry& { return n.entry; }

    static auto nodeEntry(const LfuNode& n) -> const VersionedEntry& { return n.entry; }

    static auto nodeSize(const LfuNode& n) -> size_t {
        return n.key.size() + n.entry.value.size() + sizeof(LfuNode);
    }

    void applyExisting(ListIt it, VersionedEntry entry);
    auto insertNew(const std::string& key, VersionedEntry entry) -> ListIt;
    void onAccess(ListIt it);
    void onEvictExpired(ListIt it);
    void evictOne();

    void incrementFreq(ListIt it);
    void removeFromFreqBucket(ListIt it);
    void removeFromFreqBucket(ListIt it, size_t freq);

    mutable std::shared_mutex mutex_;
    std::list<LfuNode, SlabAllocator<LfuNode>> list_;
    std::unordered_map<std::string, ListIt> index_;
    TtlWheel wheel_;
    std::unordered_map<size_t, std::vector<ListIt>> freq_buckets_;
    size_t min_freq_ = 1;
};
} // namespace cinder
