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

struct LruNode {
    std::string key;
    VersionedEntry entry;
};

class LruStore : public EvictionStoreBase<LruStore, LruNode> {
    friend class EvictionStoreBase<LruStore, LruNode>;

  public:

    using ListIt = std::list<LruNode, SlabAllocator<LruNode>>::iterator;

    explicit LruStore(size_t capacity_bytes, Clock* clock = nullptr);

  private:

    static auto nodeKey(const LruNode& n) -> const std::string& { return n.key; }

    static auto nodeEntry(LruNode& n) -> VersionedEntry& { return n.entry; }

    static auto nodeEntry(const LruNode& n) -> const VersionedEntry& { return n.entry; }

    static auto nodeSize(const LruNode& n) -> size_t {
        return n.key.size() + n.entry.value.size() + sizeof(LruNode);
    }

    void applyExisting(ListIt it, VersionedEntry entry);
    auto insertNew(const std::string& key, VersionedEntry entry) -> ListIt;
    void onAccess(ListIt it);
    void onEvictExpired(ListIt it);
    void evictOne();
    void touch(ListIt it);

    mutable std::shared_mutex mutex_;
    std::list<LruNode, SlabAllocator<LruNode>> list_;
    std::unordered_map<std::string, ListIt> index_;
    TtlWheel wheel_;
};
} // namespace cinder
