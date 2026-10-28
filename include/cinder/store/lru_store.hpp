#pragma once

#include <ankerl/unordered_dense.h>
#include <chrono>
#include <cstddef>
#include <list>
#include <shared_mutex>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/slab_allocator.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/store/detail/eviction_store_base.hpp"
#include "cinder/store/ttl_wheel.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

namespace cinder {

    struct LruNode : WheelNode {
        std::string key;
        VersionedEntry entry;
    };

    static_assert(sizeof(LruNode) == 120, "LruNode should be 120 bytes");

    class LruStore : public EvictionStoreBase<LruStore, LruNode> {
        friend class EvictionStoreBase<LruStore, LruNode>;

    public:

        using ListIt = std::list<LruNode, SlabAllocator<LruNode>>::iterator;

        explicit LruStore(size_t capacity_bytes, Clock* clock = nullptr);

    private:

        static auto nodeKey(const LruNode& n) -> const std::string& {
            return n.key;
        }

        static auto nodeEntry(LruNode& n) -> VersionedEntry& {
            return n.entry;
        }

        static auto nodeEntry(const LruNode& n) -> const VersionedEntry& {
            return n.entry;
        }

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
        // Dense open-addressing index (ankerl::unordered_dense): contiguous
        // storage, no per-node allocation, SIMD-filtered probes. Heterogeneous
        // lookup preserved via TransparentStringHash (is_transparent).
        //
        // Unlike std::unordered_map, iterators AND references are invalidated by
        // ANY insert (rehash) or erase (element relocation) — never hold
        // it->second across evictIfNeeded()/insertNew; always re-derive via
        // find() after any map mutation. See putVersioned.
        using IndexMap = ankerl::unordered_dense::
            map<std::string, ListIt, TransparentStringHash, std::equal_to<>>;

        IndexMap index_;
        TtlWheel<LruNode> wheel_;
    };
} // namespace cinder
