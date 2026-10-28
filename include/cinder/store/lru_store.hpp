#pragma once

#include <ankerl/unordered_dense.h>
#include <chrono>
#include <cstddef>
#include <list>
#include <shared_mutex>
#include <string>
#include <vector>

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

        // One stripe of the store. Base-class ops hold exactly one shard's
        // mutex; cross-shard aggregation (size/liveEntries/evictExpired)
        // visits shards in index order without nesting.
        struct Shard {
            mutable std::shared_mutex mutex;
            std::list<LruNode, SlabAllocator<LruNode>> list;
            IndexMap index;
            TtlWheel<LruNode> wheel;
            size_t current_bytes = 0;
            steady_clock::time_point last_evict = steady_clock::now();
        };

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

        void applyExisting(Shard& sh, ListIt it, VersionedEntry entry);
        auto insertNew(Shard& sh, const std::string& key, VersionedEntry entry) -> ListIt;
        void onAccess(Shard& sh, ListIt it);
        void onEvictExpired(Shard& sh, ListIt it);
        auto evictOne(Shard& sh) -> size_t;
        void touch(Shard& sh, ListIt it);

        // Sized from capacity (one shard per 64 KiB, max 16); single-shard
        // for small stores, striped for production capacities.
        std::vector<Shard> shards_;
    };
} // namespace cinder
