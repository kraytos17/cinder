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

    struct LfuNode : WheelNode {
        size_t freq = 1;
        size_t freq_index = 0; // position within freq_buckets_[freq]
        std::string key;
        VersionedEntry entry;
    };

    static_assert(sizeof(LfuNode) == 136, "LfuNode should be 136 bytes");

    class LfuStore : public EvictionStoreBase<LfuStore, LfuNode> {
        friend class EvictionStoreBase<LfuStore, LfuNode>;

    public:

        using ListIt = std::list<LfuNode, SlabAllocator<LfuNode>>::iterator;

        // Dense open-addressing index — same invalidation contract as LruStore:
        // never hold it->second across evictIfNeeded()/insertNew; re-derive via
        // find() after any map mutation. Heterogeneous lookup preserved via
        // TransparentStringHash (is_transparent).
        using IndexMap = ankerl::unordered_dense::
            map<std::string, ListIt, TransparentStringHash, std::equal_to<>>;

        // One stripe of the store; frequency buckets are per-shard so all
        // LFU bookkeeping stays under the same shard mutex.
        struct Shard {
            mutable std::shared_mutex mutex;
            std::list<LfuNode, SlabAllocator<LfuNode>> list;
            IndexMap index;
            TtlWheel<LfuNode> wheel;
            size_t current_bytes = 0;
            steady_clock::time_point last_evict = steady_clock::now();
            // Frequency buckets: tiny (one entry per distinct frequency present).
            using FreqMap = ankerl::unordered_dense::map<size_t, std::vector<ListIt>>;
            FreqMap freq_buckets;
            size_t min_freq = 1;
        };

        // Frequency buckets: tiny (one entry per distinct frequency present), so
        // a dense map is fine here too; same no-held-iterators-across-mutation
        // discipline as index_.
        using FreqMap = ankerl::unordered_dense::map<size_t, std::vector<ListIt>>;

        explicit LfuStore(size_t capacity_bytes, Clock* clock = nullptr);

    private:

        static auto nodeKey(const LfuNode& n) -> const std::string& {
            return n.key;
        }

        static auto nodeEntry(LfuNode& n) -> VersionedEntry& {
            return n.entry;
        }

        static auto nodeEntry(const LfuNode& n) -> const VersionedEntry& {
            return n.entry;
        }

        static auto nodeSize(const LfuNode& n) -> size_t {
            return n.key.size() + n.entry.value.size() + sizeof(LfuNode);
        }

        void applyExisting(Shard& sh, ListIt it, VersionedEntry entry);
        auto insertNew(Shard& sh, const std::string& key, VersionedEntry entry) -> ListIt;
        void onAccess(Shard& sh, ListIt it);
        void onEvictExpired(Shard& sh, ListIt it);
        auto evictOne(Shard& sh) -> size_t;

        void incrementFreq(Shard& sh, ListIt it);
        void removeFromFreqBucket(Shard& sh, ListIt it);
        void removeFromFreqBucket(Shard& sh, ListIt it, size_t freq);
        void advanceMinFreq(Shard& sh);

        // Sized from capacity (one shard per 64 KiB, max 16).
        std::vector<Shard> shards_;
    };
} // namespace cinder
