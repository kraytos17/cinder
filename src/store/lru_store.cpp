#include "cinder/store/lru_store.hpp"

#include <utility>

namespace cinder {

    LruStore::LruStore(size_t capacity_bytes, Clock* clock) :
        EvictionStoreBase(capacity_bytes, clock),
        shards_(shardCountFor(capacity_bytes)) {
        // Size each dense index once from its capacity slice: at ~256 B
        // average entry this holds the steady-state working set without
        // rehash churn during warmup. Under-estimates grow geometrically
        // (amortized); over-estimates waste ~7 B per spare slot.
        size_t per_shard = capacity_bytes / shards_.size();
        for (auto& sh : shards_) {
            sh.index.reserve(per_shard / 256 + 64);
            sh.last_evict = now();
        }
    }

    void LruStore::applyExisting(Shard& sh, ListIt it, VersionedEntry entry) {
        it->entry = std::move(entry);
        touch(sh, it);
    }

    auto LruStore::insertNew(Shard& sh, const std::string& key, VersionedEntry entry) -> ListIt {
        LruNode n{};
        n.key = key;
        n.entry = std::move(entry);
        sh.list.push_front(std::move(n));
        sh.index[key] = sh.list.begin();
        return sh.list.begin();
    }

    void LruStore::onAccess(Shard& sh, ListIt it) {
        touch(sh, it);
    }

    void LruStore::onEvictExpired(Shard& /*sh*/, ListIt /*it*/) {}

    auto LruStore::evictOne(Shard& sh) -> size_t {
        if (sh.list.empty()) {
            return 0;
        }
        auto& node = sh.list.back();
        size_t freed = node.key.size() + node.entry.value.size() + sizeof(LruNode);
        sh.wheel.remove(&node);
        sh.index.erase(node.key);
        sh.list.pop_back();
        return freed;
    }

    void LruStore::touch(Shard& sh, ListIt it) {
        sh.list.splice(sh.list.begin(), sh.list, it);
    }
} // namespace cinder
