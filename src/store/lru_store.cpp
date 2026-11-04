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

        // Tombstones are exempt from capacity eviction: evicting a live
        // delete marker would resurrect the key on the next anti-entropy
        // round with a replica that still holds the value. The TTL wheel
        // reaps tombstones at their GC deadline instead. Scan from the back
        // (LRU end), skipping tombstones wherever they sit.
        auto victim = sh.list.end();
        for (auto it = sh.list.rbegin(); it != sh.list.rend(); ++it) {
            if (!it->entry.isTombstone()) {
                victim = std::prev(it.base());
                break;
            }
        }
        if (victim == sh.list.end()) {
            return 0; // only tombstones left; GC handles them
        }

        auto& node = *victim;
        size_t freed = node.key.size() + node.entry.value.size() + sizeof(LruNode);
        noteRemoved(node.key, node.entry.version_and_ttl, node.entry.writer_node_hash);
        sh.wheel.remove(&node);
        sh.index.erase(node.key);
        sh.list.erase(victim);
        return freed;
    }

    void LruStore::touch(Shard& sh, ListIt it) {
        sh.list.splice(sh.list.begin(), sh.list, it);
    }
} // namespace cinder
