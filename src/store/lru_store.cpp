#include "cinder/store/lru_store.hpp"

#include <utility>

namespace cinder {

    LruStore::LruStore(size_t capacity_bytes, Clock* clock) :
        EvictionStoreBase(capacity_bytes, clock) {
        // Size the dense index once from capacity: at ~256 B average entry this
        // holds the steady-state working set without rehash churn during warmup.
        // Under-estimates grow geometrically (amortized); over-estimates waste
        // ~7 B per spare slot.
        index_.reserve(capacity_bytes / 256 + 64);
    }

    void LruStore::applyExisting(ListIt it, VersionedEntry entry) {
        it->entry = std::move(entry);
        touch(it);
    }

    auto LruStore::insertNew(const std::string& key, VersionedEntry entry) -> ListIt {
        LruNode n{};
        n.key = key;
        n.entry = std::move(entry);
        list_.push_front(std::move(n));
        index_[key] = list_.begin();
        return list_.begin();
    }

    void LruStore::onAccess(ListIt it) {
        touch(it);
    }

    void LruStore::onEvictExpired(ListIt /*it*/) {}

    void LruStore::evictOne() {
        auto& node = list_.back();
        current_bytes_ -= node.key.size() + node.entry.value.size() + sizeof(LruNode);
        wheel_.remove(&node);
        index_.erase(node.key);
        list_.pop_back();
    }

    void LruStore::touch(ListIt it) {
        list_.splice(list_.begin(), list_, it);
    }
} // namespace cinder
