#include "cinder/store/lru_store.hpp"

#include <utility>

namespace cinder {

LruStore::LruStore(size_t capacity_bytes, Clock* clock)
    : EvictionStoreBase(capacity_bytes, clock) {}

void
LruStore::applyExisting(ListIt it, VersionedEntry entry) {
    it->entry = std::move(entry);
    touch(it);
}

auto
LruStore::insertNew(const std::string& key, VersionedEntry entry) -> ListIt {
    list_.push_front({.key = key, .entry = std::move(entry)});
    index_[key] = list_.begin();
    return list_.begin();
}

void
LruStore::onAccess(ListIt it) {
    touch(it);
}

void
LruStore::onEvictExpired(ListIt /*it*/) {
    // no frequency bookkeeping — nothing to clean up on expiry
}

void
LruStore::evictOne() {
    auto& node = list_.back();
    current_bytes_ -= node.key.size() + node.entry.value.size() + sizeof(LruNode);
    wheel_.remove(node.key);
    index_.erase(node.key);
    list_.pop_back();
}

void
LruStore::touch(ListIt it) {
    list_.splice(list_.begin(), list_, it);
}
} // namespace cinder
