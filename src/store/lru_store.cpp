#include "cinder/store/lru_store.hpp"

#include <algorithm>
#include <chrono>
#include <utility>

using std::chrono::floor;
using std::chrono::milliseconds;
using std::chrono::seconds;

namespace cinder {

LruStore::LruStore(size_t capacity_bytes, Clock* clock)
    : CacheStore(clock),
      last_evict_(now()),
      capacity_bytes_(capacity_bytes),
      next_version_(static_cast<Version>(now().time_since_epoch().count())) {}

auto
LruStore::expiryTicks(steady_clock::time_point expires_at) const -> size_t {
    auto remaining = expires_at - now();
    if (remaining <= seconds(0)) {
        return 1;
    }
    auto ticks = std::chrono::ceil<seconds>(remaining).count();
    return static_cast<size_t>(ticks) < 1 ? 1 : static_cast<size_t>(ticks);
}

auto
LruStore::put(const std::string& key, std::string value, std::optional<milliseconds> ttl)
    -> Result<void> {
    VersionedEntry entry;
    entry.value = std::move(value);
    entry.version = next_version_++;
    if (ttl.has_value()) {
        entry.expires_at = now() + *ttl;
        entry.has_ttl = true;
    }
    return putVersioned(key, std::move(entry));
}

auto
LruStore::putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> {
    std::scoped_lock lock(mutex_);

    auto it = index_.find(key);
    if (it != index_.end()) {
        auto& node = it->second;
        // Idempotent apply: reject stale/equal-lower writes (replay-safe).
        if (entry.version < node->entry.version) {
            return ok();
        }
        if (entry.version == node->entry.version
            && entry.writer_node_hash < node->entry.writer_node_hash) {
            return ok();
        }

        // Lamport bump: advance past any observed (incl. replicated) version so
        // this node wins LWW if it later coordinates the same key.
        next_version_ = std::max(next_version_, entry.version + 1);
        current_bytes_ -= node->entry.value.size();
        node->entry = std::move(entry);
        current_bytes_ += node->entry.value.size();
        touch(node);
        evictIfNeeded();
        if (node->entry.has_ttl) {
            wheel_.insert(node->key, expiryTicks(node->entry.expires_at));
        } else {
            wheel_.remove(node->key);
        }

        // WAL append
        if (persistence_) {
            WalEntry we;
            we.op = WalEntry::Op::Set;
            we.key = node->key;
            we.value = node->entry.value;
            we.version = node->entry.version;
            we.writer_node_hash = node->entry.writer_node_hash;
            we.has_ttl = node->entry.has_ttl;
            if (node->entry.has_ttl) {
                RealClock real_clock;
                we.expires_at_ms = toSystemMs(real_clock, node->entry.expires_at);
            } else {
                we.expires_at_ms = 0;
            }
            persistence_->onWrite(we);
        }

        return ok();
    }

    size_t entry_size = key.size() + entry.value.size() + sizeof(Node);
    if (entry_size > capacity_bytes_) {
        return err(Error(Errc::CapacityExceeded, "value exceeds capacity"));
    }

    next_version_ = std::max(next_version_, entry.version + 1);
    bool has_ttl = entry.has_ttl;
    auto expires_at = entry.expires_at;
    auto stored_version = entry.version;
    auto stored_writer_hash = entry.writer_node_hash;
    auto stored_value = entry.value;

    lru_list_.push_front({.key = key, .entry = std::move(entry)});
    index_[key] = lru_list_.begin();
    current_bytes_ += entry_size;
    evictIfNeeded();
    if (has_ttl) {
        wheel_.insert(key, expiryTicks(expires_at));
    } else {
        wheel_.remove(key);
    }

    // WAL append
    if (persistence_) {
        WalEntry we;
        we.op = WalEntry::Op::Set;
        we.key = key;
        we.value = std::move(stored_value);
        we.version = stored_version;
        we.writer_node_hash = stored_writer_hash;
        we.has_ttl = has_ttl;
        if (has_ttl) {
            RealClock real_clock;
            we.expires_at_ms = toSystemMs(real_clock, expires_at);
        } else {
            we.expires_at_ms = 0;
        }
        persistence_->onWrite(we);
    }
    return ok();
}

auto
LruStore::mintVersion() -> Version {
    std::scoped_lock lock(mutex_);
    return next_version_++;
}

void
LruStore::forEach(
    std::move_only_function<void(const std::string&, const VersionedEntry&)> fn) const {
    // Snapshot under the lock, then invoke the visitor outside it — the visitor
    // may safely call store mutators without self-deadlocking.
    std::vector<std::pair<std::string, VersionedEntry>> items;
    {
        std::shared_lock lock(mutex_);
        items.reserve(lru_list_.size());
        auto current = now();
        for (const auto& node : lru_list_) {
            if (node.entry.has_ttl && node.entry.expires_at <= current) {
                continue; // expired — skip
            }
            items.emplace_back(node.key, node.entry);
        }
    }
    for (const auto& [key, entry] : items) {
        fn(key, entry);
    }
}

auto
LruStore::get(const std::string& key) -> std::optional<std::string> {
    std::shared_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        wheel_.remove(key);
        lru_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }

    touch(node);
    return node->entry.value;
}

auto
LruStore::getVersioned(const std::string& key) -> std::optional<VersionedEntry> {
    std::shared_lock lock(mutex_);

    auto it = index_.find(key);
    if (it == index_.end()) {
        return std::nullopt;
    }

    auto& node = it->second;
    if (node->entry.has_ttl && node->entry.expires_at <= now()) {
        current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
        wheel_.remove(key);
        lru_list_.erase(node);
        index_.erase(it);
        return std::nullopt;
    }
    return node->entry;
}

auto
LruStore::remove(const std::string& key) -> bool {
    std::scoped_lock lock(mutex_);
    auto it = index_.find(key);
    if (it == index_.end()) {
        return false;
    }

    auto& node = it->second;
    // WAL append before erase
    if (persistence_) {
        WalEntry we;
        we.op = WalEntry::Op::Del;
        we.key = key;
        we.version = node->entry.version;
        we.writer_node_hash = node->entry.writer_node_hash;
        we.has_ttl = false;
        we.expires_at_ms = 0;
        persistence_->onWrite(we);
    }

    current_bytes_ -= key.size() + node->entry.value.size() + sizeof(Node);
    wheel_.remove(key);
    lru_list_.erase(node);
    index_.erase(it);
    return true;
}

auto
LruStore::size() const -> size_t {
    std::shared_lock lock(mutex_);
    return index_.size();
}

auto
LruStore::evictExpired() -> size_t {
    std::scoped_lock lock(mutex_);

    auto current = now();
    // Advance the wheel to catch up with elapsed wall time (always ≥ 1 tick so
    // a freshly-inserted sub-second TTL fires on the very next call).
    auto elapsed = current - last_evict_;
    auto elapsed_ticks = floor<seconds>(elapsed).count();
    size_t ticks = elapsed_ticks < 1 ? 1 : static_cast<size_t>(elapsed_ticks);
    ticks = std::min(ticks, TtlWheel::K_SLOT_COUNT);
    last_evict_ = current;

    size_t evicted = 0;
    for (size_t i = 0; i < ticks; ++i) {
        wheel_.tick([&](const std::string& key) {
            auto it = index_.find(key);
            if (it == index_.end()) {
                return; // already removed
            }

            auto& node = it->second;
            if (node->entry.has_ttl && node->entry.expires_at <= current) {
                current_bytes_ -= node->key.size() + node->entry.value.size() + sizeof(Node);
                auto list_it = node;
                index_.erase(it);
                lru_list_.erase(list_it);
                ++evicted;
            } else {
                wheel_.insert(node->key, expiryTicks(node->entry.expires_at));
            }
        });
    }
    return evicted;
}

void
LruStore::touch(ListIt it) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

void
LruStore::evictIfNeeded() {
    while (current_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
        evictOne();
    }
}

void
LruStore::evictOne() {
    auto& node = lru_list_.back();
    current_bytes_ -= node.key.size() + node.entry.value.size() + sizeof(Node);
    wheel_.remove(node.key);
    index_.erase(node.key);
    lru_list_.pop_back();
}
} // namespace cinder
