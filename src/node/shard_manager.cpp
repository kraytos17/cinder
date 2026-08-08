#include "cinder/node/shard_manager.hpp"

#include <utility>
#include <vector>

#include "cinder/net/protocol.hpp"

namespace cinder {

ShardManager::ShardManager(CacheStore& store, ConsistentHashRing& ring, Transport& transport,
    MembershipTable& table, NodeId self, Clock& clock, milliseconds quarantine_interval)
    : store_(store),
      ring_(ring),
      transport_(transport),
      table_(table),
      self_(std::move(self)),
      clock_(clock),
      quarantine_interval_(quarantine_interval) {}

auto
ShardManager::rebalance() -> bool {
    // Collect pending migrations under forEach's store lock; do not send here —
    // a synchronous transport would invoke the remove callback re-entrantly and
    // deadlock on the store mutex.
    struct Pending {
        std::string key;
        VersionedEntry entry;
        NodeId owner;
    };

    std::vector<Pending> pending;
    bool deferred = false;
    auto now = clock_.now();
    store_.forEach(
        [this, &pending, &deferred, now](const std::string& key, const VersionedEntry& entry) {
        NodeId owner = ring_.getNode(key);
        if (owner == self_) {
            return;
        }
        if (table_.isQuarantined(owner, now, quarantine_interval_)) {
            deferred = true;
            return;
        }
        pending.push_back({key, entry, owner});
    });

    // Send after forEach has released the lock; the remove callback then
    // acquires the mutex cleanly regardless of transport sync/async behavior.
    for (auto& p : pending) {
        migrateKey(p.key, p.entry, p.owner);
    }
    return deferred;
}

void
ShardManager::migrateKey(const std::string& key, const VersionedEntry& entry, const NodeId& owner) {
    net::Request req;
    req.opcode = net::Opcode::Replicate;
    req.key = key;
    req.value = entry.value;
    req.version = entry.version;
    req.writer_node_hash = entry.writer_node_hash;
    if (entry.has_ttl) {
        req.expires_at = toSystemExpiry(clock_, entry.expires_at);
    }

    transport_.sendAsync(owner, req, [this, key](Result<void> r) {
        if (r.has_value()) {
            store_.remove(key);
        }
    });
}
} // namespace cinder
