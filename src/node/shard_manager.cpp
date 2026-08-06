#include "cinder/node/shard_manager.hpp"

#include <utility>

#include "cinder/net/protocol.hpp"

namespace cinder {

ShardManager::ShardManager(
    CacheStore& store, ConsistentHashRing& ring, Transport& transport, NodeId self, Clock& clock)
    : store_(store),
      ring_(ring),
      transport_(transport),
      self_(std::move(self)),
      clock_(clock) {}

void
ShardManager::rebalance() {
    // Collect pending migrations under forEach's store lock; do not send here —
    // a synchronous transport would invoke the remove
    // callback re-entrantly and deadlock on the store mutex.
    struct Pending {
        std::string key;
        VersionedEntry entry;
        NodeId owner;
    };

    std::vector<Pending> pending;
    store_.forEach([this, &pending](const std::string& key, const VersionedEntry& entry) {
        NodeId owner = ring_.getNode(key);
        if (owner != self_) {
            pending.push_back({key, entry, owner});
        }
    });

    // Send after forEach has released the lock; the remove callback then
    // acquires the mutex cleanly regardless of transport sync/async behavior.
    for (auto& p : pending) {
        migrateKey(p.key, p.entry, p.owner);
    }
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
