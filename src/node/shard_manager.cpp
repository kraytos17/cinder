#include "cinder/node/shard_manager.hpp"

#include <algorithm>
#include <utility>
#include <vector>

#include "cinder/common/logger.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

ShardManager::ShardManager(CacheStore& store, ConsistentHashRing& ring, Transport& transport,
    MembershipTable& table, NodeId self, Clock& clock, int replica_factor,
    milliseconds quarantine_interval)
    : store_(store),
      ring_(ring),
      transport_(transport),
      table_(table),
      self_(std::move(self)),
      clock_(clock),
      replica_factor_(replica_factor),
      quarantine_interval_(quarantine_interval) {}

auto
ShardManager::rebalance() -> bool {
    Logger::info("cinder shard_manager: rebalance started");

    // Collect pending migrations under forEach's store lock; do not send here —
    // a synchronous transport would invoke the remove callback re-entrantly and
    // deadlock on the store mutex.
    struct CopyPush {
        std::string key;
        VersionedEntry entry;
        NodeId owner;
    };

    struct MigratePush {
        std::string key;
        VersionedEntry entry;
        NodeId primary;
    };

    std::vector<CopyPush> copies;
    std::vector<MigratePush> migrates;
    bool deferred = false;
    auto now = clock_.now();
    store_.forEach([this, &copies, &migrates, &deferred, now](
                       const std::string& key, const VersionedEntry& entry) {
        auto desired = ring_.getNodes(key, replica_factor_);
        if (desired.empty()) {
            return;
        }

        bool staying = std::find(desired.begin(), desired.end(), self_) != desired.end();
        if (staying) {
            // Keep the local copy; make sure every other member has it too.
            for (const auto& owner : desired) {
                if (owner == self_ || table_.isQuarantined(owner, now, quarantine_interval_)) {
                    if (owner != self_) {
                        deferred = true;
                    }
                    continue;
                }
                copies.push_back({key, entry, owner});
            }
            return;
        }

        // Leaving the set: migrate to the new owners; drop locally only after
        // the new primary acknowledges.
        bool primary_quarantined = table_.isQuarantined(desired[0], now, quarantine_interval_);
        if (primary_quarantined) {
            deferred = true;
            return;
        }

        migrates.push_back({key, entry, desired[0]});
        for (size_t i = 1; i < desired.size(); ++i) {
            if (table_.isQuarantined(desired[i], now, quarantine_interval_)) {
                deferred = true;
                continue;
            }
            copies.push_back({key, entry, desired[i]});
        }
    });

    // Send after forEach has released the lock; the remove callback then
    // acquires the mutex cleanly regardless of transport sync/async behavior.
    for (auto& p : copies) {
        pushReplica(p.key, p.entry, p.owner);
    }
    for (auto& p : migrates) {
        migrateKey(p.key, p.entry, p.primary);
    }
    Logger::info("cinder shard_manager: rebalance completed copies={} migrations={}",
        copies.size(),
        migrates.size());
    return deferred;
}

auto
ShardManager::makeReplicateRequest(const std::string& key, const VersionedEntry& entry)
    -> net::Request {
    net::Request req;
    req.opcode = net::Opcode::Replicate;
    req.key = key;
    req.value = entry.value;
    req.version = entry.version;
    req.writer_node_hash = entry.writer_node_hash;
    if (entry.has_ttl) {
        req.expires_at = toSystemExpiry(clock_, entry.expires_at);
    }
    return req;
}

void
ShardManager::migrateKey(const std::string& key, const VersionedEntry& entry, const NodeId& owner) {
    auto info = table_.get(owner);
    if (info.has_value() && info->state != NodeState::Alive) {
        Logger::debug("cinder shard_manager: skip migrate key={} owner={} state={}",
            key,
            owner,
            static_cast<int>(info->state));
        return;
    }

    auto req = makeReplicateRequest(key, entry);
    transport_.sendAsync(owner, req, [this, key](Result<void> r) {
        if (r.has_value()) {
            store_.remove(key);
        }
    });
}

void
ShardManager::pushReplica(
    const std::string& key, const VersionedEntry& entry, const NodeId& owner) {
    auto info = table_.get(owner);
    if (info.has_value() && info->state != NodeState::Alive) {
        Logger::debug("cinder shard_manager: skip replica key={} owner={} state={}",
            key,
            owner,
            static_cast<int>(info->state));
        return;
    }
    auto req = makeReplicateRequest(key, entry);
    transport_.sendAsync(owner, req, [](Result<void> /*r*/) {});
}
} // namespace cinder
