#pragma once

#include <chrono>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

using std::chrono::milliseconds;

namespace cinder {

// Rebalances keys when cluster membership changes. After the ring is rebuilt,
// every node enumerates its local store and reconciles each key against its
// current replica set (primary + `replica_factor - 1` successors):
//   - if this node is still a member of the set, it keeps the key and pushes a
//     copy to every other member (repairs replica gaps left by joins/removals;
//     redundant pushes are idempotent via version-gated LWW apply);
//   - otherwise it migrates the key to the whole new set and drops its local
//     copy once the new primary acknowledges.
// All pushes go through the Replicate write path. Best-effort: a key whose
// push fails stays local as a failover copy and is retried on the next
// rebalance().
//
// Keys are NOT pushed to a quarantined owner (a node that just re-joined and is
// still warming), so a crash-looped node doesn't become a hot migration target.
class ShardManager {
  public:

    ShardManager(CacheStore& store, ConsistentHashRing& ring, Transport& transport,
        MembershipTable& table, NodeId self, Clock& clock, int replica_factor,
        milliseconds quarantine_interval);
    ~ShardManager() = default;

    ShardManager(const ShardManager&) = delete;
    auto operator=(const ShardManager&) -> ShardManager& = delete;
    ShardManager(ShardManager&&) = delete;
    auto operator=(ShardManager&&) -> ShardManager& = delete;

    // Pushes keys this node no longer owns to their new ring owners. Returns
    // true when at least one key was deferred because an owner is still inside
    // the quarantine window (so the caller can retry after it clears).
    auto rebalance() -> bool;

    void setMetrics(MetricsCollector* m) { metrics_ = m; }

  private:

    auto makeReplicateRequest(const std::string& key, const VersionedEntry& entry) -> net::Request;
    void migrateKey(const std::string& key, const VersionedEntry& entry, const NodeId& owner);
    void pushReplica(const std::string& key, const VersionedEntry& entry, const NodeId& owner);

    CacheStore& store_;
    ConsistentHashRing& ring_;
    Transport& transport_;
    MembershipTable& table_;
    NodeId self_;
    Clock& clock_;
    int replica_factor_;
    milliseconds quarantine_interval_;
    MetricsCollector* metrics_ = nullptr;
};
} // namespace cinder
