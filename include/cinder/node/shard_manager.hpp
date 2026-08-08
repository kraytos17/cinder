#pragma once

#include <chrono>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/store/cache_store.hpp"

using std::chrono::milliseconds;

namespace cinder {

// Rebalances keys when cluster membership changes. After the ring is rebuilt,
// every node enumerates its local store and pushes each key it no longer owns
// to the new ring owner (via the Replicate write path), then drops it locally
// once the replica acknowledges. Best-effort: a key whose push fails stays local
// as a failover copy and is retried on the next rebalance().
//
// Keys are NOT pushed to a quarantined owner (a node that just re-joined and is
// still warming), so a crash-looped node doesn't become a hot migration target.
class ShardManager {
  public:

    ShardManager(CacheStore& store, ConsistentHashRing& ring, Transport& transport,
        MembershipTable& table, NodeId self, Clock& clock, milliseconds quarantine_interval);
    ~ShardManager() = default;

    ShardManager(const ShardManager&) = delete;
    auto operator=(const ShardManager&) -> ShardManager& = delete;
    ShardManager(ShardManager&&) = delete;
    auto operator=(ShardManager&&) -> ShardManager& = delete;

    // Pushes keys this node no longer owns to their new ring owners. Returns
    // true when at least one key was deferred because its owner is still inside
    // the quarantine window (so the caller can retry after it clears).
    auto rebalance() -> bool;

  private:

    void migrateKey(const std::string& key, const VersionedEntry& entry, const NodeId& owner);

    CacheStore& store_;
    ConsistentHashRing& ring_;
    Transport& transport_;
    MembershipTable& table_;
    NodeId self_;
    Clock& clock_;
    milliseconds quarantine_interval_;
};
} // namespace cinder
