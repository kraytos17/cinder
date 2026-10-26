#include "cinder/node/shard_manager.hpp"

#include <algorithm>
#include <utility>
#include <vector>

#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

    ShardManager::ShardManager(
        CacheStore& store,
        ConsistentHashRing& ring,
        Transport& transport,
        MembershipTable& table,
        NodeId self,
        Clock& clock,
        int replica_factor,
        milliseconds quarantine_interval
    ) :
        store_(store),
        ring_(ring),
        transport_(transport),
        table_(table),
        self_(std::move(self)),
        clock_(clock),
        replica_factor_(replica_factor),
        quarantine_interval_(quarantine_interval) {}

    auto ShardManager::rebalance() -> bool {
        Span span("shard.rebalance");
        CINDER_INFO("rebalance started");

        // Collect pending migrations; do not send here — a synchronous transport
        // would invoke the remove callback re-entrantly and deadlock on the store
        // mutex.
        struct CopyPush {
            std::string key;
            VersionedEntry entry;
            NodeId owner;
        };

        struct MigratePush {
            std::string key;
            VersionedEntry entry;
            NodeId primary;
            std::vector<NodeId> replica_peers;
        };

        std::vector<CopyPush> copies;
        std::vector<MigratePush> migrates;

        size_t deferred_count = 0;
        auto now = clock_.now();
        for (const auto& [key, entry] : store_.liveEntries()) {
            auto desired = ring_.getNodes(key, replica_factor_);
            if (desired.empty()) {
                continue;
            }

            bool staying = std::find(desired.begin(), desired.end(), self_) != desired.end();
            if (staying) {
                // Keep the local copy; make sure every other member has it too.
                for (const auto& owner : desired) {
                    if (owner == self_ || table_.isQuarantined(owner, now, quarantine_interval_)) {
                        if (owner != self_) {
                            ++deferred_count;
                        }
                        continue;
                    }
                    copies.push_back({key, entry, owner});
                }
                continue;
            }

            // Leaving the set: migrate to the new owners; the local copy is
            // dropped only once EVERY new owner has acked (see migrateKey).
            bool primary_quarantined = table_.isQuarantined(desired[0], now, quarantine_interval_);
            if (primary_quarantined) {
                ++deferred_count;
                continue;
            }

            migrates.push_back({key, entry, desired[0], {}});
            // Non-primary members that must also receive the key. SENT by
            // migrateKey (not via `copies`), so the ack tally and the send are
            // owned by one place -- pushing to `copies` as well would replicate
            // twice and inflate the tally.
            std::vector<NodeId> replica_peers;
            for (size_t i = 1; i < desired.size(); ++i) {
                if (table_.isQuarantined(desired[i], now, quarantine_interval_)) {
                    ++deferred_count;
                    continue;
                }
                replica_peers.push_back(desired[i]);
            }
            migrates.back().replica_peers = std::move(replica_peers);
        }

        // Send after the snapshot has been collected and the lock released;
        // the remove callback then acquires the mutex cleanly regardless of
        // transport sync/async behavior.
        for (auto& p : copies) {
            pushReplica(p.key, p.entry, p.owner);
        }
        for (auto& p : migrates) {
            migrateKey(p.key, p.entry, p.primary, p.replica_peers);
        }

        CINDER_INFO(
            "rebalance completed",
            {"copies", copies.size()},
            {"migrations", migrates.size()},
            {"deferred", deferred_count}
        );
        if (metrics_) {
            metrics_->clusterMetrics().rebalance_copies.fetch_add(
                copies.size(),
                std::memory_order_relaxed
            );
            metrics_->clusterMetrics().rebalance_migrations.fetch_add(
                migrates.size(),
                std::memory_order_relaxed
            );
        }
        return deferred_count > 0;
    }

    auto ShardManager::makeReplicateRequest(const std::string& key, const VersionedEntry& entry)
        -> net::Request {
        net::Request req;
        req.opcode = net::Opcode::Replicate;
        req.key = key;
        req.value = entry.value;
        req.version = entry.version();
        req.writer_node_hash = entry.writer_node_hash;
        if (entry.hasTtl()) {
            req.expires_at = toSystemExpiry(clock_, entry.expires_at);
        }
        return req;
    }

    void ShardManager::migrateKey(
        const std::string& key,
        const VersionedEntry& entry,
        const NodeId& owner,
        const std::vector<NodeId>& replica_peers
    ) {
        Span span("shard.migrate");
        auto info = table_.get(owner);
        if (info.has_value() && info->state != NodeState::Alive) {
            CINDER_DEBUG(
                "skip migrate",
                {"key", key},
                {"owner", owner},
                {"state", static_cast<int>(info->state)}
            );
            return;
        }

        // Tracks acks from every member of the new replica set for this key. The
        // local copy is dropped ONLY once ALL of them have confirmed. Dropping on
        // the primary's ack alone (the previous behaviour) left a window where the
        // key was under-replicated, and if the non-primary pushes then failed the
        // local copy was already gone -- silent loss of an acknowledged write.
        // Retention is always safe (an extra copy is redundant, never
        // split-brain); destruction on a stale ack is not. This is the Dynamo /
        // Cassandra rule: drop a replica only once every intended holder has it.
        struct AckTally {
            std::atomic<size_t> acks{0};
        };

        auto tally = std::make_shared<AckTally>();
        const size_t expected = 1 + replica_peers.size(); // primary + replicas
        const size_t replica_count = replica_peers.size();
        for (const auto& peer : replica_peers) {
            auto peer_req = makeReplicateRequest(key, entry);
            transport_.sendAsync(peer, peer_req, [tally](Result<void> r) {
                if (r.has_value()) {
                    tally->acks.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        auto req = makeReplicateRequest(key, entry);
        transport_.sendAsync(
            owner,
            req,
            [this, key, owner, tally, expected, replica_count](Result<void> r) {
            if (!r.has_value()) {
                CINDER_WARN(
                    "migrate failed",
                    {"key", key},
                    {"to", owner},
                    {"reason", r.error().message()}
                );
                // Keep the local copy as a failover copy; notify the owner so it
                // can schedule a delayed re-rebalance once the peer recovers.
                if (metrics_) {
                    metrics_->clusterMetrics().rebalance_migration_failures.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                if (on_migration_failed_) {
                    on_migration_failed_();
                }
                return;
            }

            const size_t acks = tally->acks.fetch_add(1, std::memory_order_relaxed) + 1;
            if (acks == expected) {
                // Every member of the new set holds the key: the local copy is now
                // a redundant orphan and can be reclaimed.
                CINDER_DEBUG(
                    "migrated key",
                    {"key", key},
                    {"to", owner},
                    {"replicas", replica_count}
                );
                store_.remove(key);
                return;
            }

            // Primary acked but at least one replica has not confirmed. KEEP the
            // local copy as an extra failover replica and report it: over-
            // replication is safe, under-replication is not. The next rebalance
            // retries, and hinted handoff / anti-entropy cover the replica
            // independently.
            if (metrics_) {
                metrics_->clusterMetrics().rebalance_dropped_underreplicated.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }
            CINDER_WARN(
                "migration retained locally: replicas unconfirmed",
                {"key", key},
                {"primary", owner},
                {"acks", acks},
                {"expected", expected}
            );
        }
        );
    }

    void ShardManager::pushReplica(
        const std::string& key,
        const VersionedEntry& entry,
        const NodeId& owner
    ) {
        Span span("shard.push");
        auto info = table_.get(owner);
        if (info.has_value() && info->state != NodeState::Alive) {
            CINDER_DEBUG(
                "skip replica",
                {"key", key},
                {"owner", owner},
                {"state", static_cast<int>(info->state)}
            );
            return;
        }

        auto req = makeReplicateRequest(key, entry);
        transport_.sendAsync(owner, req, [key, owner](Result<void> r) {
            if (!r.has_value()) {
                CINDER_WARN(
                    "push failed",
                    {"key", key},
                    {"to", owner},
                    {"reason", r.error().message()}
                );
            }
        });
    }
} // namespace cinder
