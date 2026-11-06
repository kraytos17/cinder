#include <ankerl/unordered_dense.h>
#include <array>
#include <atomic>
#include <deque>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim/sim_test_helpers.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder {
    namespace {

        using namespace std::chrono_literals;

        // Cluster with both Replicate (fire-and-forget) and GetVersioned
        // (request-response) handlers registered on each node.
        class ReadRepairCluster {
        public:

            SimClock clock;
            SimBus bus{clock, 99};
            SimTransport t1{bus, "node1"};
            SimTransport t2{bus, "node2"};
            LruStore store1{1024, &clock};
            LruStore store2{1024, &clock};
            ReplicationManager mgr1{store1, "node1", clock, t1};
            ReplicationManager mgr2{store2, "node2", clock, t2};

            ReadRepairCluster() {
                bus.registerHandler("node1", applyHandler(store1, clock));
                bus.registerHandler("node2", applyHandler(store2, clock));
                bus.registerRequestHandler("node1", versionedHandler(store1, clock));
                bus.registerRequestHandler("node2", versionedHandler(store2, clock));
            }

        private:

            static auto applyHandler(LruStore& store, SimClock& clock) -> SimBus::Handler {
                return [&store, &clock](const NodeId&, const net::Request& req) {
                    if (req.opcode != net::Opcode::Replicate) {
                        return;
                    }

                    VersionedEntry e;
                    e.value = req.value;
                    e.setVersion(req.version);
                    e.writer_node_hash = req.writer_node_hash;
                    if (req.expires_at.has_value()) {
                        e.setHasTtl(true);
                        e.expires_at = toSteadyExpiry(clock, *req.expires_at);
                    }
                    // NOLINTNEXTLINE
                    (void)store.putVersioned(req.key, std::move(e));
                };
            }

            static auto versionedHandler(LruStore& store, SimClock& clock)
                -> SimBus::RequestHandler {
                return [&store, &clock](const NodeId&, const net::Request& req) -> net::Response {
                    if (req.opcode != net::Opcode::GetVersioned) {
                        return {.status = Errc::InvalidArgument, .value = std::nullopt};
                    }

                    auto entry = store.getVersioned(req.key);
                    if (!entry.has_value()) {
                        return {.status = Errc::NotFound, .value = std::nullopt};
                    }
                    net::Response resp{
                        .status = Errc::OK,
                        .value = entry->value,
                        .version = entry->version(),
                        .writer_node_hash = entry->writer_node_hash,
                    };
                    if (entry->hasTtl()) {
                        resp.expires_at = toSystemExpiry(clock, entry->expires_at);
                    }
                    return resp;
                };
            }
        };

        auto runRead(
            ReplicationManager& mgr,
            const std::string& key,
            const std::vector<NodeId>& replicas,
            size_t R
        ) -> Result<VersionedEntry> {
            Result<VersionedEntry> result = err<VersionedEntry>(Error(Errc::InternalError));
            mgr.readAsync(key, replicas, R, [&](Result<VersionedEntry> r) {
                result = std::move(r);
            });
            return result;
        }

        TEST(ReadRepairSimTest, ReadRepairFixesStaleReplica) {
            ReadRepairCluster c;

            // Write v1 to node1 (primary), replicate to node2.
            auto wr =
                runWrite(c.mgr1, "key1", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async);
            ASSERT_TRUE(wr.has_value());
            c.bus.deliver();
            ASSERT_EQ(c.store2.get("key1"), "v1");

            // Overwrite on node1 with v2 (higher version), replicate to node2.
            auto wr2 =
                runWrite(c.mgr1, "key1", "v2", std::nullopt, {"node2"}, ConsistencyMode::Async);
            ASSERT_TRUE(wr2.has_value());
            c.bus.deliver();
            ASSERT_EQ(c.store2.get("key1"), "v2");

            // Record the current version on node1 (from the second write).
            auto v2_entry = c.store1.getVersioned("key1");
            ASSERT_TRUE(v2_entry.has_value());
            Version v2_version = v2_entry->version();

            // Now simulate a stale replica: manually revert node2 to an older version.
            c.store2.remove("key1");
            VersionedEntry stale;
            stale.value = "v1";
            stale.setVersion(v2_version - 1);
            stale.writer_node_hash = 100;
            ASSERT_TRUE(c.store2.putVersioned("key1", std::move(stale)).has_value());
            ASSERT_EQ(c.store2.get("key1"), "v1");

            // Quorum read from node1 with R=2: should return v2 and repair node2.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "v2");
            EXPECT_EQ(result->version(), v2_version);

            // Deliver the repair Replicate from node1 to node2.
            c.bus.deliver();
            EXPECT_EQ(c.store2.get("key1"), "v2");
            EXPECT_EQ(c.store2.getVersioned("key1")->version(), v2_version);
        }

        TEST(ReadRepairSimTest, ReadRepairSkipsWhenAllAgree) {
            ReadRepairCluster c;

            auto wr =
                runWrite(c.mgr1, "key1", "v1", std::nullopt, {"node2"}, ConsistencyMode::Async);
            ASSERT_TRUE(wr.has_value());
            c.bus.deliver();
            ASSERT_EQ(c.store2.get("key1"), "v1");

            // Quorum read: both agree on v1, no repair needed.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "v1");

            // No Replicate messages should have been sent (bus should be empty).
            EXPECT_EQ(c.bus.pending(), 0);
        }

        // Async quorum read with deferred responses: local store misses, the
        // tail reduces two out-of-order replica versions and must return the
        // LWW winner exactly once. Exercises the read-path reduce (best_*
        // compound maximum) across the inline/tail handoff.
        TEST(ReadRepairSimTest, QuorumReadDeferredResponsesReturnBest) {
            DeferredCluster c;

            std::atomic<int> done_count{0};
            Result<VersionedEntry> result =
                err<VersionedEntry>(Error(Errc::InternalError, "unset"));
            c.mgr.readAsync("key1", {"node2", "node3"}, 2, [&](Result<VersionedEntry> r) {
                result = std::move(r);
                ++done_count;
            });
            // Both requests launched, nothing reported yet.
            EXPECT_EQ(c.transport.pendingRequests(), 2u);
            EXPECT_EQ(done_count.load(), 0);

            // Stale v1 arrives first: no quorum yet (acks=1 < 2), no callback.
            c.transport.completeRequest(1, 100, "v1");
            std::this_thread::sleep_for(milliseconds(50));
            EXPECT_EQ(done_count.load(), 0);

            // Fresh v3 arrives: quorum reached on the tail, best wins.
            c.transport.completeRequest(3, 200, "v3");
            ASSERT_TRUE(awaitDecision(done_count));
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "v3");
            EXPECT_EQ(result->version(), 3);
            EXPECT_EQ(done_count.load(), 1);
        }

        // Duplicate suppression: concurrent reads for the same key share one
        // fan-out (singleflight). The first caller becomes the leader; later
        // arrivals attach as waiters and receive the leader's LWW result,
        // while the transport sees a single request.
        TEST(ReadRepairSimTest, ConcurrentReadsForSameKeyCoalesceIntoOneFanout) {
            DeferredCluster c;

            constexpr int K_READERS = 4;
            std::atomic<int> done_count{0};
            std::array<Result<VersionedEntry>, K_READERS> results;
            for (auto& r : results) {
                r = err<VersionedEntry>(Error(Errc::NotFound, "unset"));
            }
            for (int i = 0; i < K_READERS; ++i) {
                c.mgr.readAsync("key1", {"node2"}, 2, [&, i](Result<VersionedEntry> r) {
                    results[static_cast<size_t>(i)] = std::move(r);
                    ++done_count;
                });
            }
            // One fan-out total despite four callers; nothing reported yet.
            EXPECT_EQ(c.transport.pendingRequests(), 1u);
            EXPECT_EQ(done_count.load(), 0);

            // A read for a different key starts its own flight.
            std::atomic<int> other_done{0};
            c.mgr.readAsync("key2", {"node2"}, 2, [&](Result<VersionedEntry>) { ++other_done; });
            EXPECT_EQ(c.transport.pendingRequests(), 2u);

            // The single replica response completes the coalesced flight:
            // every waiter gets the leader's result.
            c.transport.completeRequest(7, 100, "v7");
            ASSERT_TRUE(awaitDecision(done_count));
            std::this_thread::sleep_for(milliseconds(100));
            EXPECT_EQ(done_count.load(), K_READERS);
            for (const auto& r : results) {
                ASSERT_TRUE(r.has_value());
                EXPECT_EQ(r->value, "v7");
                EXPECT_EQ(r->version(), 7U);
            }

            // The other key's flight is untouched: still pending, unreported.
            EXPECT_EQ(c.transport.pendingRequests(), 1u);
            EXPECT_EQ(other_done.load(), 0);

            // It completes independently when its own replica answers.
            c.transport.completeRequest(9, 200, "other");
            ASSERT_TRUE(awaitDecision(other_done));
            EXPECT_EQ(c.transport.pendingRequests(), 0u);
        }

        TEST(ReadRepairSimTest, ReadRepairReturnsBestVersion) {
            ReadRepairCluster c;

            // Seed node1 with v3, node2 with v1 (node2 is behind).
            VersionedEntry e1;
            e1.value = "v3";
            e1.setVersion(3);
            e1.writer_node_hash = 100;
            ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

            VersionedEntry e2;
            e2.value = "v1";
            e2.setVersion(1);
            e2.writer_node_hash = 200;
            ASSERT_TRUE(c.store2.putVersioned("key1", std::move(e2)).has_value());

            // Quorum read from node1: should return v3 (highest version).
            auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "v3");
            EXPECT_EQ(result->version(), 3);
        }

        TEST(ReadRepairSimTest, ReadRepairFallsBackToLocalOnReplicaFailure) {
            ReadRepairCluster c;

            VersionedEntry e1;
            e1.value = "local-v";
            e1.setVersion(5);
            e1.writer_node_hash = 100;
            ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

            // Bring node2 down so sendRequestAsync fails.
            c.bus.setNodeDown("node2");

            // R=2 but node2 is down — should fall back to local read.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "local-v");
        }

        TEST(ReadRepairSimTest, QuorumFailureStillRepairsStaleReplica) {
            ReadRepairCluster c;

            // Seed node1 with v2, node2 with stale v1.
            VersionedEntry e1;
            e1.value = "v2";
            e1.setVersion(2);
            e1.writer_node_hash = 100;
            ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

            VersionedEntry e2;
            e2.value = "v1-stale";
            e2.setVersion(1);
            e2.writer_node_hash = 200;
            ASSERT_TRUE(c.store2.putVersioned("key1", std::move(e2)).has_value());

            // R=3 with only 1 replica: acks = local(1) + node2(1) = 2 < 3 → quorum fails.
            // Old: repair skipped. New: repair still fanned out.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 3);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "v2");
            EXPECT_EQ(result->version(), 2);

            // Repair Replicate from node1 arrives at node2.
            c.bus.deliver();
            EXPECT_EQ(c.store2.get("key1"), "v2");
            EXPECT_EQ(c.store2.getVersioned("key1")->version(), 2);
        }

        TEST(ReadRepairSimTest, QuorumFailureNotFoundStillRepairs) {
            ReadRepairCluster c;

            // node1 has the key, node2 does not (NotFound → needs_repair).
            VersionedEntry e1;
            e1.value = "only-local";
            e1.setVersion(1);
            e1.writer_node_hash = 100;
            ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

            // R=3 with 1 replica: local acks(1) + NotFound(0) = 1 < 3 → quorum fails.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 3);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "only-local");

            // Repair fanned out — node2 now has the key.
            c.bus.deliver();
            EXPECT_EQ(c.store2.get("key1"), "only-local");
            EXPECT_EQ(c.store2.getVersioned("key1")->version(), 1);
        }

        TEST(ReadRepairSimTest, ReadRepairPreservesTtl) {
            ReadRepairCluster c;

            // Seed node1 with a TTL'd entry at version 2.
            VersionedEntry e1;
            e1.value = "ttl-v2";
            e1.setVersion(2);
            e1.writer_node_hash = 100;
            e1.setHasTtl(true);
            e1.expires_at = c.clock.now() + 10s;
            ASSERT_TRUE(c.store1.putVersioned("key1", std::move(e1)).has_value());

            // Seed node2 with a stale v1 without TTL.
            VersionedEntry e2;
            e2.value = "stale-v1";
            e2.setVersion(1);
            e2.writer_node_hash = 200;
            ASSERT_TRUE(c.store2.putVersioned("key1", std::move(e2)).has_value());

            // Quorum read from node1 with R=2: local wins (v2 has_ttl), node2 is stale.
            auto result = runRead(c.mgr1, "key1", {"node2"}, 2);
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result->value, "ttl-v2");
            EXPECT_EQ(result->version(), 2);
            EXPECT_TRUE(result->hasTtl());

            // Self-heal: local entry is already the winner, putVersioned is a no-op.
            // Repair fan-out: node2 receives the TTL'd entry.
            c.bus.deliver();
            auto node2_entry = c.store2.getVersioned("key1");
            ASSERT_TRUE(node2_entry.has_value());
            EXPECT_EQ(node2_entry->value, "ttl-v2");
            EXPECT_EQ(node2_entry->version(), 2);
            EXPECT_TRUE(node2_entry->hasTtl());
        }

    } // namespace
} // namespace cinder
