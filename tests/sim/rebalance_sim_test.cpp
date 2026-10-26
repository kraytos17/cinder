#include <algorithm>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "cinder/cluster/membership.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/node/shard_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

namespace cinder {
    namespace {

        using namespace std::chrono_literals;

        // A minimal 3-node view over a shared bus: each node has a store, a ring view,
        // a membership table, and a ShardManager that pushes keys it no longer owns.
        // Inbound Replicate writes are applied to the node's store so migrated keys land.
        struct RebalanceNode {
            NodeId id;
            SimClock& clock;
            SimTransport transport;
            MembershipTable table;
            ConsistentHashRing ring;
            LruStore store;
            ShardManager shard;

            RebalanceNode(
                SimClock& c,
                SimBus& bus,
                NodeId node_id,
                size_t capacity,
                int replica_factor,
                std::chrono::milliseconds quarantine = std::chrono::milliseconds(0)
            ) :
                id(node_id),
                clock(c),
                transport(bus, node_id),
                table(node_id),
                ring(50),
                store(capacity, &c),
                shard(store, ring, transport, table, node_id, c, replica_factor, quarantine) {
                transport.onMessage([this](const NodeId&, const net::Request& req) {
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
                });
            }
        };

        struct RebalanceCluster {
            SimClock clock;
            SimBus bus{clock, 11};
            int replica_factor;

            std::unique_ptr<RebalanceNode> node1;
            std::unique_ptr<RebalanceNode> node2;
            std::unique_ptr<RebalanceNode> node3;

            explicit RebalanceCluster(
                int rf = 1,
                std::chrono::milliseconds quarantine = std::chrono::milliseconds(0)
            ) :
                replica_factor(rf) {
                constexpr size_t CAPACITY = 1'048'576; // ample — no eviction of test keys
                node1 =
                    std::make_unique<RebalanceNode>(clock, bus, "node1", CAPACITY, rf, quarantine);
                node2 =
                    std::make_unique<RebalanceNode>(clock, bus, "node2", CAPACITY, rf, quarantine);
                node3 =
                    std::make_unique<RebalanceNode>(clock, bus, "node3", CAPACITY, rf, quarantine);
            }

            // node1 only: seeded with node1 + node2 (no node3 yet).
            void seedTwoNodes() const {
                node1->table.seed({{"node2", "127.0.0.1", 17'902}});
                node1->ring.addNode("node1");
                node1->ring.addNode("node2");
            }

            // A Gossip view from node3 reaches node1 → node1 adopts node3 → ring rebuild
            // → ShardManager rebalances keys that now hash to node3.
            void joinNode3() const {
                NodeInfo rumor;
                rumor.id = "node3";
                rumor.host = "127.0.0.1";
                rumor.port = 17'903;
                rumor.state = NodeState::Alive;
                rumor.incarnation = 1;
                node1->table.applyRumor("node3", rumor);
                node1->ring.addNode("node3");
                node1->shard.rebalance();
            }

            auto node(const NodeId& id) const -> RebalanceNode* {
                if (id == "node1") {
                    return node1.get();
                }
                if (id == "node2") {
                    return node2.get();
                }
                return node3.get();
            }
        };

        TEST(RebalanceSimTest, KeysMoveToJoiningNode) {
            RebalanceCluster c;
            c.seedTwoNodes();

            // Populate node1 with keys owned by node1 or node2 in the 2-node ring.
            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
            }
            for (const auto& k : keys) {
                // NOLINTNEXTLINE
                (void)c.node1->store.put(k, "v-" + k);
            }

            // After node3 joins, keys that hash to node3 must be pushed to it and
            // removed from node1.
            c.joinNode3();
            c.clock.advance(1ms);
            c.bus.deliver();

            size_t moved_to_node3 = 0;
            for (const auto& k : keys) {
                auto owner = c.node1->ring.getNode(k);
                if (owner == "node3") {
                    EXPECT_TRUE(c.node3->store.get(k).has_value()) << k << " not migrated";
                    EXPECT_FALSE(c.node1->store.get(k).has_value()) << k << " not dropped locally";
                    ++moved_to_node3;
                }
            }
            // With 3 nodes and ~1/3 ownership per node, some keys must have moved.
            EXPECT_GT(moved_to_node3, 0);
        }

        // §2 (rebalance safety): the local copy is dropped only once EVERY member
        // of the new replica set has acked. If a non-primary replica is
        // unreachable, the copy must be RETAINED -- over-replication is harmless,
        // but dropping early is the pattern that silently loses acknowledged
        // writes (the Dynamo/Cassandra rule: drop a replica only once every
        // intended holder has it).
        //
        // rf=2 on a 3-node cluster, so a migrated key has a primary + exactly one
        // replica -- which makes the single-replica-down case directly testable.
        TEST(RebalanceSimTest, UnreachableReplicaRetainsLocalCopy) {
            RebalanceCluster c(/*rf=*/2);
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
                // NOLINTNEXTLINE
                (void)c.node1->store.put(keys.back(), "v");
            }

            // node3 joins the ring but is DOWN, so a key whose new replica set
            // includes node3 cannot gather acks from every intended holder.
            c.bus.setNodeDown("node3");
            c.joinNode3();
            c.clock.advance(1ms);
            c.bus.deliver();

            size_t retained = 0;
            for (const auto& k : keys) {
                auto replica_set = c.node1->ring.getNodes(k, 2);
                const bool node1_leaving =
                    std::find(replica_set.begin(), replica_set.end(), "node1") == replica_set.end();
                const bool set_includes_down_node =
                    std::find(replica_set.begin(), replica_set.end(), "node3") != replica_set.end();
                if (!node1_leaving || !set_includes_down_node) {
                    continue;
                }

                // The safety property: an unconfirmed replica means the local copy
                // must survive as a failover replica rather than be dropped.
                EXPECT_TRUE(c.node1->store.get(k).has_value())
                    << k
                    << " was dropped although its replica set was incomplete";
                ++retained;
            }
            EXPECT_GT(retained, 0)
                << "test setup produced no migration onto an unreachable replica";
        }

        TEST(RebalanceSimTest, FullyConfirmedMigrationDropsLocalCopy) {
            // The counterpart: with every replica reachable the local copy MUST be
            // released, so the safety gate does not degrade into never dropping.
            RebalanceCluster c(/*rf=*/2);
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
                // NOLINTNEXTLINE
                (void)c.node1->store.put(keys.back(), "v");
            }

            c.joinNode3(); // all three nodes up and reachable
            c.clock.advance(1ms);
            c.bus.deliver();

            size_t dropped = 0;
            for (const auto& k : keys) {
                auto replica_set = c.node1->ring.getNodes(k, 2);
                const bool node1_leaving =
                    std::find(replica_set.begin(), replica_set.end(), "node1") == replica_set.end();
                if (!node1_leaving) {
                    continue;
                }
                EXPECT_FALSE(c.node1->store.get(k).has_value())
                    << k
                    << " kept locally although every replica acked";
                ++dropped;
            }
            EXPECT_GT(dropped, 0) << "test setup produced no migration";
        }

        TEST(RebalanceSimTest, KeysStayingOnNode1AreUntouched) {
            RebalanceCluster c;
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
            }
            for (const auto& k : keys) {
                // NOLINTNEXTLINE
                (void)c.node1->store.put(k, "v-" + k);
            }

            c.joinNode3();
            c.clock.advance(1ms);
            c.bus.deliver();

            for (const auto& k : keys) {
                auto owner = c.node1->ring.getNode(k);
                if (owner == "node1") {
                    EXPECT_TRUE(c.node1->store.get(k).has_value()) << k << " wrongly dropped";
                }
            }
        }

        // Quarantined owners defer migration: with rf=2 over two nodes every key's
        // replica set is {node1, node2}, so while node2 is inside the quarantine
        // window every copy push is skipped, rebalance() reports deferred work,
        // and nothing is sent. After the window clears, a re-rebalance pushes.
        TEST(RebalanceSimTest, QuarantinedOwnersDeferMigration) {
            RebalanceCluster c(2, std::chrono::seconds(60));
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 8; i++) {
                keys.push_back("qk" + std::to_string(i));
            }
            for (const auto& k : keys) {
                // NOLINTNEXTLINE
                (void)c.node1->store.put(k, "v-" + k);
            }

            // Both nodes were seeded ~now, so node2 is quarantined: deferred.
            EXPECT_TRUE(c.node1->shard.rebalance());
            EXPECT_EQ(c.bus.pending(), 0u) << "quarantined owners must get no pushes";
            c.bus.deliver();
            for (const auto& k : keys) {
                EXPECT_TRUE(c.node1->store.get(k).has_value()) << k << " dropped while deferred";
                EXPECT_FALSE(c.node2->store.get(k).has_value()) << k << " pushed while quarantined";
            }

            // Past the window the same call migrates for real.
            c.clock.advance(120s);
            EXPECT_FALSE(c.node1->shard.rebalance());
            c.bus.deliver();
            for (const auto& k : keys) {
                EXPECT_TRUE(c.node2->store.get(k).has_value()) << k << " not migrated after window";
            }
        }

        // With replication factor 2, after node3 joins every key must be present on
        // both members of its 3-node replica set — node1 keeps what it still owns and
        // pushes replicas to the new member, and drops keys it no longer owns once the
        // new primary has them.
        TEST(RebalanceSimTest, KeysReplicateToNewReplicaSetOnJoin) {
            RebalanceCluster c(2);
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
            }
            for (const auto& k : keys) {
                // NOLINTNEXTLINE
                (void)c.node1->store.put(k, "v-" + k);
            }

            c.joinNode3();
            c.clock.advance(1ms);
            c.bus.deliver();

            for (const auto& k : keys) {
                auto desired = c.node1->ring.getNodes(k, 2);
                ASSERT_EQ(desired.size(), 2);
                for (const auto& member : desired) {
                    EXPECT_TRUE(c.node(member)->store.get(k).has_value())
                        << k
                        << " missing on "
                        << member;
                }
                // node1 keeps exactly the keys still in its replica set.
                bool node1_owns =
                    std::find(desired.begin(), desired.end(), "node1") != desired.end();
                EXPECT_EQ(c.node1->store.get(k).has_value(), node1_owns) << k;
            }
        }

        // With replication factor 3 and three nodes, every key belongs to the whole
        // cluster: after node3 joins, node2 and node3 must both hold every key.
        TEST(RebalanceSimTest, ReplicasSpreadToAllNewOwners) {
            RebalanceCluster c(3);
            c.seedTwoNodes();

            std::vector<std::string> keys;
            for (int i = 0; i < 100; i++) {
                keys.push_back("key" + std::to_string(i));
            }
            for (const auto& k : keys) {
                // NOLINTNEXTLINE
                (void)c.node1->store.put(k, "v-" + k);
            }

            c.joinNode3();
            c.clock.advance(1ms);
            c.bus.deliver();

            for (const auto& k : keys) {
                EXPECT_TRUE(c.node1->store.get(k).has_value()) << k << " missing on node1";
                EXPECT_TRUE(c.node2->store.get(k).has_value()) << k << " missing on node2";
                EXPECT_TRUE(c.node3->store.get(k).has_value()) << k << " missing on node3";
            }
        }
    } // namespace
} // namespace cinder
