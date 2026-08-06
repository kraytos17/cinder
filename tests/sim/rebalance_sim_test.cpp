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

    RebalanceNode(SimClock& c, SimBus& bus, NodeId node_id, size_t capacity)
        : id(node_id),
          clock(c),
          transport(bus, node_id),
          table(node_id),
          ring(50),
          store(capacity, &c),
          shard(store, ring, transport, node_id, c) {
        transport.onMessage([this](const NodeId&, const net::Request& req) {
            if (req.opcode != net::Opcode::Replicate) {
                return;
            }
            VersionedEntry e;
            e.value = req.value;
            e.version = req.version;
            e.writer_node_hash = req.writer_node_hash;
            if (req.expires_at.has_value()) {
                e.has_ttl = true;
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

    std::unique_ptr<RebalanceNode> node1;
    std::unique_ptr<RebalanceNode> node2;
    std::unique_ptr<RebalanceNode> node3;

    RebalanceCluster() {
        constexpr size_t CAPACITY = 1'048'576; // ample — no eviction of test keys
        node1 = std::make_unique<RebalanceNode>(clock, bus, "node1", CAPACITY);
        node2 = std::make_unique<RebalanceNode>(clock, bus, "node2", CAPACITY);
        node3 = std::make_unique<RebalanceNode>(clock, bus, "node3", CAPACITY);
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

} // namespace
} // namespace cinder
