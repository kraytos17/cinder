#include <gtest/gtest.h>
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

struct TestNode {
    NodeId id;
    SimClock& clock;
    SimTransport transport;
    MembershipTable table;
    ConsistentHashRing ring;
    LruStore store;
    ShardManager shard;

    TestNode(SimClock& c, SimBus& bus, NodeId node_id, size_t capacity, int replica_factor)
        : id(node_id),
          clock(c),
          transport(bus, node_id),
          table(node_id),
          ring(50),
          store(capacity, &c),
          shard(store, ring, transport, table, node_id, c, replica_factor, 0ms) {
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
            [[maybe_unused]] auto res = store.putVersioned(req.key, std::move(e));
        });
    }
};

TEST(ConcurrentRebalanceSimTest, RapidJoinLeave) {
    SimClock clock;
    SimBus bus(clock, 42);
    constexpr size_t CAPACITY = 1'048'576;

    TestNode node1(clock, bus, "node1", CAPACITY, 1);
    TestNode node2(clock, bus, "node2", CAPACITY, 1);
    TestNode node3(clock, bus, "node3", CAPACITY, 1);

    // Start with node1 only
    node1.ring.addNode("node1");
    node1.table.seed({});

    // Seed 100 keys
    for (int i = 0; i < 100; ++i) {
        auto key = "key" + std::to_string(i);
        (void)node1.store.put(key, "value" + std::to_string(i)); // NOLINT
    }

    EXPECT_EQ(node1.store.size(), 100U);
    // Rapid join/leave cycle: node2 joins, node2 leaves, node3 joins, node3 leaves
    auto add_node = [&](TestNode& node) {
        node.ring.addNode(node.id);
        NodeInfo rumor;
        rumor.id = node.id;
        rumor.host = "127.0.0.1";
        rumor.port = 17'900;
        rumor.state = NodeState::Alive;
        rumor.incarnation = 1;
        node1.table.applyRumor(node.id, rumor);
        node1.shard.rebalance();
    };

    auto remove_node = [&](TestNode& node) {
        node.ring.removeNode(node.id);
        node1.table.markDead(node.id);
        node1.shard.rebalance();
    };

    // Rapid cycle — should not crash
    for (int cycle = 0; cycle < 5; ++cycle) {
        add_node(node2);
        remove_node(node2);
        add_node(node3);
        remove_node(node3);
    }

    // All keys should still be accessible on node1
    for (int i = 0; i < 100; ++i) {
        auto key = "key" + std::to_string(i);
        EXPECT_TRUE(node1.store.get(key).has_value()) << "key " << key << " lost after cycle";
    }
}

TEST(ConcurrentRebalanceSimTest, RebalanceIdempotency) {
    SimClock clock;
    SimBus bus(clock, 99);
    constexpr size_t CAPACITY = 1'048'576;

    TestNode node1(clock, bus, "node1", CAPACITY, 1);
    TestNode node2(clock, bus, "node2", CAPACITY, 1);

    node1.ring.addNode("node1");
    node1.table.seed({});

    for (int i = 0; i < 50; ++i) {
        (void)node1.store.put("k" + std::to_string(i), "v" + std::to_string(i)); // NOLINT
    }

    // Join node2
    node2.ring.addNode("node2");
    NodeInfo rumor;
    rumor.id = "node2";
    rumor.host = "127.0.0.1";
    rumor.port = 17'901;
    rumor.state = NodeState::Alive;
    rumor.incarnation = 1;
    node1.table.applyRumor("node2", rumor);

    // Run rebalance 3 times — should be idempotent
    auto deferred1 = node1.shard.rebalance();
    auto deferred2 = node1.shard.rebalance();
    auto deferred3 = node1.shard.rebalance();

    // Results should be consistent (all deferred or all not)
    EXPECT_EQ(deferred1, deferred2);
    EXPECT_EQ(deferred2, deferred3);
}
} // namespace
} // namespace cinder
