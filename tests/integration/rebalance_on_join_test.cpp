#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::K_PORT_RB_NODE1;
using cinder::net::test::K_PORT_RB_NODE2;
using cinder::net::test::K_PORT_RB_NODE3;
using cinder::net::test::NodeProcGuard;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

auto
portOf(const std::string& id) -> int {
    if (id == "node1") {
        return K_PORT_RB_NODE1;
    }
    if (id == "node2") {
        return K_PORT_RB_NODE2;
    }
    return K_PORT_RB_NODE3;
}

// Ownership in the CURRENT 3-node ring (node1+node2+node3 all alive).
void
seedRing3(ConsistentHashRing& ring) {
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");
}

// Write each key to its owner in the 2-node ring (before node3 joins).
void
seedKeys2Node(const std::vector<std::string>& keys) {
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    for (const auto& k : keys) {
        auto owner = ring.getNode(k);
        auto res = setKey(portOf(owner), k, "v-" + k);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, Errc::OK) << k << " write failed";
    }
}

TEST(RebalanceOnJoinTest, KeysMigrateToJoiningNode) {
    NodeProcGuard node1{
        spawnNode(K_PORT_RB_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_RB_NODE2))};
    NodeProcGuard node2{
        spawnNode(K_PORT_RB_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_RB_NODE1))};
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE2)) << "node2 did not start";

    std::vector<std::string> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back("key" + std::to_string(i));
    }
    seedKeys2Node(keys);

    // Node3 joins: its gossip view (including itself) reaches node1/node2, which
    // adopt it, rebuild the ring, and migrate keys that now hash to node3.
    NodeProcGuard node3{spawnNode(K_PORT_RB_NODE3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(K_PORT_RB_NODE1)
            + ",node2@127.0.0.1:" + std::to_string(K_PORT_RB_NODE2))};
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE3)) << "node3 did not start";

    ConsistentHashRing ring(150);
    seedRing3(ring);
    // Every key that hashes to node3 in the 3-node ring must be served by node3.
    for (const auto& k : keys) {
        if (ring.getNode(k) == "node3") {
            EXPECT_TRUE(waitForValue(K_PORT_RB_NODE3, k, "v-" + k, 100))
                << k << " not migrated to node3";
        }
    }
}

TEST(RebalanceOnJoinTest, KeysStayingElsewhereUntouched) {
    NodeProcGuard node1{
        spawnNode(K_PORT_RB_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_RB_NODE2))};
    NodeProcGuard node2{
        spawnNode(K_PORT_RB_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_RB_NODE1))};
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE2)) << "node2 did not start";

    std::vector<std::string> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back("key" + std::to_string(i));
    }
    seedKeys2Node(keys);

    NodeProcGuard node3{spawnNode(K_PORT_RB_NODE3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(K_PORT_RB_NODE1)
            + ",node2@127.0.0.1:" + std::to_string(K_PORT_RB_NODE2))};
    ASSERT_TRUE(waitForPort(K_PORT_RB_NODE3)) << "node3 did not start";

    ConsistentHashRing ring(150);
    seedRing3(ring);
    // Keys still owned by node1 or node2 must remain served by their owner.
    for (const auto& k : keys) {
        auto owner = ring.getNode(k);
        if (owner == "node1" || owner == "node2") {
            EXPECT_TRUE(waitForValue(portOf(owner), k, "v-" + k, 100))
                << k << " dropped from its owner";
        }
    }
}

} // namespace
} // namespace cinder
