#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

inline constexpr int K_RF2_NODE1 = 17'950;
inline constexpr int K_RF2_NODE2 = 17'951;
inline constexpr int K_RF2_NODE3 = 17'952;

auto
rf2Port(const std::string& id) -> int {
    if (id == "node1") {
        return K_RF2_NODE1;
    }
    if (id == "node2") {
        return K_RF2_NODE2;
    }
    return K_RF2_NODE3;
}

// RF=2, three real nodes: every key must end up on BOTH members of its 3-node
// replica set after node3 joins — replicas rebalance just like primaries.
TEST(RebalanceRf2Test, KeysReachNewReplicaSetOnJoin) {
    NodeProcGuard node1{spawnNode(K_RF2_NODE1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(K_RF2_NODE2),
        /*quorum=*/false,
        /*replica_factor=*/2,
        /*quarantine_interval_ms=*/0)};
    NodeProcGuard node2{spawnNode(K_RF2_NODE2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(K_RF2_NODE1),
        /*quorum=*/false,
        /*replica_factor=*/2,
        /*quarantine_interval_ms=*/0)};
    ASSERT_TRUE(waitForPort(K_RF2_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_RF2_NODE2)) << "node2 did not start";

    std::vector<std::string> keys;
    for (int i = 0; i < 100; i++) {
        keys.push_back("key" + std::to_string(i));
    }

    ConsistentHashRing two(150);
    two.addNode("node1");
    two.addNode("node2");
    for (const auto& k : keys) {
        auto owner = two.getNode(k);
        auto res = setKey(rf2Port(owner), k, "v-" + k);
        ASSERT_TRUE(res.has_value());
        EXPECT_EQ(res.value().status, Errc::OK) << k << " write failed";
    }

    NodeProcGuard node3{spawnNode(K_RF2_NODE3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(K_RF2_NODE1)
            + ",node2@127.0.0.1:" + std::to_string(K_RF2_NODE2),
        /*quorum=*/false,
        /*replica_factor=*/2,
        /*quarantine_interval_ms=*/0)};
    ASSERT_TRUE(waitForPort(K_RF2_NODE3)) << "node3 did not start";

    ConsistentHashRing three(150);
    three.addNode("node1");
    three.addNode("node2");
    three.addNode("node3");
    for (const auto& k : keys) {
        auto desired = three.getNodes(k, 2);
        ASSERT_EQ(desired.size(), 2);
        for (const auto& member : desired) {
            EXPECT_TRUE(waitForValue(rf2Port(member), k, "v-" + k, 100))
                << k << " missing on " << member << " (port " << rf2Port(member) << ")";
        }
    }
}
} // namespace
} // namespace cinder
