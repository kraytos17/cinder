#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <thread>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using std::chrono::milliseconds;

using cinder::net::Opcode;
using cinder::net::Request;
using cinder::net::test::getKey;
using cinder::net::test::K_PORT_NODE1;
using cinder::net::test::K_PORT_NODE2;
using cinder::net::test::K_PORT_NODE3;
using cinder::net::test::NodeProcGuard;
using cinder::net::test::rawRequest;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::stopNode;
using cinder::net::test::waitForPort;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

// Determine which node owns the key and which is its replica (factor 2).
auto
ownersOf(const std::string& key) -> std::pair<int, int> {
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    auto nodes = ring.getNodes(key, 2);
    int primary = !nodes.empty() && nodes[0] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;
    int replica = nodes.size() > 1 && nodes[1] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;
    return {primary, replica};
}

TEST(ReplicaFailoverTest, FanoutReachesReplica) {
    NodeProcGuard node1{spawnNode(
        K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false, 2)};
    NodeProcGuard node2{spawnNode(
        K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    auto [primary, replica] = ownersOf("fanout-key");
    auto set_res = setKey(primary, "fanout-key", "v1");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // The replica must have applied the Replicate write (async fan-out).
    EXPECT_TRUE(waitForValue(replica, "fanout-key", "v1")) << "replica did not apply write";
}

TEST(ReplicaFailoverTest, SurvivesPrimaryFailure) {
    NodeProcGuard node1{spawnNode(
        K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false, 2)};
    NodeProcGuard node2{spawnNode(
        K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    auto [primary, replica] = ownersOf("failover-key");
    auto set_res = setKey(primary, "failover-key", "v2");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);
    ASSERT_TRUE(waitForValue(replica, "failover-key", "v2")) << "replica did not apply write";

    // Kill the primary; the replica still serves the value.
    stopNode((primary == K_PORT_NODE1 ? node1 : node2).proc());

    auto get_res = getKey(replica, "failover-key");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(get_res.value().status, Errc::OK);
    ASSERT_TRUE(get_res.value().value.has_value());
    EXPECT_EQ(*get_res.value().value, "v2");
}

TEST(ReplicaFailoverTest, QuorumFailsClosedWhenReplicaDown) {
    NodeProcGuard node1{spawnNode(
        K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), true, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";

    // Node2 never started → only local ack (1 < W=2) → fail closed.
    // qkey-4 is owned by node1 with node2 as its replica.
    auto set_res = setKey(K_PORT_NODE1, "qkey-4", "v3");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::NotReady);
}

TEST(ReplicaFailoverTest, HintedHandoffReplaysWhenReplicaReturns) {
    NodeProcGuard node1{spawnNode(
        K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";

    // hkey-5 is owned by node1 with node2 as its replica.
    constexpr int K_PRIMARY_PORT = K_PORT_NODE1;
    constexpr int K_REPLICA_PORT = K_PORT_NODE2;

    // Replica is down; async write succeeds locally and is hinted on node1.
    auto set_res = setKey(K_PRIMARY_PORT, "hkey-5", "v4");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // Bring node2 up; node1's replay timer (~1s) should deliver the hint.
    NodeProcGuard node2{spawnNode(
        K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    EXPECT_TRUE(waitForValue(K_REPLICA_PORT, "hkey-5", "v4")) << "hinted write was not replayed";
}

TEST(ReplicaFailoverTest, FanoutToThreeNodes) {
    NodeProcGuard node1{spawnNode(K_PORT_NODE1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2)
            + ",node3@127.0.0.1:" + std::to_string(K_PORT_NODE3),
        false,
        3)};
    NodeProcGuard node2{spawnNode(K_PORT_NODE2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1)
            + ",node3@127.0.0.1:" + std::to_string(K_PORT_NODE3),
        false,
        3)};
    NodeProcGuard node3{spawnNode(K_PORT_NODE3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1)
            + ",node2@127.0.0.1:" + std::to_string(K_PORT_NODE2),
        false,
        3)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE3)) << "node3 did not start";

    // Determine the primary and the two successors via the ring (factor 3).
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");
    auto nodes = ring.getNodes("fanout3-key", 3);
    ASSERT_GE(nodes.size(), 3);

    auto port_of = [](const std::string& id) -> int {
        if (id == "node1") {
            return K_PORT_NODE1;
        }
        if (id == "node2") {
            return K_PORT_NODE2;
        }
        return K_PORT_NODE3;
    };

    int primary = port_of(nodes[0]);
    int replica1 = port_of(nodes[1]);
    int replica2 = port_of(nodes[2]);

    auto set_res = setKey(primary, "fanout3-key", "v5");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // Both replicas must apply the fan-out (R-1 = 2 replicas).
    EXPECT_TRUE(waitForValue(replica1, "fanout3-key", "v5")) << "replica1 did not apply write";
    EXPECT_TRUE(waitForValue(replica2, "fanout3-key", "v5")) << "replica2 did not apply write";
}

TEST(ReplicaFailoverTest, TTLReplicationOverWire) {
    NodeProcGuard node1{spawnNode(
        K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false, 2)};
    NodeProcGuard node2{spawnNode(
        K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false, 2)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    auto nodes = ring.getNodes("ttl-over-wire", 2);
    ASSERT_GE(nodes.size(), 2);
    int primary = nodes[0] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;
    int replica = nodes[1] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;

    // Set with a short TTL; the value must propagate to the replica.
    Request req{
        .opcode = Opcode::Set,
        .key = "ttl-over-wire",
        .value = "ephemeral",
        .ttl = milliseconds(300),
    };
    auto set_res = rawRequest(primary, req);
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);
    ASSERT_TRUE(waitForValue(replica, "ttl-over-wire", "ephemeral"))
        << "replica did not apply TTL write";

    // Wait past the TTL; both primary and replica must expire the entry.
    std::this_thread::sleep_for(milliseconds(500));
    auto primary_get = getKey(primary, "ttl-over-wire");
    ASSERT_TRUE(primary_get.has_value());
    EXPECT_EQ(primary_get.value().status, Errc::NotFound);
    // Replica GET on a miss redirects to the ring owner (NotReady) — the value
    // must no longer be served locally.
    auto replica_get = getKey(replica, "ttl-over-wire");
    ASSERT_TRUE(replica_get.has_value());
    EXPECT_NE(replica_get.value().status, Errc::OK);
}
} // namespace
} // namespace cinder
