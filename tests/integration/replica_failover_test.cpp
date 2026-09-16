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
using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickEphemeralPort;
using cinder::net::test::rawRequest;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::stopNode;
using cinder::net::test::waitForNode;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

// Determine which node owns the key and which is its replica (factor 2).
auto
ownersOf(const std::string& key) -> std::pair<std::string, std::string> {
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    auto nodes = ring.getNodes(key, 2);
    return {nodes[0], nodes.size() > 1 ? nodes[1] : nodes[0]};
}

TEST(ReplicaFailoverTest, FanoutReachesReplica) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{
        spawnNode(port1, "node1", "node2@127.0.0.1:" + std::to_string(port2), false, 2)};
    NodeProcGuard node2{
        spawnNode(port2, "node2", "node1@127.0.0.1:" + std::to_string(port1), false, 2)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    auto [primary_id, replica_id] = ownersOf("fanout-key");
    int primary = (primary_id == "node1") ? port1 : port2;
    int replica = (replica_id == "node1") ? port1 : port2;
    auto set_res = setKey(primary, "fanout-key", "v1");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // The replica must have applied the Replicate write (async fan-out).
    EXPECT_TRUE(waitForValue(replica, "fanout-key", "v1")) << "replica did not apply write";
}

TEST(ReplicaFailoverTest, SurvivesPrimaryFailure) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{
        spawnNode(port1, "node1", "node2@127.0.0.1:" + std::to_string(port2), false, 2)};
    NodeProcGuard node2{
        spawnNode(port2, "node2", "node1@127.0.0.1:" + std::to_string(port1), false, 2)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    auto [primary_id, replica_id] = ownersOf("failover-key");
    int primary = (primary_id == "node1") ? port1 : port2;
    int replica = (replica_id == "node1") ? port1 : port2;
    auto set_res = setKey(primary, "failover-key", "v2");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);
    ASSERT_TRUE(waitForValue(replica, "failover-key", "v2")) << "replica did not apply write";

    // Kill the primary; the replica still serves the value.
    stopNode((primary_id == "node1" ? node1 : node2).proc());

    auto get_res = getKey(replica, "failover-key");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(get_res.value().status, Errc::OK);
    ASSERT_TRUE(get_res.value().value.has_value());
    EXPECT_EQ(*get_res.value().value, "v2");
}

TEST(ReplicaFailoverTest, QuorumFailsClosedWhenReplicaDown) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{
        spawnNode(port1, "node1", "node2@127.0.0.1:" + std::to_string(port2), true, 2)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";

    // Node2 never started → only local ack (1 < W=2) → fail closed.
    // qkey-4 is owned by node1 with node2 as its replica.
    auto set_res = setKey(port1, "qkey-4", "v3");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::NotReady);
}

TEST(ReplicaFailoverTest, HintedHandoffReplaysWhenReplicaReturns) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{
        spawnNode(port1, "node1", "node2@127.0.0.1:" + std::to_string(port2), false, 2)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";

    // hkey-5 is owned by node1 with node2 as its replica.
    // Replica is down; async write succeeds locally and is hinted on node1.
    auto set_res = setKey(port1, "hkey-5", "v4");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // Bring node2 up; node1's replay timer (~1s) should deliver the hint.
    NodeProcGuard node2{
        spawnNode(port2, "node2", "node1@127.0.0.1:" + std::to_string(port1), false, 2)};
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    EXPECT_TRUE(waitForValue(port2, "hkey-5", "v4")) << "hinted write was not replayed";
}

TEST(ReplicaFailoverTest, FanoutToThreeNodes) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    const int port3 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    ASSERT_NE(port3, 0);
    NodeProcGuard node1{spawnNode(port1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(port2) + ",node3@127.0.0.1:" + std::to_string(port3),
        false,
        3)};
    NodeProcGuard node2{spawnNode(port2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(port1) + ",node3@127.0.0.1:" + std::to_string(port3),
        false,
        3)};
    NodeProcGuard node3{spawnNode(port3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(port1) + ",node2@127.0.0.1:" + std::to_string(port2),
        false,
        3)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";
    ASSERT_TRUE(waitForNode(port3, "node3")) << "node3 did not start";

    // Determine the primary and the two successors via the ring (factor 3).
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");
    auto nodes = ring.getNodes("fanout3-key", 3);
    ASSERT_GE(nodes.size(), 3);

    auto port_of = [&](const std::string& id) -> int {
        if (id == "node1") {
            return port1;
        }
        if (id == "node2") {
            return port2;
        }
        return port3;
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
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{
        spawnNode(port1, "node1", "node2@127.0.0.1:" + std::to_string(port2), false, 2)};
    NodeProcGuard node2{
        spawnNode(port2, "node2", "node1@127.0.0.1:" + std::to_string(port1), false, 2)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    auto nodes = ring.getNodes("ttl-over-wire", 2);
    ASSERT_GE(nodes.size(), 2);
    int primary = nodes[0] == "node1" ? port1 : port2;
    int replica = nodes[1] == "node1" ? port1 : port2;

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
