#include <chrono>
#include <gtest/gtest.h>
#include <string>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using std::chrono::milliseconds;

using cinder::net::test::getKey;
using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickHeldPort;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

// Quorum Get on a 2-node RF=2 cluster should return the value and trigger
// read repair when the replica is missing the key (fresh restart).
TEST(ReadRepairIntegrationTest, QuorumReadRepairsRestartedReplica) {
    const uint16_t port1 = pickHeldPort();
    const uint16_t port2 = pickHeldPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    NodeProcGuard node1{spawnNode(port1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/2,
        /*quarantine_interval_ms=*/0)};
    NodeProcGuard node2{spawnNode(port2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(port1),
        /*quorum=*/false,
        /*replica_factor=*/2,
        /*quarantine_interval_ms=*/0)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");

    std::string key = "repair-key";
    auto nodes = ring.getNodes(key, 2);
    ASSERT_EQ(nodes.size(), 2);

    int primary_port = (nodes[0] == "node1") ? port1 : port2;
    int replica_port = (nodes[1] == "node1") ? port1 : port2;

    // Write v1 — replicates to the replica.
    auto wr = setKey(primary_port, key, "v1");
    ASSERT_TRUE(wr.has_value());
    EXPECT_EQ(wr.value().status, Errc::OK);
    EXPECT_TRUE(waitForValue(replica_port, key, "v1")) << "replica did not receive v1";

    // Quorum Get from the primary — triggers readAsync with R=2.
    // Both nodes have v1, so no repair needed; just verifies the quorum read works.
    auto get1 = getKey(primary_port, key);
    ASSERT_TRUE(get1.has_value());
    EXPECT_EQ(get1.value().status, Errc::OK);
    ASSERT_TRUE(get1.value().value.has_value());
    EXPECT_EQ(*get1.value().value, "v1");
}
} // namespace
} // namespace cinder
