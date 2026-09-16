#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickEphemeralPort;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;
using cinder::net::test::waitForValue;

namespace cinder {
namespace {

// Ownership in the CURRENT 3-node ring (node1+node2+node3 all alive).
void
seedRing3(ConsistentHashRing& ring) {
    ring.addNode("node1");
    ring.addNode("node2");
    ring.addNode("node3");
}

// Write each key to its owner in the 2-node ring (before node3 joins).
template <typename PortOf>
void
seedKeys2Node(const std::vector<std::string>& keys, PortOf&& portOf) {
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
    // Quarantine disabled so migrated keys land on node3 immediately — the
    // deferred path (quarantine window + retry) is covered elsewhere.
    // Suspect timeout raised to 30s so the failure detector tolerates ASan
    // startup delays without false-suspecting peers and disrupting the ring.
    // Only 20 keys to keep migration time reasonable under ASan overhead.
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    const int port3 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    ASSERT_NE(port3, 0);
    auto portOf = [&](const std::string& id) -> int {
        if (id == "node1") {
            return port1;
        }
        if (id == "node2") {
            return port2;
        }
        return port3;
    };
    NodeProcGuard node1{spawnNode(port1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    NodeProcGuard node2{spawnNode(port2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(port1),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    std::vector<std::string> keys;
    for (int i = 0; i < 20; i++) {
        keys.push_back("key" + std::to_string(i));
    }
    seedKeys2Node(keys, portOf);

    // Node3 joins: its gossip view (including itself) reaches node1/node2, which
    // adopt it, rebuild the ring, and migrate keys that now hash to node3.
    NodeProcGuard node3{spawnNode(port3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(port1) + ",node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    ASSERT_TRUE(waitForNode(port3, "node3")) << "node3 did not start";

    ConsistentHashRing ring(150);
    seedRing3(ring);
    // Every key that hashes to node3 in the 3-node ring must be served by node3.
    for (const auto& k : keys) {
        if (ring.getNode(k) == "node3") {
            EXPECT_TRUE(waitForValue(port3, k, "v-" + k, 1'200)) << k << " not migrated to node3";
        }
    }
}

TEST(RebalanceOnJoinTest, KeysStayingElsewhereUntouched) {
    const int port1 = pickEphemeralPort();
    const int port2 = pickEphemeralPort();
    const int port3 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    ASSERT_NE(port3, 0);
    auto portOf = [&](const std::string& id) -> int {
        if (id == "node1") {
            return port1;
        }
        if (id == "node2") {
            return port2;
        }
        return port3;
    };
    NodeProcGuard node1{spawnNode(port1,
        "node1",
        "node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    NodeProcGuard node2{spawnNode(port2,
        "node2",
        "node1@127.0.0.1:" + std::to_string(port1),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    ASSERT_TRUE(waitForNode(port1, "node1")) << "node1 did not start";
    ASSERT_TRUE(waitForNode(port2, "node2")) << "node2 did not start";

    std::vector<std::string> keys;
    for (int i = 0; i < 20; i++) {
        keys.push_back("key" + std::to_string(i));
    }
    seedKeys2Node(keys, portOf);

    NodeProcGuard node3{spawnNode(port3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(port1) + ",node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    ASSERT_TRUE(waitForNode(port3, "node3")) << "node3 did not start";

    ConsistentHashRing ring(150);
    seedRing3(ring);
    // Keys still owned by node1 or node2 must remain served by their owner.
    for (const auto& k : keys) {
        auto owner = ring.getNode(k);
        if (owner == "node1" || owner == "node2") {
            EXPECT_TRUE(waitForValue(portOf(owner), k, "v-" + k, 200))
                << k << " dropped from its owner";
        }
    }
}
} // namespace
} // namespace cinder
