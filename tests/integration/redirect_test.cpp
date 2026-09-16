#include <algorithm>
#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

#include "cinder/client/cache_client.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "integration/test_helpers.hpp"

using std::chrono::milliseconds;

using cinder::net::test::getKey;
using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickEphemeralPort;
using cinder::net::test::setKey;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;

namespace cinder {
namespace {

// After node3 joins and migration completes, keys hashing to node3 in the
// 3-node ring must be served by node3 while their stale 2-node owners answer
// with a redirect. Collect such keys — they are the non-vacuous proof that
// the client's 2-node view is stale.
template <typename PortOf>
auto
collectRedirected(const std::vector<std::string>& keys, int new_owner_port, PortOf&& portOf)
    -> std::vector<std::string> {
    ConsistentHashRing ring2(150);
    ring2.addNode("node1");
    ring2.addNode("node2");
    ConsistentHashRing ring3(150);
    ring3.addNode("node1");
    ring3.addNode("node2");
    ring3.addNode("node3");

    std::vector<std::string> redirected;
    for (int round = 0; round < 1'200 && redirected.size() < 3; ++round) {
        for (const auto& k : keys) {
            if (ring3.getNode(k) != "node3") {
                continue;
            }
            auto on_new = getKey(new_owner_port, k);
            if (!on_new.has_value() || on_new.value().status != Errc::OK) {
                continue; // not migrated yet
            }
            auto on_old = getKey(portOf(ring2.getNode(k)), k);
            if (!on_old.has_value() || on_old.value().status != Errc::NotReady) {
                continue; // not redirected yet
            }
            // The redirect must carry the new owner's address — without it the
            // client cannot follow (and the test would pass vacuously if the
            // stale owner still served the key).
            auto hint = on_old.value().value.value_or("");
            if (hint.find('@') == std::string::npos
                || std::find(redirected.begin(), redirected.end(), k) != redirected.end()) {
                continue;
            }
            redirected.push_back(k);
        }
        if (redirected.size() < 3) {
            std::this_thread::sleep_for(milliseconds(100));
        }
    }
    return redirected;
}

TEST(ClientRedirectTest, MultiGetFollowsRedirectsAfterJoin) {
    const uint16_t port1 = pickEphemeralPort();
    const uint16_t port2 = pickEphemeralPort();
    const uint16_t port3 = pickEphemeralPort();
    ASSERT_NE(port1, 0);
    ASSERT_NE(port2, 0);
    ASSERT_NE(port3, 0);
    auto port_of = [&](const std::string& id) -> int {
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
    for (int i = 0; i < 30; i++) {
        keys.push_back("rdkey" + std::to_string(i));
    }
    ConsistentHashRing ring2(150);
    ring2.addNode("node1");
    ring2.addNode("node2");
    for (const auto& k : keys) {
        auto res = setKey(port_of(ring2.getNode(k)), k, "v-" + k);
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(res.value().status, Errc::OK) << k << " write failed";
    }

    // Client frozen on the 2-node view — its ring goes stale once node3 joins.
    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", static_cast<uint16_t>(port1)});
    config.nodes.push_back({"node2", "127.0.0.1", static_cast<uint16_t>(port2)});
    CacheClient client(config);

    NodeProcGuard node3{spawnNode(port3,
        "node3",
        "node1@127.0.0.1:" + std::to_string(port1) + ",node2@127.0.0.1:" + std::to_string(port2),
        /*quorum=*/false,
        /*replica_factor=*/1,
        /*quarantine_interval_ms=*/0,
        /*suspect_timeout_ms=*/30'000)};
    ASSERT_TRUE(waitForNode(port3, "node3")) << "node3 did not start";

    auto redirected = collectRedirected(keys, port3, port_of);
    ASSERT_GE(redirected.size(), 1)
        << "no key migrated with a redirect at its stale owner — test would pass vacuously";

    // Single-key path: follow the redirect and learn the owner.
    auto one = client.get(redirected[0]);
    ASSERT_TRUE(one.has_value()) << redirected[0] << " not served after redirect";
    EXPECT_EQ(*one, "v-" + redirected[0]);

    // Batch path: every key must come back, including the migrated ones the
    // stale ring routes to the wrong node.
    auto found = client.multiGet(keys);
    EXPECT_EQ(found.size(), keys.size());
    for (const auto& k : keys) {
        auto it = found.find(k);
        ASSERT_NE(it, found.end()) << k << " missing from multiGet";
        EXPECT_EQ(it->second, "v-" + k);
    }
}
} // namespace
} // namespace cinder
