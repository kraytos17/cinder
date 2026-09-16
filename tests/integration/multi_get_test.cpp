#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/client/cache_client.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickHeldPort;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;

namespace cinder {
namespace {

TEST(MultiGetTest, BatchRetrievesExistingKeys) {
    const uint16_t port = pickHeldPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1")) << "node did not start";

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", port});
    CacheClient client(config);

    std::vector<std::string> keys;
    for (int i = 0; i < 20; i++) {
        keys.push_back("key" + std::to_string(i));
        ASSERT_TRUE(client.set(keys.back(), "v" + std::to_string(i)).has_value());
    }

    auto found = client.multiGet(keys);
    EXPECT_EQ(found.size(), keys.size());
    for (int i = 0; i < 20; i++) {
        auto it = found.find(keys[i]);
        ASSERT_NE(it, found.end()) << keys[i] << " missing";
        EXPECT_EQ(it->second, "v" + std::to_string(i));
    }
}

TEST(MultiGetTest, MissingKeysAbsent) {
    const uint16_t port = pickHeldPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1")) << "node did not start";

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", port});
    CacheClient client(config);

    auto found = client.multiGet({"missing1", "missing2"});
    EXPECT_TRUE(found.empty());
}
} // namespace
} // namespace cinder
