#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/client/cache_client.hpp"
#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;

namespace cinder {
namespace {

TEST(MultiGetTest, BatchRetrievesExistingKeys) {
    NodeProcGuard node{spawnNode(17'940, "node1", "")};
    ASSERT_TRUE(waitForPort(17'940)) << "node did not start";

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", 17'940});
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
    NodeProcGuard node{spawnNode(17'941, "node1", "")};
    ASSERT_TRUE(waitForPort(17'941)) << "node did not start";

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", 17'941});
    CacheClient client(config);

    auto found = client.multiGet({"missing1", "missing2"});
    EXPECT_TRUE(found.empty());
}
} // namespace
} // namespace cinder
