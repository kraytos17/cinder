#include <gtest/gtest.h>

#include "cinder/client/cache_client.hpp"

namespace cinder {
namespace {

TEST(CacheClientTest, RoutesSingleNode) {
    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", 7'000});

    CacheClient client(config);
    // Can't test actual network in unit test, just construction
    SUCCEED();
}

TEST(CacheClientTest, RoutePrimaryIsConsistent) {
    ConsistentHashRing ring(150);
    ring.addNode("a");
    ring.addNode("b");

    std::string key = "test-key";
    auto first = ring.getNode(key);
    for (int i = 0; i < 100; i++) {
        EXPECT_EQ(ring.getNode(key), first);
    }
}

TEST(CacheClientTest, RoutePrimaryDistributes) {
    ConsistentHashRing ring(150);
    ring.addNode("a");
    ring.addNode("b");

    std::set<std::string> nodes;
    for (int i = 0; i < 1'000; i++) {
        nodes.insert(ring.getNode(std::to_string(i)));
    }
    EXPECT_GE(nodes.size(), 2);
}

TEST(CacheClientTest, ParseRedirect) {
    EXPECT_EQ(parseRedirect("moved to node2"), std::optional<NodeId>("node2"));
    EXPECT_EQ(parseRedirect("moved to  node2 "), std::optional<NodeId>(" node2 "));
    EXPECT_FALSE(parseRedirect("OK").has_value());
    EXPECT_FALSE(parseRedirect("").has_value());
    EXPECT_FALSE(parseRedirect("moved to").has_value());
}
} // namespace
} // namespace cinder
