#include <gtest/gtest.h>
#include <string>

#include "cinder/client/connection_pool.hpp"
#include "cinder/node/cache_node_server.hpp"

namespace cinder {
namespace {

TEST(ParsePeerTest, ValidPeer) {
    ClusterConfig::NodeConfig out;
    EXPECT_TRUE(parsePeer("node2@127.0.0.1:17911", out));
    EXPECT_EQ(out.id, "node2");
    EXPECT_EQ(out.host, "127.0.0.1");
    EXPECT_EQ(out.port, 17'911);
}

TEST(ParsePeerTest, MissingSeparator) {
    ClusterConfig::NodeConfig out;
    EXPECT_FALSE(parsePeer("node2", out));
}

TEST(ParsePeerTest, MissingPort) {
    ClusterConfig::NodeConfig out;
    EXPECT_FALSE(parsePeer("node2@127.0.0.1", out));
}

TEST(ParsePeerTest, EmptyId) {
    ClusterConfig::NodeConfig out;
    EXPECT_FALSE(parsePeer("@127.0.0.1:17911", out));
}
} // namespace
} // namespace cinder
