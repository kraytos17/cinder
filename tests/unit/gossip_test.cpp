#include <gtest/gtest.h>

#include "cinder/cluster/gossip.hpp"

namespace cinder {
namespace {

TEST(GossipParseTest, ValidEntry) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("node1@127.0.0.1:7000:alive:3", info));
    EXPECT_EQ(info.id, "node1");
    EXPECT_EQ(info.host, "127.0.0.1");
    EXPECT_EQ(info.port, 7'000);
    EXPECT_EQ(info.state, NodeState::Alive);
    EXPECT_EQ(info.incarnation, 3U);
}

TEST(GossipParseTest, SuspectState) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("n2@10.0.0.1:8080:suspect:42", info));
    EXPECT_EQ(info.state, NodeState::Suspect);
    EXPECT_EQ(info.incarnation, 42U);
}

TEST(GossipParseTest, DeadState) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("n3@192.168.1.1:9090:dead:0", info));
    EXPECT_EQ(info.state, NodeState::Dead);
    EXPECT_EQ(info.incarnation, 0U);
}

TEST(GossipParseTest, MissingAt) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1:7000:alive:1", info));
}

TEST(GossipParseTest, MissingColons) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1", info));
}

TEST(GossipParseTest, MissingSecondColon) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1:7000", info));
}

TEST(GossipParseTest, MissingThirdColon) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1:7000:alive", info));
}

TEST(GossipParseTest, InvalidPort) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1:abc:alive:1", info));
}

TEST(GossipParseTest, PortOverflow) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1:99999:alive:1", info));
}

TEST(GossipParseTest, InvalidState) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("node1@127.0.0.1:7000:unknown:1", info));
}

TEST(GossipParseTest, EmptyString) {
    NodeInfo info;
    EXPECT_FALSE(gossip::parseEntry("", info));
}

TEST(GossipParseTest, EmptyId) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("@127.0.0.1:7000:alive:1", info));
    EXPECT_EQ(info.id, "");
}

TEST(GossipParseTest, EmptyHost) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("node1@:7000:alive:1", info));
    EXPECT_EQ(info.host, "");
}

TEST(GossipParseTest, LargeIncarnation) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("n@127.0.0.1:7000:alive:18446744073709551615", info));
    EXPECT_EQ(info.incarnation, UINT64_MAX);
}

TEST(GossipParseTest, IncarnationOverflow) {
    NodeInfo info;
    // 18446744073709551616 = UINT64_MAX + 1
    EXPECT_FALSE(gossip::parseEntry("n@127.0.0.1:7000:alive:18446744073709551616", info));
}

TEST(GossipParseTest, PortZero) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("n@127.0.0.1:0:alive:1", info));
    EXPECT_EQ(info.port, 0);
}

TEST(GossipParseTest, PortMax) {
    NodeInfo info;
    EXPECT_TRUE(gossip::parseEntry("n@127.0.0.1:65535:alive:1", info));
    EXPECT_EQ(info.port, 65'535);
}

TEST(GossipParseTest, PortNegative) {
    NodeInfo info;
    // from_chars with unsigned won't accept negative
    EXPECT_FALSE(gossip::parseEntry("n@127.0.0.1:-1:alive:1", info));
}
} // namespace
} // namespace cinder
