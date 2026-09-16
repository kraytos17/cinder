#include <gtest/gtest.h>
#include <string>

#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickEphemeralPort;
using cinder::net::test::rawRequest;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForNode;

namespace {

TEST(AdminTest, InfoReturnsJson) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminInfo, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("node_id"));
    EXPECT_TRUE(res->value->contains("port"));
    EXPECT_TRUE(res->value->contains("capacity_bytes"));
    // Identity values must reflect the running daemon, not file defaults.
    EXPECT_TRUE(res->value->contains("\"node_id\":\"node1\""));
    EXPECT_TRUE(res->value->contains("\"port\":" + std::to_string(port)));
}

TEST(AdminTest, ClusterReturnsNodeList) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminCluster, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("alive"));
    EXPECT_TRUE(res->value->contains("node1"));
}

TEST(AdminTest, RingReturnsJson) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminRing, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("self"));
    EXPECT_TRUE(res->value->contains("vnodes_per_node"));
}

TEST(AdminTest, CompactReturnsOk) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminCompact, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}

TEST(AdminTest, ConfigReloadReturnsOk) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{
        .opcode = cinder::net::Opcode::AdminConfigReload, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}

TEST(AdminTest, ShutdownReturnsOk) {
    const uint16_t port = pickEphemeralPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminShutdown, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}
} // namespace
