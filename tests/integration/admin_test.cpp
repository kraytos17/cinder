#include <gtest/gtest.h>
#include <string>

#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::rawRequest;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;

namespace {

constexpr int K_ADMIN_PORT1 = 17'940;
constexpr int K_ADMIN_PORT2 = 17'941;

TEST(AdminTest, InfoReturnsJson) {
    NodeProcGuard node{spawnNode(K_ADMIN_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_ADMIN_PORT1));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminInfo, .key = {}, .value = {}};
    auto res = rawRequest(K_ADMIN_PORT1, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("node_id"));
    EXPECT_TRUE(res->value->contains("port"));
    EXPECT_TRUE(res->value->contains("capacity_bytes"));
}

TEST(AdminTest, ClusterReturnsNodeList) {
    NodeProcGuard node{spawnNode(K_ADMIN_PORT2, "node1", "")};
    ASSERT_TRUE(waitForPort(K_ADMIN_PORT2));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminCluster, .key = {}, .value = {}};
    auto res = rawRequest(K_ADMIN_PORT2, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("alive"));
    EXPECT_TRUE(res->value->contains("node1"));
}

TEST(AdminTest, RingReturnsJson) {
    NodeProcGuard node{spawnNode(K_ADMIN_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_ADMIN_PORT1));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminRing, .key = {}, .value = {}};
    auto res = rawRequest(K_ADMIN_PORT1, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
    ASSERT_TRUE(res->value.has_value());
    EXPECT_TRUE(res->value->contains("self"));
    EXPECT_TRUE(res->value->contains("vnodes_per_node"));
}

TEST(AdminTest, CompactReturnsOk) {
    NodeProcGuard node{spawnNode(K_ADMIN_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_ADMIN_PORT1));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminCompact, .key = {}, .value = {}};
    auto res = rawRequest(K_ADMIN_PORT1, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}

TEST(AdminTest, ConfigReloadReturnsOk) {
    NodeProcGuard node{spawnNode(K_ADMIN_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_ADMIN_PORT1));

    cinder::net::Request req{
        .opcode = cinder::net::Opcode::AdminConfigReload, .key = {}, .value = {}};
    auto res = rawRequest(K_ADMIN_PORT1, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}
} // namespace
