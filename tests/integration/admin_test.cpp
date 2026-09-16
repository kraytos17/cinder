#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "integration/test_helpers.hpp"

using cinder::net::test::NodeProcGuard;
using cinder::net::test::pickHeldPort;
using cinder::net::test::rawRequest;
using cinder::net::test::spawnNode;
using cinder::net::test::takeHeldFd;
using cinder::net::test::waitForNode;

namespace {

TEST(AdminTest, InfoReturnsJson) {
    const uint16_t port = pickHeldPort();
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
    const uint16_t port = pickHeldPort();
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
    const uint16_t port = pickHeldPort();
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
    const uint16_t port = pickHeldPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminCompact, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}

TEST(AdminTest, ConfigReloadReturnsOk) {
    const uint16_t port = pickHeldPort();
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
    const uint16_t port = pickHeldPort();
    ASSERT_NE(port, 0);
    NodeProcGuard node{spawnNode(port, "node1", "")};
    ASSERT_TRUE(waitForNode(port, "node1"));

    cinder::net::Request req{.opcode = cinder::net::Opcode::AdminShutdown, .key = {}, .value = {}};
    auto res = rawRequest(port, req);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->status, cinder::Errc::OK);
}

TEST(AdminTest, ReloadPreservesEffectiveIdentity) {
    // The file disagrees with the CLI on identity; CLI wins at boot and the
    // reload must not revert the served identity to file values.
    const uint16_t port = pickHeldPort();
    ASSERT_NE(port, 0);
    const std::string cfg_path = "/tmp/cinder_reload_identity_" + std::to_string(port) + ".yaml";
    {
        std::ofstream f(cfg_path);
        ASSERT_TRUE(f.is_open());
        f << "server:\n  port: 1234\n  node_id: file-node\n";
    }

    auto port_str = std::to_string(port);
    int held_fd = takeHeldFd(port);
    ASSERT_NE(held_fd, -1);
    auto fd_str = std::to_string(held_fd);
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH,
            "cinderd",
            "--port",
            port_str.c_str(),
            "--node-id",
            "cfg-node",
            "--config",
            cfg_path.c_str(),
            "--listen-fd",
            fd_str.c_str(),
            nullptr);
        _exit(1);
    }

    ::close(held_fd);
    cinder::net::test::NodeProcGuard node{{pid, port, "cfg-node"}};
    auto check_identity = [&](const char* phase) {
        cinder::net::Request req{.opcode = cinder::net::Opcode::AdminInfo, .key = {}, .value = {}};
        auto res = rawRequest(port, req);
        ASSERT_TRUE(res.has_value()) << phase;
        ASSERT_EQ(res->status, cinder::Errc::OK) << phase;
        ASSERT_TRUE(res->value.has_value()) << phase;
        EXPECT_TRUE(res->value->contains("\"node_id\":\"cfg-node\"")) << phase;
        EXPECT_TRUE(res->value->contains("\"port\":" + std::to_string(port))) << phase;
    };

    ASSERT_TRUE(waitForNode(port, "cfg-node")) << "server did not start";
    check_identity("before reload");

    cinder::net::Request reload{
        .opcode = cinder::net::Opcode::AdminConfigReload, .key = {}, .value = {}};
    auto reload_res = rawRequest(port, reload);
    ASSERT_TRUE(reload_res.has_value());
    EXPECT_EQ(reload_res.value().status, cinder::Errc::OK);

    check_identity("after reload");
    (void)std::remove(cfg_path.c_str());
}
} // namespace
