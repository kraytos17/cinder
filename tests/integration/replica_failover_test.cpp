#include <asio.hpp>
#include <chrono>
#include <csignal>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using asio::ip::address_v4;
using asio::ip::tcp;

using cinder::net::Opcode;
using cinder::net::Request;
using cinder::net::Response;
using cinder::net::test::readResponse;
using cinder::net::test::waitForPort;

namespace cinder {
namespace {

constexpr int K_PORT_NODE1 = 17'910;
constexpr int K_PORT_NODE2 = 17'911;

struct NodeProc {
    pid_t pid = -1;
    int port = 0;
    std::string id;
};

auto
spawnNode(int port, const std::string& id, const std::string& peer_list, bool quorum) -> NodeProc {
    auto port_str = std::to_string(port);
    pid_t pid = fork();
    if (pid == -1) {
        ADD_FAILURE() << "fork failed";
        return {};
    }
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH,
            "cinderd",
            "--port",
            port_str.c_str(),
            "--node-id",
            id.c_str(),
            "--replication-factor",
            "2",
            "--consistency",
            quorum ? "quorum" : "async",
            "--peers",
            peer_list.c_str(),
            nullptr);
        _exit(1);
    }
    return {pid, port, id};
}

// Terminate a spawned node; SIGKILL fallback if it does not exit within the
// deadline so tests can never deadlock on shutdown.
void
stopNode(const NodeProc& node) {
    if (node.pid <= 0) {
        return;
    }

    (void)kill(node.pid, SIGTERM);
    int status = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        pid_t r = waitpid(node.pid, &status, WNOHANG);
        if (r == node.pid) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    (void)kill(node.pid, SIGKILL);
    (void)waitpid(node.pid, &status, 0);
}

// RAII guard: ensures spawned nodes are stopped on any test exit/failure.
class NodeProcGuard {
  public:

    explicit NodeProcGuard(NodeProc node)
        : node_(std::move(node)) {}

    NodeProcGuard(const NodeProcGuard&) = delete;
    auto operator=(const NodeProcGuard&) -> NodeProcGuard& = delete;

    NodeProcGuard(NodeProcGuard&& other) noexcept
        : node_(std::move(other.node_)) {
        other.node_ = {};
    }

    auto operator=(NodeProcGuard&& other) noexcept -> NodeProcGuard& {
        if (this != &other) {
            stopNode(node_);
            node_ = std::move(other.node_);
            other.node_ = {};
        }
        return *this;
    }

    ~NodeProcGuard() { stopNode(node_); }

    auto proc() const -> const NodeProc& { return node_; }

  private:

    NodeProc node_;
};

auto
rawRequest(int port, const Request& req) -> cinder::Result<Response> {
    asio::io_context io;
    tcp::socket socket(io);
    asio::error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), port), ec);
    if (ec) {
        return cinder::err<Response>(cinder::Error(cinder::Errc::InternalError, "connect failed"));
    }

    auto encoded = cinder::net::encode(req);
    if (!encoded.has_value()) {
        return cinder::err<Response>(encoded.error());
    }
    (void)asio::write(socket, asio::buffer(encoded.value()), ec);
    if (ec) {
        return cinder::err<Response>(cinder::Error(cinder::Errc::InternalError, "write failed"));
    }

    auto resp = readResponse(socket);
    socket.close();
    return resp;
}

auto
setKey(int port, const std::string& key, const std::string& value) -> cinder::Result<Response> {
    Request req{.opcode = Opcode::Set, .key = key, .value = value, .ttl = std::nullopt};
    return rawRequest(port, req);
}

auto
getKey(int port, const std::string& key) -> cinder::Result<Response> {
    Request req{.opcode = Opcode::Get, .key = key, .value = {}, .ttl = std::nullopt};
    return rawRequest(port, req);
}

// Determine which node owns the key and which is its replica (factor 2).
auto
ownersOf(const std::string& key) -> std::pair<int, int> {
    ConsistentHashRing ring(150);
    ring.addNode("node1");
    ring.addNode("node2");
    auto nodes = ring.getNodes(key, 2);
    int primary = !nodes.empty() && nodes[0] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;
    int replica = nodes.size() > 1 && nodes[1] == "node1" ? K_PORT_NODE1 : K_PORT_NODE2;
    return {primary, replica};
}

// Poll a node until it returns the expected value (async fan-out/replay is not
// instant).
auto
waitForValue(int port, const std::string& key, const std::string& expected, int max_attempts = 50)
    -> bool {
    for (int i = 0; i < max_attempts; i++) {
        auto res = getKey(port, key);
        if (res.has_value() && res.value().status == Errc::OK && res.value().value.has_value()
            && *res.value().value == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

TEST(ReplicaFailoverTest, FanoutReachesReplica) {
    NodeProcGuard node1{
        spawnNode(K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false)};
    NodeProcGuard node2{
        spawnNode(K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    auto [primary, replica] = ownersOf("fanout-key");
    auto set_res = setKey(primary, "fanout-key", "v1");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // The replica must have applied the Replicate write (async fan-out).
    EXPECT_TRUE(waitForValue(replica, "fanout-key", "v1")) << "replica did not apply write";
}

TEST(ReplicaFailoverTest, SurvivesPrimaryFailure) {
    NodeProcGuard node1{
        spawnNode(K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false)};
    NodeProcGuard node2{
        spawnNode(K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    auto [primary, replica] = ownersOf("failover-key");
    auto set_res = setKey(primary, "failover-key", "v2");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);
    ASSERT_TRUE(waitForValue(replica, "failover-key", "v2")) << "replica did not apply write";

    // Kill the primary; the replica still serves the value.
    stopNode((primary == K_PORT_NODE1 ? node1 : node2).proc());

    auto get_res = getKey(replica, "failover-key");
    ASSERT_TRUE(get_res.has_value());
    EXPECT_EQ(get_res.value().status, Errc::OK);
    ASSERT_TRUE(get_res.value().value.has_value());
    EXPECT_EQ(*get_res.value().value, "v2");
}

TEST(ReplicaFailoverTest, QuorumFailsClosedWhenReplicaDown) {
    NodeProcGuard node1{
        spawnNode(K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), true)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";

    // Node2 never started → only local ack (1 < W=2) → fail closed.
    // qkey-4 is owned by node1 with node2 as its replica.
    auto set_res = setKey(K_PORT_NODE1, "qkey-4", "v3");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::NotReady);
}

TEST(ReplicaFailoverTest, HintedHandoffReplaysWhenReplicaReturns) {
    NodeProcGuard node1{
        spawnNode(K_PORT_NODE1, "node1", "node2@127.0.0.1:" + std::to_string(K_PORT_NODE2), false)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE1)) << "node1 did not start";

    // hkey-5 is owned by node1 with node2 as its replica.
    constexpr int K_PRIMARY_PORT = K_PORT_NODE1;
    constexpr int K_REPLICA_PORT = K_PORT_NODE2;

    // Replica is down; async write succeeds locally and is hinted on node1.
    auto set_res = setKey(K_PRIMARY_PORT, "hkey-5", "v4");
    ASSERT_TRUE(set_res.has_value());
    EXPECT_EQ(set_res.value().status, Errc::OK);

    // Bring node2 up; node1's replay timer (~1s) should deliver the hint.
    NodeProcGuard node2{
        spawnNode(K_PORT_NODE2, "node2", "node1@127.0.0.1:" + std::to_string(K_PORT_NODE1), false)};
    ASSERT_TRUE(waitForPort(K_PORT_NODE2)) << "node2 did not start";

    EXPECT_TRUE(waitForValue(K_REPLICA_PORT, "hkey-5", "v4")) << "hinted write was not replayed";
}
} // namespace
} // namespace cinder
