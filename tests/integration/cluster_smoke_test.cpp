#include <asio.hpp>
#include <chrono>
#include <csignal>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::write;
using std::chrono::milliseconds;

using asio::ip::address_v4;
using asio::ip::tcp;

using cinder::net::Opcode;
using cinder::net::Request;
using cinder::net::test::NodeProcGuard;
using cinder::net::test::readResponse;
using cinder::net::test::spawnNode;
using cinder::net::test::waitForPort;

namespace cinder::net {
namespace {

constexpr int K_SMOKE_PORT1 = 17'890;
constexpr int K_SMOKE_PORT2 = 17'891;
constexpr int K_SMOKE_PORT3 = 17'892;
constexpr int K_SMOKE_PORT4 = 17'893;

TEST(ClusterSmokeTest, SetGetDelPing) {
    NodeProcGuard node{spawnNode(K_SMOKE_PORT1, "node1", "")};
    ASSERT_TRUE(waitForPort(K_SMOKE_PORT1)) << "server did not start in time";

    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), K_SMOKE_PORT1), ec);
    ASSERT_FALSE(ec) << "connect failed";

    // SET
    {
        Request req{.opcode = Opcode::Set, .key = "k", .value = "v", .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // GET
    {
        Request req{.opcode = Opcode::Get, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
        ASSERT_TRUE(resp.value().value.has_value());
        EXPECT_EQ(*resp.value().value, "v");
    }

    // DEL
    {
        Request req{.opcode = Opcode::Del, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // GET after delete -> NotFound
    {
        Request req{.opcode = Opcode::Get, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::NotFound);
    }

    // PING
    {
        Request req{.opcode = Opcode::Ping, .key = {}, .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }
}

TEST(ClusterSmokeTest, TTLExpiry) {
    NodeProcGuard node{spawnNode(K_SMOKE_PORT2, "node1", "")};
    ASSERT_TRUE(waitForPort(K_SMOKE_PORT2)) << "server did not start in time";

    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), K_SMOKE_PORT2), ec);
    ASSERT_FALSE(ec) << "connect failed";

    // SET with 200ms TTL
    {
        Request req{
            .opcode = Opcode::Set,
            .key = "ttlkey",
            .value = "ephemeral",
            .ttl = milliseconds(200),
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // Get immediately - still valid
    {
        Request req{
            .opcode = Opcode::Get,
            .key = "ttlkey",
            .value = {},
            .ttl = std::nullopt,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // Wait past TTL
    std::this_thread::sleep_for(milliseconds(300));
    // Get after TTL - should be expired
    {
        Request req{
            .opcode = Opcode::Get,
            .key = "ttlkey",
            .value = {},
            .ttl = std::nullopt,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::NotFound);
    }
}

TEST(ClusterSmokeTest, CapacityEviction) {
    // This test requires --capacity which spawnNode doesn't support, so we
    // fork/exec manually.
    int port = K_SMOKE_PORT3;
    auto port_str = std::to_string(port);
    auto cap_str = std::to_string(300);
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH,
            "cinderd",
            "--port",
            port_str.c_str(),
            "--capacity",
            cap_str.c_str(),
            nullptr);
        _exit(1);
    }

    ASSERT_TRUE(waitForPort(port)) << "server did not start in time";

    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), port), ec);
    ASSERT_FALSE(ec) << "connect failed";

    std::string val(30, 'x');
    // SET k1 - ~35 bytes
    {
        Request req{.opcode = Opcode::Set, .key = "k1", .value = val};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // SET k2 - ~35 bytes, total ~70 < 80
    {
        Request req{.opcode = Opcode::Set, .key = "k2", .value = val};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // SET k3 - total ~105 > 80, k1 should be evicted
    {
        Request req{.opcode = Opcode::Set, .key = "k3", .value = val};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // k1 should be evicted
    {
        Request req{
            .opcode = Opcode::Get,
            .key = "k1",
            .value = {},
            .ttl = std::nullopt,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::NotFound);
    }

    // k3 should still be present
    {
        Request req{
            .opcode = Opcode::Get,
            .key = "k3",
            .value = {},
            .ttl = std::nullopt,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
        ASSERT_TRUE(resp.value().value.has_value());
        EXPECT_EQ(*resp.value().value, val);
    }

    socket.close();
    (void)kill(pid, SIGTERM);
    (void)waitpid(pid, nullptr, 0);
}

TEST(ClusterSmokeTest, LargeValue) {
    NodeProcGuard node{spawnNode(K_SMOKE_PORT4, "node1", "")};
    ASSERT_TRUE(waitForPort(K_SMOKE_PORT4)) << "server did not start in time";

    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), K_SMOKE_PORT4), ec);
    ASSERT_FALSE(ec) << "connect failed";

    std::string big_val(50'000, 'Z');
    // SET big value
    {
        Request req{
            .opcode = Opcode::Set,
            .key = "big",
            .value = big_val,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // GET back and verify
    {
        Request req{
            .opcode = Opcode::Get,
            .key = "big",
            .value = {},
            .ttl = std::nullopt,
        };

        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());
        (void)write(socket, buffer(encoded.value()));
        auto resp = readResponse(socket);

        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
        ASSERT_TRUE(resp.value().value.has_value());
        EXPECT_EQ(resp.value().value->size(), big_val.size());
        EXPECT_EQ(*resp.value().value, big_val);
    }
}
} // namespace
} // namespace cinder::net
