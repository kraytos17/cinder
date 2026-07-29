#include <asio.hpp>
#include <chrono>
#include <csignal>
#include <cstring>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include "cinder/net/protocol.hpp"

using asio::ip::tcp;

namespace cinder::net {
namespace {

auto
read_response(tcp::socket& socket) -> Result<Response> {
    std::array<std::byte, 4'096> buf;
    asio::error_code ec;
    (void)asio::read(socket, asio::buffer(buf.data(), kFrameHeaderSize), ec);
    if (ec) {
        return err<Response>(Error(Errc::InternalError, "read header failed"));
    }

    uint32_t net_len;
    std::memcpy(&net_len, &buf[3], sizeof(net_len));
    size_t payload_len = std::byteswap(net_len);
    if (payload_len > buf.size() - kFrameHeaderSize) {
        return err<Response>(Error(Errc::InvalidArgument, "response too large"));
    }
    if (payload_len > 0) {
        (void)asio::read(socket, asio::buffer(buf.data() + kFrameHeaderSize, payload_len), ec);
        if (ec) {
            return err<Response>(Error(Errc::InternalError, "read payload failed"));
        }
    }
    return decode_response(std::span<const std::byte>(buf.data(), kFrameHeaderSize + payload_len));
}

auto
wait_for_port(int port, int max_retries = 50) -> bool {
    asio::io_context io;
    for (int i = 0; i < max_retries; i++) {
        tcp::socket sock(io);
        asio::error_code ec;
        sock.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
        if (!ec) {
            sock.close();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

TEST(ClusterSmokeTest, SetGetDelPing) {
    int port = 17'890;
    std::array<char, 16> port_str;
    (void)snprintf(port_str.data(), port_str.size(), "%d", port);

    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork failed";
    if (pid == 0) {
        execl(CINDER_TEST_CINDERD_PATH, "cinderd", "--port", port_str.data(), nullptr);
        _exit(1);
    }

    ASSERT_TRUE(wait_for_port(port)) << "server did not start in time";

    asio::io_context io;
    tcp::socket socket(io);
    asio::error_code ec;
    socket.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
    ASSERT_FALSE(ec) << "connect failed";
    // SET
    {
        Request req{.opcode = Opcode::Set, .key = "k", .value = "v", .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());

        (void)asio::write(socket, asio::buffer(encoded.value()));
        auto resp = read_response(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // GET
    {
        Request req{.opcode = Opcode::Get, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());

        (void)asio::write(socket, asio::buffer(encoded.value()));
        auto resp = read_response(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
        ASSERT_TRUE(resp.value().value.has_value());
        EXPECT_EQ(*resp.value().value, "v"); // NOLINT
    }

    // DEL
    {
        Request req{.opcode = Opcode::Del, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());

        (void)asio::write(socket, asio::buffer(encoded.value()));
        auto resp = read_response(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    // GET after delete → NotFound
    {
        Request req{.opcode = Opcode::Get, .key = "k", .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());

        (void)asio::write(socket, asio::buffer(encoded.value()));
        auto resp = read_response(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::NotFound);
    }

    // PING
    {
        Request req{.opcode = Opcode::Ping, .key = {}, .value = {}, .ttl = std::nullopt};
        auto encoded = encode(req);
        ASSERT_TRUE(encoded.has_value());

        (void)asio::write(socket, asio::buffer(encoded.value()));
        auto resp = read_response(socket);
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp.value().status, Errc::OK);
    }

    socket.close();
    (void)kill(pid, SIGTERM);
    (void)waitpid(pid, nullptr, 0);
}
} // namespace
} // namespace cinder::net
