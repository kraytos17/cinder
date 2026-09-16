#ifdef CINDER_ENABLE_TLS

#include <asio.hpp>
#include <asio/ssl.hpp>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/common/status.hpp"
#include "cinder/net/protocol.hpp"
#include "integration/test_helpers.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::read;
using asio::write;
using asio::ip::tcp;
using asio::ssl::context;
using asio::ssl::stream;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace {

// Path to test certificates (relative to build dir)
std::string
fixturePath(const std::string& name) {
    // Try source tree first, then build dir
    auto src = std::filesystem::path(CMAKE_CURRENT_SOURCE_DIR) / "fixtures" / name;
    if (std::filesystem::exists(src)) {
        return src.string();
    }
    return std::filesystem::path(CMAKE_CURRENT_BINARY_DIR) / "fixtures" / name;
}

template <typename Stream>
[[maybe_unused]] static auto
readResponse(Stream& socket) -> cinder::Result<cinder::net::Response> {
    std::array<std::byte, 65'536> buf{};
    error_code ec;
    (void)read(socket, buffer(buf.data(), cinder::net::K_FRAME_HEADER_SIZE), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "read header failed"));
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &buf[3], sizeof(net_len));
    size_t payload_len = std::byteswap(net_len);
    if (payload_len > buf.size() - cinder::net::K_FRAME_HEADER_SIZE) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InvalidArgument, "response too large"));
    }
    if (payload_len > 0) {
        (void)read(socket, buffer(buf.data() + cinder::net::K_FRAME_HEADER_SIZE, payload_len), ec);
        if (ec) {
            return cinder::err<cinder::net::Response>(
                cinder::Error(cinder::Errc::InternalError, "read payload failed"));
        }
    }
    return cinder::net::decodeResponse(
        std::span<const std::byte>(buf.data(), cinder::net::K_FRAME_HEADER_SIZE + payload_len));
}

[[maybe_unused]] static auto
rawTlsRequest(int port, const cinder::net::Request& req, context& ssl_ctx)
    -> cinder::Result<cinder::net::Response> {
    io_context io;
    stream<tcp::socket> socket(io, ssl_ctx);
    error_code ec;
    socket.lowest_layer().connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "connect failed: " + ec.message()));
    }

    socket.handshake(stream<tcp::socket>::client, ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "tls handshake failed: " + ec.message()));
    }

    auto encoded = cinder::net::encode(req);
    if (!encoded.has_value()) {
        return cinder::err<cinder::net::Response>(encoded.error());
    }

    (void)write(socket, buffer(encoded.value()), ec);
    if (ec) {
        return cinder::err<cinder::net::Response>(
            cinder::Error(cinder::Errc::InternalError, "write failed: " + ec.message()));
    }

    auto resp = readResponse(socket);
    socket.lowest_layer().close(ec);
    return resp;
}

[[maybe_unused]] static auto
waitForTlsNode(int port, context& ssl_ctx, int max_retries = 50) -> bool {
    // Identity check over TLS: the answering daemon must report the expected
    // bound port, rejecting stale daemons from earlier runs.
    for (int i = 0; i < max_retries; i++) {
        cinder::net::Request req{.opcode = cinder::net::Opcode::AdminInfo};
        auto res = rawTlsRequest(port, req, ssl_ctx);
        if (res.has_value() && res.value().status == cinder::Errc::OK
            && res.value().value.has_value()
            && res.value().value->contains("\"port\":" + std::to_string(port))) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(100));
    }
    return false;
}

auto
spawnTlsDaemon(int port) -> pid_t {
    auto port_str = std::to_string(port);
    auto cert = fixturePath("server.pem");
    auto key = fixturePath("server-key.pem");
    auto ca = fixturePath("ca.pem");
    int held_fd = cinder::net::test::takeHeldFd(static_cast<uint16_t>(port));
    auto fd_str = std::to_string(held_fd);
    pid_t pid = fork();
    if (pid == 0) {
        // NOLINTNEXTLINE
        execl(CINDER_TEST_CINDERD_PATH,
            "cinderd",
            "--port",
            port_str.c_str(),
            "--tls",
            "--tls-cert",
            cert.c_str(),
            "--tls-key",
            key.c_str(),
            "--tls-ca",
            ca.c_str(),
            "--listen-fd",
            fd_str.c_str(),
            nullptr);
        _exit(1);
    }
    if (held_fd >= 0) {
        ::close(held_fd);
    }
    return pid;
}

void
stopDaemon(pid_t pid) {
    if (pid <= 0) {
        return;
    }

    (void)kill(pid, SIGTERM);
    int status = 0;
    auto deadline = steady_clock::now() + seconds(2);
    while (steady_clock::now() < deadline) {
        pid_t r = waitpid(pid, &status, WNOHANG);
        if (r == pid) {
            return;
        }
        std::this_thread::sleep_for(milliseconds(50));
    }
    (void)kill(pid, SIGKILL);
    (void)waitpid(pid, &status, 0);
}

class TlsIntegrationTest : public ::testing::Test {
  protected:

    void SetUp() override {
        ssl_ctx_.emplace(context::tlsv12_client);
        ssl_ctx_->load_verify_file(fixturePath("ca.pem"));
        ssl_ctx_->set_verify_mode(asio::ssl::verify_peer);
    }

    std::optional<context> ssl_ctx_;
};

TEST_F(TlsIntegrationTest, SetGetOverTls) {
    const uint16_t port = cinder::net::test::pickHeldPort();
    ASSERT_NE(port, 0);
    pid_t dpid = spawnTlsDaemon(port);
    ASSERT_TRUE(waitForTlsNode(port, *ssl_ctx_));

    cinder::net::Request set_req{
        .opcode = cinder::net::Opcode::Set, .key = "tls-key", .value = "tls-value"};

    auto set_res = rawTlsRequest(port, set_req, *ssl_ctx_);
    ASSERT_TRUE(set_res.has_value()) << set_res.error().message();
    EXPECT_EQ(set_res.value().status, cinder::Errc::OK);

    cinder::net::Request get_req{.opcode = cinder::net::Opcode::Get, .key = "tls-key"};

    auto get_res = rawTlsRequest(port, get_req, *ssl_ctx_);
    ASSERT_TRUE(get_res.has_value()) << get_res.error().message();
    EXPECT_EQ(get_res.value().status, cinder::Errc::OK);
    ASSERT_TRUE(get_res.value().value.has_value());
    EXPECT_EQ(*get_res.value().value, "tls-value");

    stopDaemon(dpid);
}

TEST_F(TlsIntegrationTest, PingOverTls) {
    const uint16_t port = cinder::net::test::pickHeldPort();
    ASSERT_NE(port, 0);
    pid_t dpid = spawnTlsDaemon(port);
    ASSERT_TRUE(waitForTlsNode(port, *ssl_ctx_));

    cinder::net::Request ping_req{.opcode = cinder::net::Opcode::Ping};
    auto res = rawTlsRequest(port, ping_req, *ssl_ctx_);
    ASSERT_TRUE(res.has_value()) << res.error().message();
    EXPECT_EQ(res.value().status, cinder::Errc::OK);

    stopDaemon(dpid);
}

TEST_F(TlsIntegrationTest, PlaintextRejected) {
    // Server with TLS enabled should reject plaintext connections
    const uint16_t port = cinder::net::test::pickHeldPort();
    ASSERT_NE(port, 0);
    pid_t dpid = spawnTlsDaemon(port);
    ASSERT_TRUE(waitForTlsNode(port, *ssl_ctx_));

    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
    ASSERT_FALSE(ec) << ec.message();

    // Send a raw (unencrypted) frame — the server expects a TLS ClientHello
    cinder::net::Request ping_req{.opcode = cinder::net::Opcode::Ping};
    auto encoded = cinder::net::encode(ping_req);
    ASSERT_TRUE(encoded.has_value());
    (void)write(socket, buffer(encoded.value()), ec);

    // Read should fail — server closes the connection
    std::array<std::byte, 64> buf{};
    (void)read(socket, buffer(buf), ec);
    // Either read fails or we get garbage — either way, no valid response
    EXPECT_TRUE(ec || buf[0] != std::byte{cinder::net::K_MAGIC});

    socket.close();
    stopDaemon(dpid);
}
} // namespace

#endif // CINDER_ENABLE_TLS
