#pragma once

#include <asio.hpp>
#include <chrono>
#include <cstring>
#include <csignal>
#include <gtest/gtest.h>
#include <span>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>

#include "cinder/net/protocol.hpp"

using asio::ip::tcp;

namespace cinder::net::test {

// Integration-test ports (distinct per test file to avoid collisions when the
// two integration binaries run concurrently, and unique across the suite).
inline constexpr int K_PORT_NODE1 = 17'910;
inline constexpr int K_PORT_NODE2 = 17'911;
inline constexpr int K_PORT_NODE3 = 17'912;
inline constexpr int K_PORT_RB_NODE1 = 17'930;
inline constexpr int K_PORT_RB_NODE2 = 17'931;
inline constexpr int K_PORT_RB_NODE3 = 17'932;

[[maybe_unused]] static auto
readResponse(asio::ip::tcp::socket& socket) -> Result<Response> {
    std::array<std::byte, 65'536> buf{};
    asio::error_code ec;
    (void)asio::read(socket, asio::buffer(buf.data(), K_FRAME_HEADER_SIZE), ec);
    if (ec) {
        return err<Response>(Error(Errc::InternalError, "read header failed"));
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &buf[3], sizeof(net_len));
    size_t payload_len = std::byteswap(net_len);
    if (payload_len > buf.size() - K_FRAME_HEADER_SIZE) {
        return err<Response>(Error(Errc::InvalidArgument, "response too large"));
    }
    if (payload_len > 0) {
        (void)asio::read(socket,
            asio::buffer(buf.data() + K_FRAME_HEADER_SIZE, payload_len), ec);
        if (ec) {
            return err<Response>(Error(Errc::InternalError, "read payload failed"));
        }
    }
    return decodeResponse(
        std::span<const std::byte>(buf.data(), K_FRAME_HEADER_SIZE + payload_len));
}

[[maybe_unused]] static auto
waitForPort(int port, int max_retries = 50) -> bool {
    asio::io_context io;
    for (int i = 0; i < max_retries; i++) {
        asio::ip::tcp::socket sock(io);
        asio::error_code ec;
        sock.connect(asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
        if (!ec) {
            sock.close();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

// ---- Shared multi-process node harness (fork/exec real cinderd) ----

struct NodeProc {
    pid_t pid = -1;
    int port = 0;
    std::string id;
};

[[maybe_unused]] static auto
spawnNode(int port, const std::string& id, const std::string& peer_list, bool quorum = false,
    int replica_factor = 1, int quarantine_interval_ms = 10'000) -> NodeProc {
    auto port_str = std::to_string(port);
    auto factor_str = std::to_string(replica_factor);
    auto quarantine_str = std::to_string(quarantine_interval_ms);
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
            factor_str.c_str(),
            "--consistency",
            quorum ? "quorum" : "async",
            "--peers",
            peer_list.c_str(),
            "--quarantine-interval",
            quarantine_str.c_str(),
            nullptr);
        _exit(1);
    }
    return {pid, port, id};
}

// Terminate a spawned node; SIGKILL fallback if it does not exit within the
// deadline so tests can never deadlock on shutdown.
[[maybe_unused]] static void
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

    [[nodiscard]] auto proc() const -> const NodeProc& { return node_; }

  private:

    NodeProc node_;
};

[[maybe_unused]] static auto
rawRequest(int port, const Request& req) -> cinder::Result<Response> {
    asio::io_context io;
    tcp::socket socket(io);
    asio::error_code ec;
    socket.connect(tcp::endpoint(asio::ip::address_v4::loopback(), port), ec);
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

[[maybe_unused]] static auto
setKey(int port, const std::string& key, const std::string& value) -> cinder::Result<Response> {
    Request req{.opcode = Opcode::Set, .key = key, .value = value, .ttl = std::nullopt};
    return rawRequest(port, req);
}

[[maybe_unused]] static auto
getKey(int port, const std::string& key) -> cinder::Result<Response> {
    Request req{.opcode = Opcode::Get, .key = key, .value = {}, .ttl = std::nullopt};
    return rawRequest(port, req);
}

// Poll a node until it returns the expected value (async fan-out/replay is not
// instant).
[[maybe_unused]] static auto
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
} // namespace cinder::net::test
