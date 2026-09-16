#pragma once

#include <asio.hpp>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <mutex>
#include <netinet/in.h>
#include <span>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <utility>

#include "cinder/net/protocol.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::read;
using asio::write;
using asio::ip::address_v4;
using asio::ip::tcp;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder::net::test {

// Pick a currently-free loopback port: bind :0, read back the assignment,
// close. Returns uint16_t so the result feeds NodeConfig braced-init
// without narrowing. Best-effort — another process could bind it first,
// so callers must verify identity after spawn (waitForNode).
[[maybe_unused]] static auto
pickEphemeralPort() -> uint16_t {
    io_context io;
    tcp::acceptor acc(io);
    error_code ec;
    acc.open(tcp::v4(), ec);
    if (ec) {
        return 0;
    }
    acc.set_option(tcp::acceptor::reuse_address(true), ec);
    acc.bind(tcp::endpoint(address_v4::loopback(), 0), ec);
    if (ec) {
        return 0;
    }
    auto ep = acc.local_endpoint(ec);
    if (ec) {
        return 0;
    }
    return ep.port();
}

// Sockets held for imminently-spawning daemons, keyed by port. pickHeldPort
// binds+holds; the spawner for that port consumes the fd via takeHeldFd and
// passes it as --listen-fd. Mutex-guarded. Entries live until consumed: a
// test that picks without spawning leaks one bound socket until process
// exit — bounded and harmless, and every call site spawns what it picks.
// Per-TU instances (header statics): pick and spawn for any given daemon
// always happen in the same translation unit, so no sharing is needed.
namespace held_detail {
inline std::mutex mutex;
inline std::unordered_map<uint16_t, int> fds;
} // namespace held_detail

[[maybe_unused]] static auto
pickHeldPort() -> uint16_t {
    // Raw POSIX socket on purpose: asio sets CLOEXEC, which would close the
    // fd across exec before the daemon can adopt it.
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    socklen_t len = sizeof(addr);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0
        || ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }

    uint16_t port = ntohs(addr.sin_port);
    std::scoped_lock lock(held_detail::mutex);
    held_detail::fds.insert_or_assign(port, fd);
    return port;
}

// Take the held socket for a picked port (unregisters it). Returns -1 when
// the port was not picked — the spawner then binds normally.
[[maybe_unused]] static auto
takeHeldFd(uint16_t port) -> int {
    std::scoped_lock lock(held_detail::mutex);
    auto it = held_detail::fds.find(port);
    if (it == held_detail::fds.end()) {
        return -1;
    }

    int fd = it->second;
    held_detail::fds.erase(it);
    return fd;
}

[[maybe_unused]] static auto
readResponse(tcp::socket& socket) -> Result<Response> {
    std::array<std::byte, 65'536> buf{};
    error_code ec;
    (void)read(socket, buffer(buf.data(), K_FRAME_HEADER_SIZE), ec);
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
        (void)read(socket, buffer(buf.data() + K_FRAME_HEADER_SIZE, payload_len), ec);
        if (ec) {
            return err<Response>(Error(Errc::InternalError, "read payload failed"));
        }
    }
    return decodeResponse(
        std::span<const std::byte>(buf.data(), K_FRAME_HEADER_SIZE + payload_len));
}

struct NodeProc {
    pid_t pid = -1;
    int port = 0;
    std::string id;
};

[[maybe_unused]] static auto
spawnNode(int port, const std::string& id, const std::string& peer_list, bool quorum = false,
    int replica_factor = 1, int quarantine_interval_ms = 10'000, int suspect_timeout_ms = 3'000,
    int ping_interval_ms = 1'000) -> NodeProc {
    auto port_str = std::to_string(port);
    auto factor_str = std::to_string(replica_factor);
    auto quarantine_str = std::to_string(quarantine_interval_ms);
    auto suspect_str = std::to_string(suspect_timeout_ms);
    auto ping_str = std::to_string(ping_interval_ms);
    // Adopt the held socket when this port was picked via pickHeldPort, so
    // the daemon never binds a port another process could have stolen.
    // Looked up before fork: the child only execs, never touches the map.
    int held_fd = takeHeldFd(static_cast<uint16_t>(port));
    auto fd_str = std::to_string(held_fd);
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
            "--suspect-timeout",
            suspect_str.c_str(),
            "--ping-interval",
            ping_str.c_str(),
            "--listen-fd",
            fd_str.c_str(),
            nullptr);
        _exit(1);
    }
    if (held_fd >= 0) {
        // The child has its own copy, which survives exec into the daemon.
        ::close(held_fd);
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
    auto deadline = steady_clock::now() + seconds(2);
    while (steady_clock::now() < deadline) {
        pid_t r = waitpid(node.pid, &status, WNOHANG);
        if (r == node.pid) {
            return;
        }
        std::this_thread::sleep_for(milliseconds(50));
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
    io_context io;
    tcp::socket socket(io);
    error_code ec;
    socket.connect(tcp::endpoint(address_v4::loopback(), port), ec);
    if (ec) {
        return cinder::err<Response>(cinder::Error(cinder::Errc::InternalError, "connect failed"));
    }

    struct timeval tv{};
    tv.tv_sec = 5;
    setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    auto encoded = cinder::net::encode(req);
    if (!encoded.has_value()) {
        return cinder::err<Response>(encoded.error());
    }

    (void)write(socket, buffer(encoded.value()), ec);
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
waitForValue(int port, const std::string& key, const std::string& expected, int max_attempts = 100)
    -> bool {
    for (int i = 0; i < max_attempts; i++) {
        auto res = getKey(port, key);
        if (res.has_value() && res.value().status == Errc::OK && res.value().value.has_value()
            && *res.value().value == expected) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(100));
    }
    return false;
}

// Wait until the daemon answering on `port` identifies as node `id` bound to
// `port` (AdminInfo). Unlike a bare connect check, this rejects stale daemons
// left behind by earlier runs and cross-talk from other tests' daemons — the
// failure mode fixed ports made likely. Required companion to
// pickEphemeralPort.
[[maybe_unused]] static auto
waitForNode(int port, const std::string& id, int max_retries = 50) -> bool {
    for (int i = 0; i < max_retries; i++) {
        Request req{.opcode = Opcode::AdminInfo, .key = {}, .value = {}};
        auto res = rawRequest(port, req);
        if (res.has_value() && res.value().status == Errc::OK && res.value().value.has_value()
            && res.value().value->contains(R"("node_id":")" + id + "\"")
            && res.value().value->contains("\"port\":" + std::to_string(port))) {
            return true;
        }
        std::this_thread::sleep_for(milliseconds(100));
    }
    return false;
}
} // namespace cinder::net::test
