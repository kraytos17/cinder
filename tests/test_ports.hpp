#pragma once

// Test port allocation.
//
// CTest runs every case as its own process (`gtest_discover_tests`), so
// parallel workers share the loopback namespace: two suites binding the same
// fixed port fail with EADDRINUSE (observed: rpc_timeout's 17880-17889 vs the
// stress suite's 17880-17899). Nothing here uses fixed ports — every port is
// kernel-assigned, hence collision-free by construction.
//
// Two patterns:
//   - Held ports (pickHeldPort/takeHeldFd): bind :0 and hold the socket so
//     the port cannot be stolen before the server adopts the fd via
//     TcpServer::setListenFd or CacheNodeServerOptions::listen_fd. No rebind,
//     so no TOCTOU window at all.
//   - Direct bind (bindEphemeral): for test doubles that own their acceptor.
//     The same acceptor stays bound; the test reads the assigned port back.
//
// The held-fd registry lives behind a function-local static: it is built on
// first test use (never during dynamic initialization before main), so a
// throwing constructor stays catchable by the caller. A namespace-scope
// equivalent trips CERT ERR58-CPP ("initialization with static storage
// duration may throw an exception that cannot be caught").

#include <ankerl/unordered_dense.h>
#include <asio.hpp>
#include <cstdint>
#include <mutex>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace cinder::net::test {

    // Sockets held for imminently-binding servers, keyed by port.
    namespace held_detail {
        struct Registry {
            std::mutex mutex;
            ankerl::unordered_dense::map<uint16_t, int> fds;
        };

        inline auto registry() -> Registry& {
            static Registry instance;
            return instance;
        }
    } // namespace held_detail

    // Bind loopback :0 and hold the socket; returns the assigned port, 0 on
    // failure. The holder must consume the fd via takeHeldFd and hand it to
    // the server (TcpServer::setListenFd); an unclaimed socket is closed at
    // process exit. Raw POSIX socket on purpose: asio sets CLOEXEC, which
    // would close the fd across exec before the daemon can adopt it.
    [[maybe_unused]]
    static auto pickHeldPort() -> uint16_t {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return 0;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        socklen_t len = sizeof(addr);
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr))
            != 0
            || ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len)
            != 0) {
            ::close(fd);
            return 0;
        }

        uint16_t port = ntohs(addr.sin_port);
        auto& reg = held_detail::registry();
        std::scoped_lock lock(reg.mutex);
        reg.fds.insert_or_assign(port, fd);
        return port;
    }

    // Take the held socket for a picked port (unregisters it). Returns -1
    // when the port was not picked — the binder then binds normally.
    [[maybe_unused]]
    static auto takeHeldFd(uint16_t port) -> int {
        auto& reg = held_detail::registry();
        std::scoped_lock lock(reg.mutex);
        auto it = reg.fds.find(port);
        if (it == reg.fds.end()) {
            return -1;
        }

        int fd = it->second;
        reg.fds.erase(it);
        return fd;
    }

    // Bind `acc` to loopback :0 and listen; returns the assigned port, 0 on
    // error. The acceptor stays bound: callers read the port back and use this
    // same acceptor, so there is no pick-then-bind race.
    [[maybe_unused]]
    static auto bindEphemeral(asio::ip::tcp::acceptor& acc) -> uint16_t {
        asio::error_code ec;
        acc.open(asio::ip::tcp::v4(), ec);
        if (ec) {
            return 0;
        }

        acc.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
        acc.bind(asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0), ec);
        if (ec) {
            return 0;
        }

        acc.listen(asio::socket_base::max_listen_connections, ec);
        if (ec) {
            return 0;
        }

        auto ep = acc.local_endpoint(ec);
        if (ec) {
            return 0;
        }
        return ep.port();
    }
} // namespace cinder::net::test
