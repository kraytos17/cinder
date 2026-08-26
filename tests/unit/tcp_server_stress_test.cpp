#include <asio.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/net/tcp_server.hpp"
#include "cinder/store/lru_store.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::ip::address_v4;
using asio::ip::tcp;
using cinder::net::Opcode;
using cinder::net::Request;
using std::chrono::milliseconds;

namespace cinder {
namespace {

// Port range for stress tests — avoids overlap with integration/CLI suites.
constexpr uint16_t K_STRESS_BASE = 17'860;
constexpr int K_ITERATIONS = 50;
constexpr int K_CONCURRENT_CONNS = 4;

auto
sendSet(tcp::socket& sock, const std::string& key, const std::string& value) -> bool {
    Request req{.opcode = Opcode::Set, .key = key, .value = value, .ttl = std::nullopt};
    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        return false;
    }
    error_code ec;
    asio::write(sock, buffer(encoded.value()), ec);
    return !ec;
}

// Rapid start/stop loop with concurrent connections hitting the server while
// it shuts down.  The race under test: doAccept() completion vs shutdown()
// closing the acceptor.  Before the strand fix this would produce EBADF
// warnings and TSan data-race reports.
TEST(TcpServerStrandStress, RapidStartStopWithConcurrentConnections) {
    cinder::RealClock clock;
    cinder::ConsistentHashRing ring;
    ring.addNode("node1");
    io_context io(4);
    int errors = 0;

    for (int i = 0; i < K_ITERATIONS; ++i) {
        uint16_t port = K_STRESS_BASE + static_cast<uint16_t>(i % 20);
        cinder::LruStore store{1'024, &clock};
        cinder::net::TcpServer server(io, port, store, ring, "node1", clock);

        auto start_res = server.start();
        ASSERT_TRUE(start_res.has_value()) << "iteration " << i;

        // Fire concurrent connections that send SET then immediately close.
        std::vector<std::thread> clients;
        clients.reserve(K_CONCURRENT_CONNS);
        for (int c = 0; c < K_CONCURRENT_CONNS; ++c) {
            clients.emplace_back([&io, port, c, i]() {
                error_code ec;
                tcp::socket sock(io);
                sock.connect(tcp::endpoint(address_v4::loopback(), port), ec);
                if (ec) {
                    return;
                }

                auto key = "stress-" + std::to_string(i) + "-" + std::to_string(c);
                sendSet(sock, key, "v");
                error_code close_ec;
                sock.close(close_ec);
            });
        }

        // Shutdown immediately — concurrent with in-flight connections.
        server.shutdown();
        for (auto& t : clients) {
            t.join();
        }

        // Let the io_context drain pending handlers.
        io.restart();
        while (io.poll_one() > 0) {
        }
    }

    EXPECT_EQ(errors, 0);
}

// Shutdown while connections are mid-request (read in progress on server side).
// Forces the server to tear down connections that have partially-written data.
TEST(TcpServerStrandStress, ShutdownMidRequest) {
    cinder::RealClock clock;
    cinder::ConsistentHashRing ring;
    ring.addNode("node1");
    io_context io(4);

    for (int i = 0; i < K_ITERATIONS; ++i) {
        uint16_t port = K_STRESS_BASE + 20 + static_cast<uint16_t>(i % 20);
        cinder::LruStore store{1'024, &clock};
        cinder::net::TcpServer server(io, port, store, ring, "node1", clock);
        auto start_res = server.start();
        ASSERT_TRUE(start_res.has_value());

        // Open a connection but write only a partial frame, then shut down.
        std::thread client([&io, port]() {
            error_code ec;
            tcp::socket sock(io);
            sock.connect(tcp::endpoint(address_v4::loopback(), port), ec);
            if (ec) {
                return;
            }
            // Write just the magic + opcode (2 bytes) — not a complete frame.
            std::array<std::byte, 2> partial{std::byte{0x43}, std::byte{0x01}};
            asio::write(sock, buffer(partial), ec);
            std::this_thread::sleep_for(milliseconds(10));
            error_code close_ec;
            sock.close(close_ec);
        });

        // Brief pause to let the partial frame arrive, then tear down.
        std::this_thread::sleep_for(milliseconds(2));
        server.shutdown();
        client.join();

        io.restart();
        while (io.poll_one() > 0) {
        }
    }
}

// Multiple servers on different ports, all starting and stopping concurrently.
// Uses a dedicated io thread with work_guard so that ALL strand-dispatched
// handlers (including shutdown lambdas) complete before we destroy servers.
TEST(TcpServerStrandStress, ConcurrentServersShutdown) {
    constexpr int K_SERVERS = 8;
    constexpr int K_ROUNDS = 20;

    cinder::RealClock clock;
    cinder::ConsistentHashRing ring;
    ring.addNode("node1");

    for (int round = 0; round < K_ROUNDS; ++round) {
        io_context io;
        auto work = asio::make_work_guard(io);
        std::thread io_thread([&io]() { io.run(); });

        std::vector<std::unique_ptr<cinder::LruStore>> stores;
        std::vector<std::unique_ptr<cinder::net::TcpServer>> servers;

        for (int s = 0; s < K_SERVERS; ++s) {
            uint16_t port =
                K_STRESS_BASE + 60 + static_cast<uint16_t>((round * K_SERVERS + s) % 40);
            auto store = std::make_unique<cinder::LruStore>(1'024, &clock);
            auto server =
                std::make_unique<cinder::net::TcpServer>(io, port, *store, ring, "node1", clock);
            stores.push_back(std::move(store));
            servers.push_back(std::move(server));
            ASSERT_TRUE(servers.back()->start().has_value());
        }

        // Fire one connection per server.
        std::vector<std::thread> clients;
        for (int s = 0; s < K_SERVERS; ++s) {
            uint16_t port =
                K_STRESS_BASE + 60 + static_cast<uint16_t>((round * K_SERVERS + s) % 40);
            clients.emplace_back([&io, port, round, s]() {
                error_code ec;
                tcp::socket sock(io);
                sock.connect(tcp::endpoint(address_v4::loopback(), port), ec);
                if (ec) {
                    return;
                }
                auto key = "multi-" + std::to_string(round) + "-" + std::to_string(s);
                sendSet(sock, key, "v");
                error_code close_ec;
                sock.close(close_ec);
            });
        }

        for (auto& t : clients) {
            t.join();
        }

        // Shutdown all servers — posts lambdas to each server's strand.
        for (auto& s : servers) {
            s->shutdown();
        }

        // Release work guard and join — guarantees ALL posted handlers
        // (including the shutdown lambdas that capture `this`) run to
        // completion before we destroy the servers below.
        work.reset();
        io_thread.join();

        servers.clear();
        stores.clear();
    }
}
} // namespace
} // namespace cinder
