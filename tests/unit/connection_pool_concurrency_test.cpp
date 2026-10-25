// §1.1: ConnectionPool must not serialise concurrent RPCs to one peer behind a
// single connection.
//
// The protocol is not multiplexed -- one socket carries one request/response at
// a time, and each NodeConn's strand enforces that. With ONE connection per
// peer every concurrent caller queued behind the in-flight RPC, so N callers
// each paid a full RTT of head-of-line blocking (total ~N x delay). The pool
// now holds K_POOL_SIZE connections with distinct strands, so callers spread
// across them overlap.
//
// The test drives a stub server that delays every response by a fixed amount,
// then issues N concurrent sends and asserts wall-clock is far below the
// fully-serialised N x delay. It is written to FAIL against a one-connection
// pool (the old behaviour), not merely to pass against the new one.
#include <atomic>
#include <chrono>
#include <cstddef>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <thread>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/net/protocol.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::ip::address_v4;
using asio::ip::tcp;
using std::chrono::milliseconds;

namespace cinder {
    namespace {

        // Stub server: accept many concurrent connections; for each request
        // read the frame, wait `delay`, then reply OK. Delay is what makes the
        // serialisation observable.
        class DelayingServer {
        public:

            DelayingServer(io_context& io, uint16_t port, milliseconds delay) :
                io_(io),
                acceptor_(io, tcp::endpoint(address_v4::loopback(), port)),
                delay_(delay) {}

            void start() {
                acceptNext();
            }

            void stop() {
                error_code ec;
                acceptor_.close(ec);
            }

            auto accepted() const -> size_t {
                return accepted_.load();
            }

        private:

            void acceptNext() {
                auto sock = std::make_shared<tcp::socket>(io_);
                acceptor_.async_accept(*sock, [this, sock](error_code ec) {
                    if (ec) {
                        return;
                    }
                    accepted_.fetch_add(1);
                    serve(sock);
                    acceptNext();
                });
            }

            void serve(const std::shared_ptr<tcp::socket>& sock) {
                auto header = std::make_shared<std::array<std::byte, net::K_FRAME_HEADER_SIZE>>();
                asio::async_read(
                    *sock,
                    buffer(*header),
                    [this, sock, header](error_code ec, size_t) {
                    if (ec) {
                        return;
                    }
                    // Read the declared payload, then reply after the delay.
                    uint32_t len = 0;
                    std::memcpy(&len, &(*header)[3], sizeof(len));
                    len = std::byteswap(len);
                    auto payload =
                        std::make_shared<std::vector<std::byte>>(static_cast<size_t>(len));
                    asio::async_read(
                        *sock,
                        buffer(*payload),
                        [this, sock, payload](error_code pec, size_t) {
                        if (pec) {
                            return;
                        }
                        // Delay the response, then answer. The timer keeps the
                        // io_context busy for exactly delay_.
                        auto timer = std::make_shared<asio::steady_timer>(io_, delay_);
                        timer->async_wait([sock, timer](error_code tec) {
                            if (tec) {
                                return;
                            }
                            auto resp = net::Response{.status = Errc::OK, .value = std::nullopt};
                            auto encoded = net::encode(resp);
                            if (!encoded) {
                                return;
                            }
                            error_code wec;
                            asio::write(*sock, buffer(*encoded), wec);
                            // Keep the connection open and serve the next
                            // request on it (mirrors the real server).
                            auto next_header =
                                std::make_shared<std::array<std::byte, net::K_FRAME_HEADER_SIZE>>();
                            asio::async_read(
                                *sock,
                                buffer(*next_header),
                                [](error_code, size_t) { /* drained on close */ }
                            );
                        });
                    }
                    );
                }
                );
            }

            io_context& io_;
            tcp::acceptor acceptor_;
            milliseconds delay_;
            std::atomic<size_t> accepted_{0};
        };

        TEST(ConnectionPoolConcurrencyTest, ConcurrentSendsToSamePeerOverlap) {
            constexpr uint16_t K_PORT = 17'899;
            constexpr milliseconds K_DELAY{400};
            constexpr size_t K_CALLERS = 4;

            io_context io;
            DelayingServer server(io, K_PORT, K_DELAY);
            server.start();

            // The pool's io_context needs its own thread while the test thread
            // issues blocking send() calls.
            auto work = asio::make_work_guard(io);
            std::thread io_thread([&io] { io.run(); });

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", K_PORT});
            config.rpc_timeout_ms = 10'000;

            ConnectionPool pool(config, io);

            std::atomic<size_t> ok_count{0};
            const auto start = std::chrono::steady_clock::now();

            std::vector<std::thread> callers;
            callers.reserve(K_CALLERS);
            for (size_t i = 0; i < K_CALLERS; ++i) {
                callers.emplace_back([&pool, &ok_count, i] {
                    net::Request req{
                        .opcode = net::Opcode::Set,
                        .key = "k" + std::to_string(i),
                        .value = "v",
                    };
                    auto res = pool.send("node1", req);
                    if (res.has_value() && res->status == Errc::OK) {
                        ok_count.fetch_add(1);
                    }
                });
            }
            for (auto& t : callers) {
                t.join();
            }

            const auto elapsed =
                std::chrono::duration_cast<milliseconds>(std::chrono::steady_clock::now() - start);

            work.reset();
            pool.shutdown();
            io.stop();
            io_thread.join();
            server.stop();

            ASSERT_EQ(ok_count.load(), K_CALLERS) << "all RPCs should succeed";

            // Fully serialised, K_CALLERS RPCs would take ~K_CALLERS * K_DELAY
            // (=1600ms). With K_POOL_SIZE=4 and 4 callers they should overlap
            // and finish near a single delay. Generous bound: well under the
            // serialised cost but tolerant of CI scheduling noise.
            const auto serialised = K_DELAY * static_cast<int>(K_CALLERS / 2);
            EXPECT_LT(elapsed, serialised)
                << "concurrent RPCs appear serialised: "
                << elapsed.count()
                << "ms for "
                << K_CALLERS
                << " callers at "
                << K_DELAY.count()
                << "ms each (serialised would be ~"
                << (K_DELAY * K_CALLERS).count()
                << "ms)";

            // The pool must actually have opened more than one connection.
            EXPECT_GE(server.accepted(), 2U) << "expected multiple connections to the same peer";
        }
    } // namespace
} // namespace cinder
