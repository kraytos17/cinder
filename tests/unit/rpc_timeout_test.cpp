#include <asio.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/clock.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/net/tcp_transport.hpp"
#include "test_ports.hpp"

using asio::buffer;
using asio::error_code;
using asio::io_context;
using asio::ip::tcp;
using cinder::net::Opcode;
using cinder::net::Request;
using std::chrono::milliseconds;

namespace cinder {
    namespace {

        // A TCP server that accepts connections but never responds — used to trigger
        // the RPC deadline timer. Binds a kernel-assigned ephemeral port (see
        // test_ports.hpp) so parallel workers can never collide on it.
        class BlackHoleServer {
        public:

            explicit BlackHoleServer(io_context& io) :
                acceptor_(io) {
                port_ = net::test::bindEphemeral(acceptor_);
            }

            [[nodiscard]]
            auto port() const -> uint16_t {
                return port_;
            }

            void start() {
                doAccept();
            }

            void stop() {
                error_code ec;
                acceptor_.close(ec);
                for (auto& sock : sockets_) {
                    sock.close(ec);
                }
            }

        private:

            void doAccept() {
                auto& sock = sockets_.emplace_back(acceptor_.get_executor());
                acceptor_.async_accept(sock, [this](error_code ec) {
                    if (!ec) {
                        // Connection accepted — do nothing. Let the client time out.
                        doAccept();
                    }
                });
            }

            tcp::acceptor acceptor_;
            uint16_t port_ = 0;
            std::vector<tcp::socket> sockets_;
        };

        TEST(RpcTimeout, RequestTimesOutWhenServerDoesNotRespond) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});

            TcpTransport transport(io);
            transport.setConfig(config);
            transport.setRpcTimeout(K_TIMEOUT);

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            transport.sendRequestAsync("node1", req, [&](Result<net::Response> res) {
                callback_called = true;
                EXPECT_FALSE(res.has_value());
                EXPECT_EQ(res.error().code(), Errc::Timeout);
                io.stop();
            });

            io.run();
            server.stop();
            EXPECT_TRUE(callback_called);
        }

        TEST(RpcTimeout, FastRpcCompletesBeforeDeadline) {
            // A fast server that responds immediately — the deadline must NOT fire.
            constexpr auto K_TIMEOUT = milliseconds(5000);

            io_context io;
            tcp::acceptor acceptor(io);
            const uint16_t k_port = net::test::bindEphemeral(acceptor);
            ASSERT_NE(k_port, 0);

            // Server loop: accept, read request, send a valid response.
            std::function<void()> do_accept;
            do_accept = [&] {
                auto sock = std::make_shared<tcp::socket>(io);
                acceptor.async_accept(*sock, [&, sock](error_code ec) {
                    if (ec) {
                        return;
                    }

                    // Read the request in the background (ignore content).
                    auto buf = std::make_shared<std::vector<std::byte>>(4096);
                    sock->async_read_some(buffer(*buf), [sock, buf](error_code, size_t) {
                        auto resp = net::Response{.status = Errc::OK, .value = std::nullopt};
                        auto encoded = net::encode(resp);
                        if (encoded) {
                            error_code wec;
                            asio::write(*sock, buffer(*encoded), wec);
                        }
                    });
                    do_accept();
                });
            };
            do_accept();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});

            TcpTransport transport(io);
            transport.setConfig(config);
            transport.setRpcTimeout(K_TIMEOUT);

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            transport.sendRequestAsync("node1", req, [&](Result<net::Response> res) {
                callback_called = true;
                EXPECT_TRUE(res.has_value());
                EXPECT_EQ(res->status, Errc::OK);
                io.stop();
            });

            io.run();
            error_code ec;
            acceptor.close(ec);
            EXPECT_TRUE(callback_called);
        }

        TEST(PoolRpcTimeout, SingleRequestTimesOutAgainstBlackHole) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = static_cast<int>(K_TIMEOUT.count());

            ConnectionPool pool(config, io);

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            pool.sendAsync("node1", req, [&](Result<net::Response> res) {
                callback_called = true;
                EXPECT_FALSE(res.has_value());
                EXPECT_EQ(res.error().code(), Errc::Timeout);
                io.stop();
            });

            io.run();
            server.stop();
            EXPECT_TRUE(callback_called);
        }

        TEST(PoolRpcTimeout, BatchRequestTimesOutAgainstBlackHole) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = static_cast<int>(K_TIMEOUT.count());

            ConnectionPool pool(config, io);

            std::vector<Request> reqs;
            reqs.push_back({.opcode = Opcode::Get, .key = "k1", .value = {}, .ttl = std::nullopt});
            reqs.push_back({.opcode = Opcode::Get, .key = "k2", .value = {}, .ttl = std::nullopt});

            bool callback_called = false;
            pool.sendBatchAsync("node1", reqs, [&](Result<std::vector<net::Response>> res) {
                callback_called = true;
                EXPECT_FALSE(res.has_value());
                EXPECT_EQ(res.error().code(), Errc::Timeout);
                io.stop();
            });

            io.run();
            server.stop();
            EXPECT_TRUE(callback_called);
        }

        TEST(PoolRpcTimeout, NoDeadlineWhenZero) {
            // rpc_timeout_ms = 0 means disabled — should NOT time out.

            io_context io;
            tcp::acceptor acceptor(io);
            const uint16_t k_port = net::test::bindEphemeral(acceptor);
            ASSERT_NE(k_port, 0);

            std::function<void()> do_accept;
            do_accept = [&] {
                auto sock = std::make_shared<tcp::socket>(io);
                acceptor.async_accept(*sock, [&, sock](error_code ec) {
                    if (ec) {
                        return;
                    }

                    auto buf = std::make_shared<std::vector<std::byte>>(4096);
                    sock->async_read_some(buffer(*buf), [sock, buf](error_code, size_t) {
                        auto resp = net::Response{.status = Errc::OK, .value = std::nullopt};
                        auto encoded = net::encode(resp);
                        if (encoded) {
                            error_code wec;
                            asio::write(*sock, buffer(*encoded), wec);
                        }
                    });
                    do_accept();
                });
            };
            do_accept();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = 0;

            ConnectionPool pool(config, io);

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            pool.sendAsync("node1", req, [&](Result<net::Response> res) {
                callback_called = true;
                EXPECT_TRUE(res.has_value());
                EXPECT_EQ(res->status, Errc::OK);
                io.stop();
            });

            io.run();
            error_code ec;
            acceptor.close(ec);
            EXPECT_TRUE(callback_called);
        }

        // Span outcome assertions for the Phase 4 transport/pool coverage.
        // The recording subscriber observes every exchange span; a black hole
        // must report Timeout, a fast peer Ok.

        struct StatusGuard {
            explicit StatusGuard(std::shared_ptr<RecordingSubscriber> sub) :
                prev_(globalSubscriber()) {
                setGlobalSubscriber(std::move(sub));
            }

            ~StatusGuard() {
                setGlobalSubscriber(prev_);
            }

            std::shared_ptr<Subscriber> prev_;
        };

        auto findOp(
            const std::vector<RecordingSubscriber::RecordedSpan>& spans,
            std::string_view operation
        ) -> const RecordingSubscriber::RecordedSpan* {
            for (const auto& s : spans) {
                if (s.operation == operation) {
                    return &s;
                }
            }
            return nullptr;
        }

        auto findAttr(const RecordingSubscriber::RecordedSpan& span, std::string_view key)
            -> const RecordingSubscriber::RecordedField* {
            for (const auto& a : span.attributes) {
                if (a.key == key) {
                    return &a;
                }
            }
            return nullptr;
        }

        TEST(PoolRpcTimeout, TimeoutMarksSpan) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = static_cast<int>(K_TIMEOUT.count());

            ConnectionPool pool(config, io);
            auto sub = std::make_shared<RecordingSubscriber>();

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            {
                StatusGuard guard(sub);
                pool.sendAsync("node1", req, [&](Result<net::Response> res) {
                    callback_called = true;
                    EXPECT_FALSE(res.has_value());
                    io.stop();
                });

                io.run();
            }
            server.stop();
            EXPECT_TRUE(callback_called);

            auto spans = sub->spans();
            const auto* span = findOp(spans, "pool.send");
            ASSERT_NE(span, nullptr);
            EXPECT_EQ(span->status, SpanStatus::Timeout);
            EXPECT_TRUE(span->closed);
            // The black hole accepts then goes silent: resolve/connect/write
            // succeed, the exchange dies waiting for the response header.
            const auto* phase = findAttr(*span, "phase");
            ASSERT_NE(phase, nullptr);
            EXPECT_EQ(phase->str_val, "read header");
            const auto* peer = findAttr(*span, "peer");
            ASSERT_NE(peer, nullptr);
            EXPECT_EQ(peer->str_val, "127.0.0.1");
            const auto* reused = findAttr(*span, "reused");
            ASSERT_NE(reused, nullptr);
            EXPECT_FALSE(reused->bool_val);
            const auto* bytes_out = findAttr(*span, "bytes_out");
            ASSERT_NE(bytes_out, nullptr);
            EXPECT_GT(bytes_out->uint_val, 0U);
        }

        TEST(PoolRpcTimeout, SuccessMarksSpanOk) {
            io_context io;
            tcp::acceptor acceptor(io);
            const uint16_t k_port = net::test::bindEphemeral(acceptor);
            ASSERT_NE(k_port, 0);

            std::function<void()> do_accept;
            do_accept = [&] {
                auto sock = std::make_shared<tcp::socket>(io);
                acceptor.async_accept(*sock, [&, sock](error_code ec) {
                    if (ec) {
                        return;
                    }

                    auto buf = std::make_shared<std::vector<std::byte>>(4096);
                    sock->async_read_some(buffer(*buf), [sock, buf](error_code, size_t) {
                        auto resp = net::Response{.status = Errc::OK, .value = std::nullopt};
                        auto encoded = net::encode(resp);
                        if (encoded) {
                            error_code wec;
                            asio::write(*sock, buffer(*encoded), wec);
                        }
                    });
                    do_accept();
                });
            };
            do_accept();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = 5000;

            ConnectionPool pool(config, io);
            auto sub = std::make_shared<RecordingSubscriber>();

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            {
                StatusGuard guard(sub);
                pool.sendAsync("node1", req, [&](Result<net::Response> res) {
                    callback_called = true;
                    EXPECT_TRUE(res.has_value());
                    io.stop();
                });

                io.run();
            }
            error_code ec;
            acceptor.close(ec);
            EXPECT_TRUE(callback_called);

            auto spans = sub->spans();
            const auto* span = findOp(spans, "pool.send");
            ASSERT_NE(span, nullptr);
            EXPECT_EQ(span->status, SpanStatus::Ok);
            // Success carries sizes but no failure phase.
            EXPECT_EQ(findAttr(*span, "phase"), nullptr);
            const auto* bytes_out = findAttr(*span, "bytes_out");
            ASSERT_NE(bytes_out, nullptr);
            EXPECT_GT(bytes_out->uint_val, 0U);
            const auto* bytes_in = findAttr(*span, "bytes_in");
            ASSERT_NE(bytes_in, nullptr);
            EXPECT_GT(bytes_in->uint_val, 0U);
        }

        TEST(PoolRpcTimeout, BatchTimeoutMarksSpan) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});
            config.rpc_timeout_ms = static_cast<int>(K_TIMEOUT.count());

            ConnectionPool pool(config, io);
            auto sub = std::make_shared<RecordingSubscriber>();

            std::vector<Request> reqs;
            reqs.push_back({.opcode = Opcode::Get, .key = "k1", .value = {}, .ttl = std::nullopt});

            bool callback_called = false;
            {
                StatusGuard guard(sub);
                pool.sendBatchAsync("node1", reqs, [&](Result<std::vector<net::Response>> res) {
                    callback_called = true;
                    EXPECT_FALSE(res.has_value());
                    io.stop();
                });

                io.run();
            }
            server.stop();
            EXPECT_TRUE(callback_called);

            auto spans = sub->spans();
            const auto* span = findOp(spans, "pool.send_batch");
            ASSERT_NE(span, nullptr);
            EXPECT_EQ(span->status, SpanStatus::Timeout);
        }

        TEST(RpcTimeout, TimeoutMarksSpan) {
            constexpr auto K_TIMEOUT = milliseconds(100);

            io_context io;
            BlackHoleServer server(io);
            ASSERT_NE(server.port(), 0);
            const uint16_t k_port = server.port();
            server.start();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});

            TcpTransport transport(io);
            transport.setConfig(config);
            transport.setRpcTimeout(K_TIMEOUT);
            auto sub = std::make_shared<RecordingSubscriber>();

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            {
                StatusGuard guard(sub);
                transport.sendRequestAsync("node1", req, [&](Result<net::Response> res) {
                    callback_called = true;
                    EXPECT_FALSE(res.has_value());
                    io.stop();
                });

                io.run();
            }
            server.stop();
            EXPECT_TRUE(callback_called);

            auto spans = sub->spans();
            const auto* span = findOp(spans, "transport.rpc");
            ASSERT_NE(span, nullptr);
            EXPECT_EQ(span->status, SpanStatus::Timeout);
        }

        TEST(RpcTimeout, SuccessMarksSpanOk) {
            io_context io;
            tcp::acceptor acceptor(io);
            const uint16_t k_port = net::test::bindEphemeral(acceptor);
            ASSERT_NE(k_port, 0);

            std::function<void()> do_accept;
            do_accept = [&] {
                auto sock = std::make_shared<tcp::socket>(io);
                acceptor.async_accept(*sock, [&, sock](error_code ec) {
                    if (ec) {
                        return;
                    }

                    auto buf = std::make_shared<std::vector<std::byte>>(4096);
                    sock->async_read_some(buffer(*buf), [sock, buf](error_code, size_t) {
                        auto resp = net::Response{.status = Errc::OK, .value = std::nullopt};
                        auto encoded = net::encode(resp);
                        if (encoded) {
                            error_code wec;
                            asio::write(*sock, buffer(*encoded), wec);
                        }
                    });
                    do_accept();
                });
            };
            do_accept();

            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", k_port});

            TcpTransport transport(io);
            transport.setConfig(config);
            transport.setRpcTimeout(milliseconds(5000));
            auto sub = std::make_shared<RecordingSubscriber>();

            Request req{.opcode = Opcode::Set, .key = "k", .value = "v"};

            bool callback_called = false;
            {
                StatusGuard guard(sub);
                transport.sendRequestAsync("node1", req, [&](Result<net::Response> res) {
                    callback_called = true;
                    EXPECT_TRUE(res.has_value());
                    io.stop();
                });

                io.run();
            }
            error_code ec;
            acceptor.close(ec);
            EXPECT_TRUE(callback_called);

            auto spans = sub->spans();
            const auto* span = findOp(spans, "transport.rpc");
            ASSERT_NE(span, nullptr);
            EXPECT_EQ(span->status, SpanStatus::Ok);
        }
    } // namespace
} // namespace cinder
