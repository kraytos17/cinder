#include <asio.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <optional>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/clock.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/net/tcp_transport.hpp"

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

// A TCP server that accepts connections but never responds — used to trigger
// the RPC deadline timer.
class BlackHoleServer {
  public:

    BlackHoleServer(io_context& io, uint16_t port)
        : acceptor_(io, tcp::endpoint(address_v4::loopback(), port)) {}

    void start() { doAccept(); }

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
    std::vector<tcp::socket> sockets_;
};

TEST(RpcTimeout, RequestTimesOutWhenServerDoesNotRespond) {
    constexpr uint16_t K_PORT = 17'880;
    constexpr auto K_TIMEOUT = milliseconds(100);

    io_context io;
    BlackHoleServer server(io, K_PORT);
    server.start();

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", K_PORT});

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
    constexpr uint16_t K_PORT = 17'881;
    constexpr auto K_TIMEOUT = milliseconds(5'000);

    io_context io;
    tcp::acceptor acceptor(io, tcp::endpoint(address_v4::loopback(), K_PORT));

    // Server loop: accept, read request, send a valid response.
    std::function<void()> do_accept;
    do_accept = [&]() {
        auto sock = std::make_shared<tcp::socket>(io);
        acceptor.async_accept(*sock, [&, sock](error_code ec) {
            if (ec) {
                return;
            }

            // Read the request in the background (ignore content).
            auto buf = std::make_shared<std::vector<std::byte>>(4'096);
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
    config.nodes.push_back({"node1", "127.0.0.1", K_PORT});

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
    constexpr uint16_t K_PORT = 17'882;
    constexpr auto K_TIMEOUT = milliseconds(100);

    io_context io;
    BlackHoleServer server(io, K_PORT);
    server.start();

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", K_PORT});
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
    constexpr uint16_t K_PORT = 17'883;
    constexpr auto K_TIMEOUT = milliseconds(100);

    io_context io;
    BlackHoleServer server(io, K_PORT);
    server.start();

    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", K_PORT});
    config.rpc_timeout_ms = static_cast<int>(K_TIMEOUT.count());

    ConnectionPool pool(config, io);

    std::vector<Request> reqs;
    reqs.push_back({.opcode = Opcode::Get, .key = "k1", .value = {}, .ttl = std::nullopt});
    reqs.push_back({.opcode = Opcode::Get, .key = "k2", .value = {}, .ttl = std::nullopt});

    bool callback_called = false;
    pool.sendBatchAsync("node1", std::move(reqs), [&](Result<std::vector<net::Response>> res) {
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
    constexpr uint16_t K_PORT = 17'884;

    io_context io;
    tcp::acceptor acceptor(io, tcp::endpoint(address_v4::loopback(), K_PORT));

    std::function<void()> do_accept;
    do_accept = [&]() {
        auto sock = std::make_shared<tcp::socket>(io);
        acceptor.async_accept(*sock, [&, sock](error_code ec) {
            if (ec)
                return;
            auto buf = std::make_shared<std::vector<std::byte>>(4'096);
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
    config.nodes.push_back({"node1", "127.0.0.1", K_PORT});
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
} // namespace
} // namespace cinder
