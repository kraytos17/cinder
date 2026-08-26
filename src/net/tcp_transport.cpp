#include "cinder/net/tcp_transport.hpp"

#include <array>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

using asio::async_connect;
using asio::async_read;
using asio::async_write;
using asio::buffer;
using asio::error_code;
using asio::io_context;

namespace cinder {

TcpTransport::TcpTransport(io_context& io
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : io_(io)
#ifdef CINDER_ENABLE_TLS
      ,
      ssl_ctx_(ssl_ctx)
#endif
{
}

TcpTransport::~TcpTransport() {
    shutdown();
}

void
TcpTransport::shutdown() {
    std::scoped_lock lock(mu_);
    if (stopping_) {
        return;
    }

    stopping_ = true;
    for (auto& [id, conn] : conns_) {
        if (!conn || !conn->connected) {
            continue;
        }

        std::error_code ec;
#ifdef CINDER_ENABLE_TLS
        if (conn->stream) {
            conn->stream->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
            conn->stream->lowest_layer().close(ec);
        } else {
            conn->socket.shutdown(tcp::socket::shutdown_both, ec);
            conn->socket.close(ec);
        }
#else
        conn->socket.shutdown(tcp::socket::shutdown_both, ec);
        conn->socket.close(ec);
#endif
        conn->connected = false;
    }
}

void
TcpTransport::setConfig(const ClusterConfig& config) {
    std::scoped_lock lock(mu_);
    for (const auto& n : config.nodes) {
        addrs_[n.id] = n;
    }
}

void
TcpTransport::addAddr(const NodeId& id, const std::string& host, uint16_t port) {
    std::scoped_lock lock(mu_);
    addrs_[id] = {id, host, port};
}

void
TcpTransport::setRpcTimeout(std::chrono::milliseconds timeout) {
    rpc_timeout_ = timeout;
}

auto
TcpTransport::getOrCreateConn(const NodeId& id) -> NodeConn& {
    std::scoped_lock lock(mu_);
    auto it = conns_.find(id);
    if (it != conns_.end() && it->second) {
        return *it->second;
    }

    auto addr_it = addrs_.find(id);
    auto addr = (addr_it != addrs_.end()) ? addr_it->second : ClusterConfig::NodeConfig{id, {}, 0};
    auto conn = std::make_unique<NodeConn>(io_, addr);
    auto& ref = *conn;
    conns_[id] = std::move(conn);
    return ref;
}

auto
TcpTransport::sendCoroutine(NodeConn& conn, std::vector<std::byte> data)
    -> asio::awaitable<Result<net::Response>> {
    std::error_code ec;
    auto ex = co_await asio::this_coro::executor; // NOLINT(clang-analyzer-core.CallAndMessage) —
                                                  // false positive in asio awaitable internals

    // Start deadline timer — runs on the same strand as this coroutine.
    // When the deadline expires the timer callback cancels in-flight socket
    // operations, causing any in-progress co_await to complete with
    // operation_aborted.
    bool timed_out = false;
    asio::steady_timer timer(ex, rpc_timeout_);
    timer.async_wait([&conn, &timed_out](std::error_code timer_ec) {
        if (!timer_ec) {
            timed_out = true;
#ifdef CINDER_ENABLE_TLS
            if (conn.stream) {
                conn.stream->lowest_layer().cancel();
            } else {
                conn.socket.cancel();
            }
#else
            conn.socket.cancel();
#endif
        }
    });

    auto rpc_err = [&timed_out](const char* phase, const std::error_code& err_ec) {
        if (timed_out) {
            return Error(Errc::Timeout, "RPC deadline exceeded");
        }
        return Error(Errc::NotReady, std::string(phase) + ": " + err_ec.message());
    };

    if (!conn.connected) {
        tcp::resolver resolver(ex);
        auto endpoints = co_await resolver.async_resolve(
            conn.addr.host, std::to_string(conn.addr.port), asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<net::Response>(rpc_err("resolve", ec));
        }
#ifdef CINDER_ENABLE_TLS
        if (ssl_ctx_) {
            conn.stream = std::make_unique<asio::ssl::stream<tcp::socket>>(ex, *ssl_ctx_);
            co_await async_connect(
                conn.stream->lowest_layer(), endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(rpc_err("connect", ec));
            }

            co_await conn.stream->async_handshake(
                asio::ssl::stream_base::client, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(rpc_err("tls handshake", ec));
            }
        } else {
            co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(rpc_err("connect", ec));
            }
        }
#else
        co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<net::Response>(rpc_err("connect", ec));
        }
#endif
        conn.connected = true;
    }

#ifdef CINDER_ENABLE_TLS
    if (conn.stream) {
        co_await async_write(*conn.stream, buffer(data), asio::redirect_error(ec));
    } else {
        co_await async_write(conn.socket, buffer(data), asio::redirect_error(ec));
    }
#else
    co_await async_write(conn.socket, buffer(data), asio::redirect_error(ec));
#endif
    if (ec) {
        conn.connected = false;
        co_return err<net::Response>(rpc_err("write", ec));
    }

    std::array<std::byte, net::K_FRAME_HEADER_SIZE> header{};
#ifdef CINDER_ENABLE_TLS
    if (conn.stream) {
        co_await async_read(*conn.stream, buffer(header), asio::redirect_error(ec));
    } else {
        co_await async_read(conn.socket, buffer(header), asio::redirect_error(ec));
    }
#else
    co_await async_read(conn.socket, buffer(header), asio::redirect_error(ec));
#endif
    if (ec) {
        conn.connected = false;
        co_return err<net::Response>(rpc_err("read header", ec));
    }
    if (header[0] != std::byte{net::K_MAGIC}) {
        conn.connected = false;
        co_return err<net::Response>(Error(Errc::InternalError, "bad magic in response"));
    }

    uint32_t payload_len = 0;
    std::memcpy(&payload_len, &header[3], sizeof(payload_len));
    payload_len = std::byteswap(payload_len);
    if (payload_len > net::K_MAX_MESSAGE_SIZE) {
        conn.connected = false;
        co_return err<net::Response>(Error(Errc::InternalError, "response payload too large"));
    }

    std::vector<std::byte> frame(net::K_FRAME_HEADER_SIZE + payload_len);
    std::memcpy(frame.data(), header.data(), net::K_FRAME_HEADER_SIZE);
    if (payload_len > 0) {
#ifdef CINDER_ENABLE_TLS
        if (conn.stream) {
            co_await async_read(*conn.stream,
                buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                asio::redirect_error(ec));
        } else {
            co_await async_read(conn.socket,
                buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                asio::redirect_error(ec));
        }
#else
        co_await async_read(conn.socket,
            buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
            asio::redirect_error(ec));
#endif
        if (ec) {
            conn.connected = false;
            co_return err<net::Response>(rpc_err("read payload", ec));
        }
    }
    timer.cancel();
    co_return net::decodeResponse(frame);
}

void
TcpTransport::sendAsync(const NodeId& to, const net::Request& req, SendCallback on_done) {
    {
        std::scoped_lock lock(mu_);
        if (!addrs_.contains(to)) {
            on_done(err(Error(Errc::NotFound, "unknown node")));
            return;
        }
    }

    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        on_done(err(encoded.error()));
        return;
    }

    auto& conn = getOrCreateConn(to);
    asio::co_spawn(conn.strand,
        sendCoroutine(conn, std::move(encoded.value())),
        [cb = std::move(on_done)](std::exception_ptr ep, Result<net::Response> result) mutable {
        if (ep) {
            cb(err(Error(Errc::NotReady, "coroutine failed")));
            return;
        }
        if (!result.has_value()) {
            cb(err(result.error()));
            return;
        }
        if (result.value().status != Errc::OK) {
            cb(err(Error(result.value().status, "replica rejected write")));
            return;
        }
        cb(ok());
    });
}

void
TcpTransport::sendRequestAsync(const NodeId& to, const net::Request& req, RequestCallback on_done) {
    {
        std::scoped_lock lock(mu_);
        if (!addrs_.contains(to)) {
            on_done(err<net::Response>(Error(Errc::NotFound, "unknown node")));
            return;
        }
    }

    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        on_done(err<net::Response>(encoded.error()));
        return;
    }

    auto& conn = getOrCreateConn(to);
    asio::co_spawn(conn.strand,
        sendCoroutine(conn, std::move(encoded.value())),
        [cb = std::move(on_done)](std::exception_ptr ep, Result<net::Response> result) mutable {
        if (ep) {
            cb(err<net::Response>(Error(Errc::NotReady, "coroutine failed")));
            return;
        }
        cb(std::move(result));
    });
}

void
TcpTransport::onMessage(MessageHandler handler) {
    handler_ = std::move(handler);
}
} // namespace cinder
