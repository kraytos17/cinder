#include "cinder/client/connection_pool.hpp"

#include <array>
#include <exception>
#include <string>
#include <utility>
#include <vector>

using asio::async_connect;
using asio::async_read;
using asio::async_write;
using asio::buffer;
using asio::error_code;
using asio::io_context;

namespace {
auto
poolError(const char* phase, const error_code& ec) -> cinder::Error {
    return cinder::Error(cinder::Errc::NotReady, std::string(phase) + ": " + ec.message());
}
} // namespace

namespace cinder {

ConnectionPool::ConnectionPool(const ClusterConfig& config, io_context& io
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
    for (const auto& n : config.nodes) {
        node_addrs_.insert_or_assign(n.id, n);
    }
}

ConnectionPool::~ConnectionPool() {
    shutdown();
}

void
ConnectionPool::shutdown() {
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
    conns_.clear();
    node_addrs_.clear();
}

auto
ConnectionPool::getOrCreateConn(const NodeId& node_id) -> NodeConn& {
    std::scoped_lock lock(mu_);
    auto it = conns_.find(node_id);
    if (it != conns_.end() && it->second) {
        return *it->second;
    }

    auto addr_it = node_addrs_.find(node_id);
    auto addr = (addr_it != node_addrs_.end()) ? addr_it->second
                                               : ClusterConfig::NodeConfig{node_id, {}, 0};

    auto conn = std::make_unique<NodeConn>(io_, addr);
    auto& ref = *conn;
    conns_[node_id] = std::move(conn);
    return ref;
}

auto
ConnectionPool::sendCoroutine(NodeConn& conn, std::vector<std::byte> data)
    -> asio::awaitable<Result<net::Response>> {
    std::error_code ec;
    auto ex = co_await asio::this_coro::executor; // NOLINT
    if (!conn.connected) {
        tcp::resolver resolver(ex);
        auto endpoints = co_await resolver.async_resolve(
            conn.addr.host, std::to_string(conn.addr.port), asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<net::Response>(poolError("resolve", ec));
        }
#ifdef CINDER_ENABLE_TLS
        if (ssl_ctx_) {
            conn.stream = std::make_unique<stream_type>(ex, *ssl_ctx_);
            co_await async_connect(
                conn.stream->lowest_layer(), endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(poolError("connect", ec));
            }

            co_await conn.stream->async_handshake(
                asio::ssl::stream_base::client, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(poolError("tls handshake", ec));
            }
        } else {
            co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<net::Response>(poolError("connect", ec));
            }
        }
#else
        co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<net::Response>(poolError("connect", ec));
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
        co_return err<net::Response>(poolError("write", ec));
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
        co_return err<net::Response>(poolError("read header", ec));
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
            co_return err<net::Response>(poolError("read payload", ec));
        }
    }
    co_return net::decodeResponse(frame);
}

auto
ConnectionPool::sendBatchCoroutine(NodeConn& conn, std::vector<std::vector<std::byte>> all_data)
    -> asio::awaitable<Result<std::vector<net::Response>>> {
    std::error_code ec;
    auto ex = co_await asio::this_coro::executor; // NOLINT
    if (!conn.connected) {
        tcp::resolver resolver(ex);
        auto endpoints = co_await resolver.async_resolve(
            conn.addr.host, std::to_string(conn.addr.port), asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<std::vector<net::Response>>(poolError("resolve", ec));
        }
#ifdef CINDER_ENABLE_TLS
        if (ssl_ctx_) {
            conn.stream = std::make_unique<stream_type>(ex, *ssl_ctx_);
            co_await async_connect(
                conn.stream->lowest_layer(), endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<std::vector<net::Response>>(poolError("connect", ec));
            }

            co_await conn.stream->async_handshake(
                asio::ssl::stream_base::client, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<std::vector<net::Response>>(poolError("tls handshake", ec));
            }
        } else {
            co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
            if (ec) {
                conn.connected = false;
                co_return err<std::vector<net::Response>>(poolError("connect", ec));
            }
        }
#else
        co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
        if (ec) {
            conn.connected = false;
            co_return err<std::vector<net::Response>>(poolError("connect", ec));
        }
#endif
        conn.connected = true;
    }

#ifdef CINDER_ENABLE_TLS
    auto writeAll = [&]() -> asio::awaitable<void> {
        for (auto& data : all_data) {
            if (conn.stream) {
                co_await async_write(*conn.stream, buffer(data), asio::redirect_error(ec));
            } else {
                co_await async_write(conn.socket, buffer(data), asio::redirect_error(ec));
            }
            if (ec) {
                co_return;
            }
        }
    };
    co_await writeAll();
#else
    for (auto& data : all_data) {
        co_await async_write(conn.socket, buffer(data), asio::redirect_error(ec));
        if (ec) {
            break;
        }
    }
#endif
    if (ec) {
        conn.connected = false;
        co_return err<std::vector<net::Response>>(poolError("write", ec));
    }

    std::vector<net::Response> responses;
    responses.reserve(all_data.size());
    for (size_t i = 0; i < all_data.size(); ++i) {
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
            co_return err<std::vector<net::Response>>(poolError("read header", ec));
        }
        if (header[0] != std::byte{net::K_MAGIC}) {
            conn.connected = false;
            co_return err<std::vector<net::Response>>(
                Error(Errc::InternalError, "bad magic in response"));
        }

        uint32_t payload_len = 0;
        std::memcpy(&payload_len, &header[3], sizeof(payload_len));
        payload_len = std::byteswap(payload_len);
        if (payload_len > net::K_MAX_MESSAGE_SIZE) {
            conn.connected = false;
            co_return err<std::vector<net::Response>>(
                Error(Errc::InternalError, "response payload too large"));
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
                co_return err<std::vector<net::Response>>(poolError("read payload", ec));
            }
        }
        responses.push_back(net::decodeResponse(frame).value());
    }
    co_return ok(std::move(responses));
}

void
ConnectionPool::sendAsync(const NodeId& node_id, const net::Request& req,
    std::function<void(Result<net::Response>)> on_done) {
    {
        std::scoped_lock lock(mu_);
        if (stopping_) {
            on_done(err<net::Response>(Error(Errc::NotReady, "pool shutting down")));
            return;
        }
        if (!node_addrs_.contains(node_id)) {
            on_done(err<net::Response>(Error(Errc::NotFound, "unknown node")));
            return;
        }
    }

    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        on_done(err<net::Response>(encoded.error()));
        return;
    }

    auto& conn = getOrCreateConn(node_id);
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
ConnectionPool::sendBatchAsync(const NodeId& node_id, std::vector<net::Request> reqs,
    std::function<void(Result<std::vector<net::Response>>)> on_done) {
    if (reqs.empty()) {
        on_done(ok(std::vector<net::Response>{}));
        return;
    }

    {
        std::scoped_lock lock(mu_);
        if (stopping_) {
            on_done(err<std::vector<net::Response>>(Error(Errc::NotReady, "pool shutting down")));
            return;
        }
        if (!node_addrs_.contains(node_id)) {
            on_done(err<std::vector<net::Response>>(Error(Errc::NotFound, "unknown node")));
            return;
        }
    }

    std::vector<std::vector<std::byte>> all_data;
    all_data.reserve(reqs.size());
    for (const auto& req : reqs) {
        auto encoded = net::encode(req);
        if (!encoded.has_value()) {
            on_done(err<std::vector<net::Response>>(encoded.error()));
            return;
        }
        all_data.push_back(std::move(encoded.value()));
    }

    auto& conn = getOrCreateConn(node_id);
    asio::co_spawn(conn.strand,
        sendBatchCoroutine(conn, std::move(all_data)),
        [cb = std::move(on_done)](
            std::exception_ptr ep, Result<std::vector<net::Response>> result) mutable {
        if (ep) {
            cb(err<std::vector<net::Response>>(Error(Errc::NotReady, "coroutine failed")));
            return;
        }
        cb(std::move(result));
    });
}

auto
ConnectionPool::send(const NodeId& node_id, const net::Request& req) -> Result<net::Response> {
    std::promise<Result<net::Response>> promise;
    auto future = promise.get_future();
    sendAsync(node_id, req, [&promise](Result<net::Response> result) {
        promise.set_value(std::move(result));
    });
    return future.get();
}

auto
ConnectionPool::sendBatch(const NodeId& node_id, const std::vector<net::Request>& reqs)
    -> Result<std::vector<net::Response>> {
    if (reqs.empty()) {
        return ok(std::vector<net::Response>{});
    }

    std::promise<Result<std::vector<net::Response>>> promise;
    auto future = promise.get_future();
    sendBatchAsync(node_id, reqs, [&promise](Result<std::vector<net::Response>> result) {
        promise.set_value(std::move(result));
    });
    return future.get();
}
} // namespace cinder
