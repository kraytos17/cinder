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

TcpTransport::TcpTransport(io_context& io)
    : io_(io) {}

void
TcpTransport::setConfig(const ClusterConfig& config) {
    for (const auto& n : config.nodes) {
        addrs_[n.id] = n;
    }
}

void
TcpTransport::addAddr(const NodeId& id, const std::string& host, uint16_t port) {
    addrs_[id] = {id, host, port};
}

auto
TcpTransport::sendCoroutine(std::string host, uint16_t port, std::vector<std::byte> data)
    -> asio::awaitable<Result<net::Response>> {
    std::error_code ec;
    auto ex = co_await asio::this_coro::executor;
    tcp::resolver resolver(ex);
    auto endpoints =
        co_await resolver.async_resolve(host, std::to_string(port), asio::redirect_error(ec));
    if (ec) {
        co_return err<net::Response>(Error(Errc::NotReady, "resolve: " + ec.message()));
    }

    tcp::socket socket(ex);
    co_await async_connect(socket, endpoints, asio::redirect_error(ec));
    if (ec) {
        co_return err<net::Response>(Error(Errc::NotReady, "connect: " + ec.message()));
    }

    co_await async_write(socket, buffer(data), asio::redirect_error(ec));
    if (ec) {
        co_return err<net::Response>(Error(Errc::NotReady, "write: " + ec.message()));
    }

    std::array<std::byte, net::K_FRAME_HEADER_SIZE> header{};
    co_await async_read(socket, buffer(header), asio::redirect_error(ec));
    if (ec) {
        co_return err<net::Response>(Error(Errc::NotReady, "read header: " + ec.message()));
    }
    if (header[0] != std::byte{net::K_MAGIC}) {
        co_return err<net::Response>(Error(Errc::InternalError, "bad magic in response"));
    }

    uint32_t payload_len = 0;
    std::memcpy(&payload_len, &header[3], sizeof(payload_len));
    payload_len = std::byteswap(payload_len);
    if (payload_len > net::K_MAX_MESSAGE_SIZE) {
        co_return err<net::Response>(Error(Errc::InternalError, "response payload too large"));
    }

    std::vector<std::byte> frame(net::K_FRAME_HEADER_SIZE + payload_len);
    std::memcpy(frame.data(), header.data(), net::K_FRAME_HEADER_SIZE);
    if (payload_len > 0) {
        co_await async_read(socket,
            buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
            asio::redirect_error(ec));
        if (ec) {
            co_return err<net::Response>(Error(Errc::NotReady, "read payload: " + ec.message()));
        }
    }
    co_return net::decodeResponse(frame);
}

void
TcpTransport::sendAsync(const NodeId& to, const net::Request& req, SendCallback on_done) {
    auto addr_it = addrs_.find(to);
    if (addr_it == addrs_.end()) {
        on_done(err(Error(Errc::NotFound, "unknown node")));
        return;
    }

    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        on_done(err(encoded.error()));
        return;
    }

    auto host = addr_it->second.host;
    auto port = addr_it->second.port;
    asio::co_spawn(io_,
        sendCoroutine(host, port, std::move(encoded.value())),
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
    auto addr_it = addrs_.find(to);
    if (addr_it == addrs_.end()) {
        on_done(err<net::Response>(Error(Errc::NotFound, "unknown node")));
        return;
    }

    auto encoded = net::encode(req);
    if (!encoded.has_value()) {
        on_done(err<net::Response>(encoded.error()));
        return;
    }

    auto host = addr_it->second.host;
    auto port = addr_it->second.port;
    asio::co_spawn(io_,
        sendCoroutine(host, port, std::move(encoded.value())),
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
