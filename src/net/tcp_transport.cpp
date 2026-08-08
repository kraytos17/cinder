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

    auto* op =
        new OutboundRequest(io_, addr_it->second.host, addr_it->second.port, std::move(on_done));
    op->write_buf = std::move(encoded.value());
    start(op);
}

void
TcpTransport::onMessage(MessageHandler handler) {
    handler_ = std::move(handler);
}

void
TcpTransport::start(OutboundRequest* self) {
    self->resolver.async_resolve(self->host,
        std::to_string(self->port),
        [this, self](std::error_code ec, tcp::resolver::results_type endpoints) {
        if (ec) {
            finish(self, err(Error(Errc::NotReady, "resolve failed: " + ec.message())));
            return;
        }
        connect(self, endpoints);
    });
}

void
TcpTransport::connect(OutboundRequest* self, const tcp::resolver::results_type& endpoints) {
    async_connect(self->socket, endpoints, [this, self](std::error_code ec, const tcp::endpoint&) {
        if (ec) {
            finish(self, err(Error(Errc::NotReady, "connect failed: " + ec.message())));
            return;
        }
        writeFrame(self);
    });
}

void
TcpTransport::writeFrame(OutboundRequest* self) {
    async_write(self->socket, buffer(self->write_buf), [this, self](std::error_code ec, size_t) {
        if (ec) {
            finish(self, err(Error(Errc::NotReady, "write failed: " + ec.message())));
            return;
        }
        readHeader(self);
    });
}

void
TcpTransport::readHeader(OutboundRequest* self) {
    async_read(self->socket, buffer(self->header), [this, self](std::error_code ec, size_t) {
        if (ec) {
            finish(self, err(Error(Errc::NotReady, "read failed: " + ec.message())));
            return;
        }
        if (self->header[0] != std::byte{net::K_MAGIC}) {
            finish(self, err(Error(Errc::InternalError, "bad magic in response")));
            return;
        }

        uint32_t payload_len = 0;
        std::memcpy(&payload_len, &self->header[3], sizeof(payload_len));
        payload_len = std::byteswap(payload_len);
        if (payload_len > net::K_MAX_MESSAGE_SIZE) {
            finish(self, err(Error(Errc::InternalError, "response payload too large")));
            return;
        }
        readPayload(self, payload_len);
    });
}

void
TcpTransport::readPayload(OutboundRequest* self, uint32_t payload_len) {
    self->frame.resize(net::K_FRAME_HEADER_SIZE + payload_len);
    std::memcpy(self->frame.data(), self->header.data(), net::K_FRAME_HEADER_SIZE);
    if (payload_len == 0) {
        finishDecode(self);
        return;
    }

    async_read(self->socket,
        buffer(self->frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
        [this, self](std::error_code ec, size_t) {
        if (ec) {
            finish(self, err(Error(Errc::NotReady, "read failed: " + ec.message())));
            return;
        }
        finishDecode(self);
    });
}

void
TcpTransport::finishDecode(OutboundRequest* self) {
    auto res = net::decodeResponse(self->frame);
    if (!res.has_value()) {
        finish(self, err(res.error()));
        return;
    }
    if (res.value().status != Errc::OK) {
        finish(self, err(Error(res.value().status, "replica rejected write")));
        return;
    }
    finish(self, ok());
}

void
TcpTransport::finish(OutboundRequest* self, Result<void> result) {
    if (self->done) {
        return;
    }

    self->done = true;
    auto cb = std::move(self->on_done);
    delete self;
    cb(std::move(result));
}
} // namespace cinder
