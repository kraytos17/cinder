#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <utility>

namespace cinder::net {

TcpConnection::TcpConnection(asio::ip::tcp::socket socket, CacheStore& store,
    const ConsistentHashRing& ring, std::string node_id)
    : socket_(std::move(socket)),
      store_(store),
      ring_(ring),
      node_id_(std::move(node_id)) {}

TcpConnection::~TcpConnection() {
    if (socket_.is_open()) {
        std::error_code ec;
        socket_.close(ec);
    }
}

void
TcpConnection::start() {
    maybe_read();
}

void
TcpConnection::maybe_read() {
    if (!reading_ && write_queue_.size() < kMaxWriteQueue) {
        reading_ = true;
        do_read_header();
    }
}

void
TcpConnection::do_read_header() {
    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(read_buf_.data(), kFrameHeaderSize),
        [this, self](std::error_code ec, size_t) {
        if (ec) {
            return;
        }
        on_header(ec, kFrameHeaderSize);
    });
}

void
TcpConnection::on_header(std::error_code ec, size_t) {
    if (ec) {
        return;
    }
    if (read_buf_[0] != std::byte{kMagic} || read_buf_[1] != std::byte{kVersion}) {
        return;
    }

    uint32_t net_len;
    std::memcpy(&net_len, &read_buf_[3], sizeof(net_len));
    payload_len_ = std::byteswap(net_len);
    if (payload_len_ > kMaxMessageSize || payload_len_ + kFrameHeaderSize > kBufferSize) {
        return;
    }
    do_read_payload(payload_len_);
}

void
TcpConnection::do_read_payload(size_t len) {
    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(read_buf_.data() + kFrameHeaderSize, len),
        [this, self](std::error_code ec, size_t) { on_payload(ec, payload_len_); });
}

void
TcpConnection::on_payload(std::error_code ec, size_t len) {
    reading_ = false;
    if (ec) {
        return;
    }

    auto result = decode(std::span<const std::byte>(read_buf_.data(), kFrameHeaderSize + len));
    if (!result.has_value()) {
        Response res{.status = Errc::InvalidArgument, .value = std::nullopt};
        send_response(res);
        return;
    }
    handle_request(result.value());
}

void
TcpConnection::handle_request(const Request& req) {
    if (req.opcode != Opcode::Ping) {
        auto owner = ring_.get_node(req.key);
        if (owner != node_id_) {
            send_response({.status = Errc::NotReady, .value = "moved to " + owner});
            maybe_read();
            return;
        }
    }

    Response res;
    switch (req.opcode) {
        case Opcode::Get: {
            auto val = store_.get(req.key);
            if (val.has_value()) {
                res.status = Errc::OK;
                res.value = std::move(val);
            } else {
                res.status = Errc::NotFound;
            }
            break;
        }
        case Opcode::Set: {
            auto result = store_.put(req.key, req.value, req.ttl);
            res.status = result.has_value() ? Errc::OK : result.error().code();
            break;
        }
        case Opcode::Del: {
            store_.remove(req.key);
            res.status = Errc::OK;
            break;
        }
        case Opcode::Ping: {
            res.status = Errc::OK;
            break;
        }
        default: {
            res.status = Errc::NotSupported;
            break;
        }
    }

    send_response(res);
    maybe_read();
}

void
TcpConnection::send_response(const Response& res) {
    auto result = encode(res);
    if (!result.has_value()) {
        return;
    }

    write_queue_.push_back(std::move(result.value()));
    if (!writing_) {
        do_write();
    }
}

void
TcpConnection::do_write() {
    if (write_queue_.empty()) {
        writing_ = false;
        return;
    }

    writing_ = true;
    auto self = shared_from_this();
    auto& buf = write_queue_.front();
    asio::async_write(
        socket_, asio::buffer(buf.data(), buf.size()), [this, self](std::error_code ec, size_t) {
        if (ec) {
            write_queue_.clear();
            writing_ = false;
            return;
        }

        write_queue_.pop_front();
        if (!write_queue_.empty()) {
            do_write();
        } else {
            writing_ = false;
            maybe_read();
        }
    });
}
} // namespace cinder::net
