#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/node/replication_manager.hpp"

namespace cinder::net {

TcpConnection::TcpConnection(asio::ip::tcp::socket socket, CacheStore& store,
    const ConsistentHashRing& ring, std::string node_id, ReplicationManager* repl,
    int replica_factor, ConsistencyMode mode, GossipManager* gossip)
    : socket_(std::move(socket)),
      store_(store),
      ring_(ring),
      node_id_(std::move(node_id)),
      repl_(repl),
      replica_factor_(replica_factor),
      mode_(mode),
      gossip_(gossip),
      read_buf_{} {}

TcpConnection::~TcpConnection() {
    if (socket_.is_open()) {
        std::error_code ec;
        socket_.close(ec);
    }
}

void
TcpConnection::start() {
    maybeRead();
}

void
TcpConnection::maybeRead() {
    if (!reading_ && write_queue_.size() < K_MAX_WRITE_QUEUE) {
        reading_ = true;
        doReadHeader();
    }
}

void
TcpConnection::doReadHeader() {
    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(read_buf_.data(), K_FRAME_HEADER_SIZE),
        [this, self](std::error_code ec, size_t) {
        if (ec) {
            return;
        }
        onHeader(ec, K_FRAME_HEADER_SIZE);
    });
}

void
TcpConnection::onHeader(std::error_code ec, size_t /*unused*/) {
    if (ec) {
        return;
    }
    if (read_buf_[0] != std::byte{K_MAGIC} || read_buf_[1] != std::byte{K_VERSION}) {
        return;
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &read_buf_[3], sizeof(net_len));
    payload_len_ = std::byteswap(net_len);
    if (payload_len_ > K_MAX_MESSAGE_SIZE || payload_len_ + K_FRAME_HEADER_SIZE > K_BUFFER_SIZE) {
        return;
    }
    doReadPayload(payload_len_);
}

void
TcpConnection::doReadPayload(size_t len) {
    auto self = shared_from_this();
    asio::async_read(socket_,
        asio::buffer(read_buf_.data() + K_FRAME_HEADER_SIZE, len),
        [this, self](std::error_code ec, size_t) { onPayload(ec, payload_len_); });
}

void
TcpConnection::onPayload(std::error_code ec, size_t bytes) {
    reading_ = false;
    if (ec) {
        return;
    }

    auto result = decode(std::span<const std::byte>(read_buf_.data(), K_FRAME_HEADER_SIZE + bytes));
    if (!result.has_value()) {
        Response res{.status = Errc::InvalidArgument, .value = std::nullopt};
        sendResponse(res);
        return;
    }
    handleRequest(result.value());
}

void
TcpConnection::handleRequest(const Request& req) {
    // Replicate/Hint/Gossip are inter-node messages addressed to this node
    // directly (a replica does not own the key) — skip the ring ownership check.
    bool is_internal = req.opcode == Opcode::Replicate || req.opcode == Opcode::Hint
                       || req.opcode == Opcode::Gossip;

    // Reads are served from the local store when present — a replica holds a
    // copy and can keep serving reads after the primary fails (failover read).
    // Ownership only matters for writes and for read misses (redirect the
    // client to the ring owner).
    bool is_read = req.opcode == Opcode::Get;
    if (!is_internal && req.opcode != Opcode::Ping && !is_read) {
        auto owner = ring_.getNode(req.key);
        if (owner != node_id_) {
            sendResponse({.status = Errc::NotReady, .value = "moved to " + owner});
            maybeRead();
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
            } else if (!is_internal && ring_.getNode(req.key) != node_id_) {
                sendResponse(
                    {.status = Errc::NotReady, .value = "moved to " + ring_.getNode(req.key)});
                maybeRead();
                return;
            } else {
                res.status = Errc::NotFound;
            }
            break;
        }
        case Opcode::Set: {
            if (repl_ != nullptr && replica_factor_ > 1) {
                auto nodes = ring_.getNodes(req.key, replica_factor_);
                std::vector<NodeId> replicas;
                for (const auto& n : nodes) {
                    if (n != node_id_) {
                        replicas.push_back(n);
                    }
                }

                auto self = shared_from_this();
                repl_->writeAsync(req.key,
                    req.value,
                    req.ttl,
                    replicas,
                    mode_,
                    [this, self](Result<void> result) {
                    Response async_res{
                        .status = result.has_value() ? Errc::OK : result.error().code(),
                        .value = std::nullopt,
                    };
                    sendResponse(async_res);
                    maybeRead();
                });
                return; // response sent asynchronously from the write callback
            } else {
                auto result = store_.put(req.key, req.value, req.ttl);
                res.status = result.has_value() ? Errc::OK : result.error().code();
            }
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
        case Opcode::Replicate:
        case Opcode::Hint: {
            VersionedEntry entry;
            entry.value = req.value;
            entry.version = req.version;
            entry.writer_node_hash = req.writer_node_hash;
            if (req.ttl.has_value()) {
                entry.expires_at = std::chrono::steady_clock::now() + *req.ttl;
                entry.has_ttl = true;
            }

            auto result = store_.putVersioned(req.key, std::move(entry));
            res.status = result.has_value() ? Errc::OK : result.error().code();
            break;
        }
        case Opcode::Gossip: {
            if (gossip_ != nullptr) {
                // Sender identity is best-effort: the connection knows only its
                // own node_id_; the sender's entry is always inside the payload.
                gossip_->handleMessage(node_id_, req);
            }
            res.status = Errc::OK;
            break;
        }
        default: {
            std::unreachable();
        }
    }

    sendResponse(res);
    maybeRead();
}

void
TcpConnection::sendResponse(const Response& res) {
    // Encode into the scratch buffer, then hand ownership to the write queue.
    auto result = encodeInto(res, encode_buf_);
    if (!result.has_value()) {
        return;
    }

    write_queue_.push_back(std::move(encode_buf_));
    if (!writing_) {
        doWrite();
    }
}

void
TcpConnection::doWrite() {
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

        // Recycle the completed buffer's capacity for the next encode.
        encode_buf_ = std::move(write_queue_.front());
        write_queue_.pop_front();
        if (!write_queue_.empty()) {
            doWrite();
        } else {
            writing_ = false;
            maybeRead();
        }
    });
}
} // namespace cinder::net
