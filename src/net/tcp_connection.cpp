#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/status.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::async_read;
using asio::async_write;
using asio::buffer;

namespace cinder::net {

TcpConnection::TcpConnection(tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
    std::string_view node_id, Clock& clock, ReplicationManager* repl, int replica_factor,
    ConsistencyMode mode, GossipManager* gossip
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : socket_(std::move(socket)),
      strand_(socket_.get_executor()),
      store_(store),
      ring_(ring),
      clock_(clock),
      repl_(repl),
      gossip_(gossip),
      node_id_(node_id),
      replica_factor_(replica_factor),
      mode_(mode),
      read_buf_{},
      encode_buf_(512) { // pre-allocate for typical requests
#ifdef CINDER_ENABLE_TLS
    if (ssl_ctx) {
        ssl_stream_.emplace(socket_, *ssl_ctx);
    }
#endif
}

TcpConnection::~TcpConnection() {
    if (socket_.is_open()) {
        std::error_code ec;
#ifdef CINDER_ENABLE_TLS
        if (ssl_stream_) {
            ssl_stream_->shutdown(ec);
        }
#endif
        socket_.close(ec);
        Logger::debug("cinder tcp_connection: connection closed");
    }
}

void
TcpConnection::start() {
    // Called from the accept loop on an arbitrary pool thread; all connection
    // state mutation happens on-strand.
    auto self = shared_from_this();
    asio::post(strand_, [self]() mutable { self->startOnStrand(); });
}

void
TcpConnection::startOnStrand() {
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        ssl_stream_->async_handshake(asio::ssl::stream_base::server,
            asio::bind_executor(strand_, [this, self = shared_from_this()](std::error_code ec) {
            if (ec) {
                std::error_code close_ec;
                socket_.close(close_ec);
                return;
            }
            maybeRead();
        }));
        return;
    }
#endif
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
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        async_read(*ssl_stream_,
            buffer(read_buf_.data(), K_FRAME_HEADER_SIZE),
            asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
            if (ec) {
                return;
            }
            onHeader(ec, K_FRAME_HEADER_SIZE);
        }));
        return;
    }
#endif
    async_read(socket_,
        buffer(read_buf_.data(), K_FRAME_HEADER_SIZE),
        asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
        if (ec) {
            return;
        }
        onHeader(ec, K_FRAME_HEADER_SIZE);
    }));
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
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        async_read(*ssl_stream_,
            buffer(read_buf_.data() + K_FRAME_HEADER_SIZE, len),
            asio::bind_executor(strand_,
                [this, self](std::error_code ec, size_t) { onPayload(ec, payload_len_); }));
        return;
    }
#endif
    async_read(socket_,
        buffer(read_buf_.data() + K_FRAME_HEADER_SIZE, len),
        asio::bind_executor(
            strand_, [this, self](std::error_code ec, size_t) { onPayload(ec, payload_len_); }));
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
    Logger::debug("cinder tcp_connection: request received opcode={} key={}",
        static_cast<int>(req.opcode),
        req.key);

    // Replicate/Hint/Gossip are inter-node messages addressed to this node
    // directly (a replica does not own the key) — skip the ring ownership check.
    bool is_internal = req.opcode == Opcode::Replicate || req.opcode == Opcode::Hint
                       || req.opcode == Opcode::Gossip;

    // Reads are served from the local store when present — a replica holds a
    // copy and can keep serving reads after the primary fails (failover read).
    // Ownership only matters for writes and for read misses (redirect the
    // client to the ring owner).
    bool is_read = req.opcode == Opcode::Get || req.opcode == Opcode::GetVersioned;
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
            if (repl_ != nullptr && replica_factor_ > 1 && !is_internal) {
                auto nodes = ring_.getNodes(req.key, replica_factor_);
                std::vector<NodeId> replicas;
                for (const auto& n : nodes) {
                    if (n != node_id_) {
                        replicas.push_back(n);
                    }
                }

                auto self = shared_from_this();
                repl_->readAsync(req.key,
                    replicas,
                    replica_factor_,
                    [this, self](Result<VersionedEntry> result) {
                    // Quorum completion lands on an arbitrary pool thread —
                    // hop back onto this connection's strand.
                    asio::post(strand_, [this, self, result = std::move(result)]() mutable {
                        Response async_res;
                        if (result.has_value()) {
                            async_res.status = Errc::OK;
                            async_res.value = std::move(result->value);
                        } else {
                            async_res.status = result.error().code();
                        }
                        sendResponse(async_res);
                        maybeRead();
                    });
                });
                return;
            }

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
        case Opcode::GetVersioned: {
            auto entry = store_.getVersioned(req.key);
            if (entry.has_value()) {
                res.status = Errc::OK;
                res.value = std::move(entry->value);
                res.version = entry->version;
                res.writer_node_hash = entry->writer_node_hash;
                if (entry->has_ttl) {
                    res.expires_at = toSystemExpiry(clock_, entry->expires_at);
                }
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
                    // Quorum completion lands on an arbitrary pool thread —
                    // hop back onto this connection's strand.
                    asio::post(strand_, [this, self, result]() {
                        Response async_res{
                            .status = result.has_value() ? Errc::OK : result.error().code(),
                            .value = std::nullopt,
                        };
                        sendResponse(async_res);
                        maybeRead();
                    });
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
            // Apply the primary's absolute wall-clock expiry on the local steady
            // basis, so all replicas expire the key at the same instant.
            if (req.expires_at.has_value()) {
                entry.expires_at = toSteadyExpiry(clock_, *req.expires_at);
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
                gossip_->handleMessage(std::string(node_id_), req);
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
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        async_write(*ssl_stream_,
            buffer(buf.data(), buf.size()),
            asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
            if (ec) {
                Logger::warn("cinder tcp_connection: tls write failed: {}", ec.message());
                write_queue_.clear();
                writing_ = false;
                return;
            }

            encode_buf_ = std::move(write_queue_.front());
            write_queue_.pop_front();
            if (!write_queue_.empty()) {
                doWrite();
            } else {
                writing_ = false;
                maybeRead();
            }
        }));
        return;
    }
#endif
    async_write(socket_,
        buffer(buf.data(), buf.size()),
        asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
        if (ec) {
            Logger::warn("cinder tcp_connection: write failed: {}", ec.message());
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
    }));
}
} // namespace cinder::net
