#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::async_read;
using asio::async_write;
using asio::buffer;

namespace cinder::net {

TcpConnection::TcpConnection(tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
    std::string_view node_id, Clock& clock, ReplicationManager* repl, int replica_factor,
    ConsistencyMode mode, GossipManager* gossip, std::shared_ptr<std::atomic<size_t>> conn_counter
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : socket_(std::move(socket)),
      strand_(socket_.get_executor()),
      idle_timer_(socket_.get_executor()),
      store_(store),
      ring_(ring),
      clock_(clock),
      repl_(repl),
      gossip_(gossip),
      node_id_(node_id),
      replica_factor_(replica_factor),
      mode_(mode),
      read_buf_{},
      encode_buf_(512), // pre-allocate for typical requests
      conn_counter_(std::move(conn_counter)) {
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
    }
    if (conn_counter_) {
        conn_counter_->fetch_sub(1, std::memory_order_relaxed);
    }
    if (metrics_) {
        metrics_->connectionMetrics().connections_closed.fetch_add(1, std::memory_order_relaxed);
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
    std::error_code ec;
    auto ep = socket_.remote_endpoint(ec);
    if (!ec) {
        Logger::info("cinder tcp_connection: connection opened peer={}:{}",
            ep.address().to_string(),
            ep.port());
    } else {
        Logger::info("cinder tcp_connection: connection opened peer=<unknown>");
    }
    resetIdleTimer();
    maybeRead();
}

void
TcpConnection::close() {
    auto self = shared_from_this();
    asio::post(strand_,
        [this, self]() { closeConnection("server shutdown", asio::error::operation_aborted); });
}

void
TcpConnection::drain() {
    auto self = shared_from_this();
    asio::post(strand_, [this, self]() {
        draining_ = true;
        if (!pending_opcode_.has_value() && write_queue_.empty() && !writing_) {
            closeConnection("drained");
            return;
        }

        // Backstop: give the in-flight request its grace period, then force-close.
        idle_timer_.expires_after(K_DRAIN_TIMEOUT);
        std::weak_ptr<TcpConnection> weak = shared_from_this();
        idle_timer_.async_wait(asio::bind_executor(strand_, [weak](std::error_code ec) {
            if (auto s = weak.lock()) {
                s->onIdleTimeout(ec);
            }
        }));
    });
}

void
TcpConnection::maybeRead() {
    if (draining_
        || (!reading_ && write_queue_.size() < K_MAX_WRITE_QUEUE
            && write_queue_bytes_ < K_MAX_WRITE_QUEUE_BYTES)) {
        reading_ = true;
        doReadHeader();
    }
}

void
TcpConnection::resetIdleTimer() {
    idle_timer_.expires_after(K_IDLE_TIMEOUT);
    std::weak_ptr<TcpConnection> weak = shared_from_this();
    idle_timer_.async_wait(asio::bind_executor(strand_, [weak](std::error_code ec) {
        if (auto self = weak.lock()) {
            self->onIdleTimeout(ec);
        }
    }));
}

void
TcpConnection::onIdleTimeout(std::error_code ec) {
    if (ec) {
        return; // canceled or error — connection active or already closed
    }
    closeConnection("idle timeout");
}

void
TcpConnection::closeConnection(const char* reason, std::error_code ec) {
    if (!socket_.is_open()) {
        return;
    }

    // Broken pipe / connection reset / operation aborted are transient — peers
    // closing or node shutdown. Normal lifecycle closes (drained, idle timeout,
    // server shutdown) are expected too. Log those at debug, real failures at warn.
    bool transient = ec == asio::error::broken_pipe || ec == asio::error::connection_reset
                     || ec == asio::error::operation_aborted
                     || std::string_view(reason) == "drained"
                     || std::string_view(reason) == "idle timeout"
                     || std::string_view(reason) == "server shutdown";
    if (transient) {
        if (ec) {
            Logger::debug(
                "cinder tcp_connection: closing connection reason={} err={}", reason, ec.message());
        } else {
            Logger::debug("cinder tcp_connection: closing connection reason={}", reason);
        }
    } else {
        Logger::warn("cinder tcp_connection: closing connection reason={}", reason);
    }

    idle_timer_.cancel();
    std::error_code close_ec;
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        ssl_stream_->shutdown(close_ec);
    }
#endif
    socket_.close(close_ec);
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

    resetIdleTimer();
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

    resetIdleTimer();
    auto result = decode(std::span<const std::byte>(read_buf_.data(), K_FRAME_HEADER_SIZE + bytes));
    if (!result.has_value()) {
        Logger::debug("cinder tcp_connection: decode failed");
        if (metrics_) {
            metrics_->connectionMetrics().decode_failures.fetch_add(1, std::memory_order_relaxed);
        }

        Response res{.status = Errc::InvalidArgument, .value = std::nullopt};
        sendResponse(res);
        return;
    }
    handleRequest(result.value());
}

void
TcpConnection::handleRequest(const Request& req) {
    pending_opcode_ = req.opcode;
    request_start_ = std::chrono::steady_clock::now();
    Logger::debug("cinder tcp_connection: request received opcode={} key={}",
        static_cast<int>(req.opcode),
        req.key);

    if (metrics_) {
        switch (req.opcode) {
            case Opcode::Get:
                metrics_->opcodeMetrics().gets.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Set:
                metrics_->opcodeMetrics().sets.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Del:
                metrics_->opcodeMetrics().dels.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Ping:
                metrics_->opcodeMetrics().pings.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Replicate:
                metrics_->opcodeMetrics().replicates.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Hint:
                metrics_->opcodeMetrics().hints.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::GetVersioned:
                metrics_->opcodeMetrics().gets_versioned.fetch_add(1, std::memory_order_relaxed);
                break;
            default:
                break;
        }
    }

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
            Logger::debug("cinder tcp_connection: redirect key={} to={}", req.key, owner);
            if (metrics_) {
                metrics_->connectionMetrics().redirects.fetch_add(1, std::memory_order_relaxed);
            }

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

            Logger::trace("cinder tcp_connection: opcode={} key={} status={}",
                static_cast<int>(req.opcode),
                req.key,
                static_cast<int>(res.status));
            break;
        }
        case Opcode::Del: {
            store_.remove(req.key);
            res.status = Errc::OK;
            Logger::trace("cinder tcp_connection: opcode={} key={} status={}",
                static_cast<int>(req.opcode),
                req.key,
                static_cast<int>(res.status));
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
    if (pending_opcode_.has_value() && metrics_) {
        auto elapsed_ns =
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - request_start_)
                    .count());
        metrics_->opcodeMetrics().recordLatency(std::to_underlying(*pending_opcode_), elapsed_ns);
        pending_opcode_.reset();
    }
    // Encode into the scratch buffer, then hand ownership to the write queue.
    auto result = encodeInto(res, encode_buf_);
    if (!result.has_value()) {
        return;
    }

    write_queue_.push_back(std::move(encode_buf_));
    write_queue_bytes_ += write_queue_.back().size();
    if (write_queue_bytes_ > K_MAX_WRITE_QUEUE_BYTES) {
        if (metrics_) {
            metrics_->connectionMetrics().write_failures.fetch_add(1, std::memory_order_relaxed);
        }
        closeConnection("write queue overflow");
        return;
    }
    if (!writing_) {
        doWrite();
    }
    // A drain with no pending request and nothing left to write (e.g. the
    // decode-failure path replies without a queued write) can finish here.
    if (draining_ && !pending_opcode_.has_value() && write_queue_.empty() && !writing_) {
        closeConnection("drained");
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
    auto on_write_done = [this, self](std::error_code ec) {
        if (ec) {
            if (ec == asio::error::broken_pipe || ec == asio::error::connection_reset
                || ec == asio::error::operation_aborted || ec == asio::error::bad_descriptor) {
                Logger::debug("cinder tcp_connection: write failed: {}", ec.message());
            } else {
                Logger::warn("cinder tcp_connection: write failed: {}", ec.message());
            }

            if (metrics_) {
                metrics_->connectionMetrics().write_failures.fetch_add(
                    1, std::memory_order_relaxed);
            }

            write_queue_.clear();
            write_queue_bytes_ = 0;
            writing_ = false;
            closeConnection("write failure", ec);
            return;
        }

        // Recycle the completed buffer's capacity for the next encode.
        write_queue_bytes_ -= write_queue_.front().size();
        encode_buf_ = std::move(write_queue_.front());
        write_queue_.pop_front();
        resetIdleTimer();
        if (!write_queue_.empty()) {
            doWrite();
        } else {
            writing_ = false;
            if (draining_) {
                closeConnection("drained");
            } else {
                maybeRead();
            }
        }
    };
#ifdef CINDER_ENABLE_TLS
    if (ssl_stream_) {
        async_write(*ssl_stream_,
            buffer(buf.data(), buf.size()),
            asio::bind_executor(
                strand_, [on_write_done = std::move(on_write_done)](std::error_code ec, size_t) {
            on_write_done(ec);
        }));
        return;
    }
#endif
    async_write(socket_,
        buffer(buf.data(), buf.size()),
        asio::bind_executor(
            strand_, [on_write_done = std::move(on_write_done)](std::error_code ec, size_t) {
        on_write_done(ec);
    }));
}
} // namespace cinder::net
