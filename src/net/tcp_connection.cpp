#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/hmac.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::async_read;
using asio::async_write;
using asio::buffer;

namespace cinder::net {

TcpConnection::TcpConnection(tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
    std::string_view node_id, Clock& clock, ReplicationManager* repl, int replica_factor,
    ConsistencyMode mode, GossipManager* gossip, std::shared_ptr<std::atomic<size_t>> conn_counter,
    AntiEntropyManager* anti_entropy, std::string shared_secret
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
      anti_entropy_(anti_entropy),
      node_id_(node_id),
      replica_factor_(replica_factor),
      mode_(mode),
      shared_secret_(std::move(shared_secret)),
      encode_buf_(512),
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
                Event::warn("TLS handshake failed", {{"err", ec.message()}});
                if (metrics_) {
                    metrics_->connectionMetrics().connections_closed.fetch_add(
                        1, std::memory_order_relaxed);
                }
                closeConnection("TLS handshake failed");
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
        Event::info("connection opened",
            {{"peer", std::format("{}:{}", ep.address().to_string(), ep.port())}});
    } else {
        Event::info("connection opened", {{"peer", "<unknown>"}});
    }
    resetIdleTimer();
    maybeRead();
}

void
TcpConnection::close() {
    Span span("tcp.close");
    auto self = shared_from_this();
    asio::post(strand_,
        [this, self]() { closeConnection("server shutdown", asio::error::operation_aborted); });
}

void
TcpConnection::drain() {
    auto self = shared_from_this();
    asio::post(strand_, [this, self]() {
        setDraining(true);
        if (!pending_opcode_.has_value() && write_queue_.empty() && !isWriting()) {
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
    if (isDraining()
        || (!isReading() && write_queue_.size() < K_MAX_WRITE_QUEUE
            && write_queue_bytes_ < K_MAX_WRITE_QUEUE_BYTES)) {
        setReading(true);
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
            Event::debug("closing connection", {{"reason", reason}, {"err", ec.message()}});
        } else {
            Event::debug("closing connection", {{"reason", reason}});
        }
    } else {
        Event::warn("closing connection", {{"reason", reason}});
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
    Span span("tcp.onHeader");
    if (ec) {
        return;
    }

    resetIdleTimer();
    if (read_buf_[0] != std::byte{K_MAGIC} || read_buf_[1] != std::byte{K_VERSION}) {
        Event::warn("bad protocol header",
            {{"magic", std::format("{:#x}", std::to_integer<int>(read_buf_[0]))},
                {"version", std::format("{:#x}", std::to_integer<int>(read_buf_[1]))}});
        if (metrics_) {
            metrics_->connectionMetrics().decode_failures.fetch_add(1, std::memory_order_relaxed);
        }
        closeConnection("bad protocol header");
        return;
    }

    uint32_t net_len = 0;
    std::memcpy(&net_len, &read_buf_[3], sizeof(net_len));
    payload_len_ = std::byteswap(net_len);
    if (payload_len_ > K_MAX_MESSAGE_SIZE || payload_len_ + K_FRAME_HEADER_SIZE > K_BUFFER_SIZE) {
        Event::warn("oversized payload", {{"len", std::to_string(payload_len_)}});
        if (metrics_) {
            metrics_->connectionMetrics().decode_failures.fetch_add(1, std::memory_order_relaxed);
        }
        closeConnection("oversized payload");
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
    setReading(false);
    if (ec) {
        return;
    }

    resetIdleTimer();
    auto result = decode(std::span<const std::byte>(read_buf_.data(), K_FRAME_HEADER_SIZE + bytes));
    if (!result.has_value()) {
        Event::debug("decode failed");
        if (metrics_) {
            metrics_->connectionMetrics().decode_failures.fetch_add(1, std::memory_order_relaxed);
        }

        Response res{.status = Errc::InvalidArgument, .value = std::nullopt};
        sendResponse(res);
        return;
    }

    auto decoded = std::move(result.value());
    auto& req = decoded.req;
    // Generate structured trace-id at the connection entry point if not already
    // present
    if (req.trace_id == 0) {
        static std::atomic<uint64_t> s_next_trace_id{1};
        req.trace_id = s_next_trace_id.fetch_add(1, std::memory_order_relaxed);
    }
    if (req.span_id == 0) {
        static std::atomic<uint64_t> s_next_span_id{1};
        req.span_id = s_next_span_id.fetch_add(1, std::memory_order_relaxed);
    }
    handleRequest(req, decoded.auth_token);
}

static auto
getOperationName(Opcode op) -> std::string_view {
    switch (op) {
        case Opcode::Get:
            return "tcp.get";
        case Opcode::Set:
            return "tcp.set";
        case Opcode::Del:
            return "tcp.del";
        case Opcode::Ping:
            return "tcp.ping";
        case Opcode::Replicate:
            return "replication.write";
        case Opcode::Hint:
            return "replication.hint";
        case Opcode::GetVersioned:
            return "tcp.getVersioned";
        case Opcode::Gossip:
            return "gossip.handle";
        case Opcode::AntiEntropyDigest:
            return "anti_entropy.digest";
        case Opcode::AntiEntropySync:
            return "anti_entropy.sync";
        case Opcode::AdminInfo:
            return "admin.info";
        case Opcode::AdminCluster:
            return "admin.cluster";
        case Opcode::AdminRing:
            return "admin.ring";
        case Opcode::AdminCompact:
            return "admin.compact";
        case Opcode::AdminConfigReload:
            return "admin.config_reload";
        case Opcode::AdminShutdown:
            return "admin.shutdown";
        default:
            return "unknown";
    }
}

void
TcpConnection::handleRequest(const Request& req, std::string_view auth_token) {
    pending_opcode_ = req.opcode;
    request_start_ = std::chrono::steady_clock::now();

    Span span(getOperationName(req.opcode), req.span_id);
    Event::debug("request received",
        {{"opcode", std::to_string(static_cast<int>(req.opcode))}, {"key", req.key}});

    if (metrics_) {
        switch (req.opcode) {
            case Opcode::Get:
                metrics_->opcodeMetrics().client.gets.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Set:
                metrics_->opcodeMetrics().client.sets.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Del:
                metrics_->opcodeMetrics().client.dels.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Ping:
                metrics_->opcodeMetrics().client.pings.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Replicate:
                metrics_->opcodeMetrics().repl.replicates.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::Hint:
                metrics_->opcodeMetrics().repl.hints.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::GetVersioned:
                metrics_->opcodeMetrics().repl.gets_versioned.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case Opcode::AntiEntropyDigest:
                metrics_->opcodeMetrics().repl.anti_entropy_digest.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case Opcode::AntiEntropySync:
                metrics_->opcodeMetrics().repl.anti_entropy_sync.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case Opcode::AdminInfo:
                metrics_->opcodeMetrics().admin.info.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::AdminCluster:
                metrics_->opcodeMetrics().admin.cluster.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::AdminRing:
                metrics_->opcodeMetrics().admin.ring.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::AdminCompact:
                metrics_->opcodeMetrics().admin.compact.fetch_add(1, std::memory_order_relaxed);
                break;
            case Opcode::AdminConfigReload:
                metrics_->opcodeMetrics().admin.config_reload.fetch_add(
                    1, std::memory_order_relaxed);
                break;
            case Opcode::AdminShutdown:
                metrics_->opcodeMetrics().admin.shutdown.fetch_add(1, std::memory_order_relaxed);
                break;
            default:
                break;
        }
    }

    // Replicate/Hint/Gossip/AntiEntropy*/Admin* are inter-node or admin messages
    // addressed to this node directly — skip the ring ownership check.
    bool is_internal = req.opcode == Opcode::Replicate || req.opcode == Opcode::Hint
                       || req.opcode == Opcode::Gossip || req.opcode == Opcode::AntiEntropyDigest
                       || req.opcode == Opcode::AntiEntropySync || req.opcode == Opcode::AdminInfo
                       || req.opcode == Opcode::AdminCluster || req.opcode == Opcode::AdminRing
                       || req.opcode == Opcode::AdminCompact
                       || req.opcode == Opcode::AdminConfigReload
                       || req.opcode == Opcode::AdminShutdown;

    // Node authentication — validate auth token on internal opcodes when a
    // shared_secret is configured. Client opcodes (Get/Set/Del/Ping) skip
    // auth since they go through the normal ring-ownership path.
    if (is_internal && !shared_secret_.empty()) {
        if (!verifyAuthToken(shared_secret_, std::string(node_id_), auth_token)) {
            Event::warn("auth failed",
                {{"opcode", std::to_string(static_cast<int>(req.opcode))},
                    {"node", std::string(node_id_)}});
            if (metrics_) {
                metrics_->connectionMetrics().connections_closed.fetch_add(
                    1, std::memory_order_relaxed);
            }

            sendResponse({.status = Errc::PermissionDenied, .value = "auth failed"});
            maybeRead();
            return;
        }
    }

    // Reads are served from the local store when present — a replica holds a
    // copy and can keep serving reads after the primary fails (failover read).
    // Ownership only matters for writes and for read misses (redirect the
    // client to the ring owner).
    bool is_read = req.opcode == Opcode::Get || req.opcode == Opcode::GetVersioned;
    if (!is_internal && req.opcode != Opcode::Ping && !is_read) {
        auto owner = ring_.getNode(req.key);
        if (owner != node_id_) {
            Event::debug("redirect", {{"key", req.key}, {"to", owner}});
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

                ring_.incrementLoad(node_id_);
                auto self = shared_from_this();
                auto trace_id = req.trace_id;
                auto span_id = req.span_id;
                repl_->readAsync(req.key,
                    replicas,
                    static_cast<size_t>(replica_factor_),
                    [this, self, trace_id, span_id](Result<VersionedEntry> result) {
                    ring_.decrementLoad(node_id_);
                    // Quorum completion lands on an arbitrary pool thread —
                    // hop back onto this connection's strand.
                    asio::post(strand_,
                        [this, self, result = std::move(result), trace_id, span_id]() mutable {
                        Span async_span("replication.read.quorum", span_id);
                        Response async_res;
                        if (result.has_value()) {
                            async_res.status = Errc::OK;
                            async_res.value = std::move(result->value);
                        } else {
                            async_res.status = result.error().code();
                        }

                        async_res.trace_id = trace_id;
                        async_res.span_id = span_id;
                        sendResponse(async_res);
                        maybeRead();
                    });
                },
                    trace_id,
                    span_id);
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
                res.version = entry->version();
                res.writer_node_hash = entry->writer_node_hash;
                if (entry->hasTtl()) {
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

                ring_.incrementLoad(node_id_);
                auto self = shared_from_this();
                auto trace_id = req.trace_id;
                auto span_id = req.span_id;
                repl_->writeAsync(req.key,
                    req.value,
                    req.ttl,
                    replicas,
                    mode_,
                    [this, self, trace_id, span_id](Result<void> result) {
                    ring_.decrementLoad(node_id_);
                    // Quorum completion lands on an arbitrary pool thread —
                    // hop back onto this connection's strand.
                    asio::post(strand_, [this, self, result, trace_id, span_id]() {
                        Span async_span("replication.write.quorum", span_id);
                        Response async_res{
                            .status = result.has_value() ? Errc::OK : result.error().code(),
                            .value = std::nullopt,
                        };

                        async_res.trace_id = trace_id;
                        async_res.span_id = span_id;
                        sendResponse(async_res);
                        maybeRead();
                    });
                },
                    trace_id,
                    span_id);
                return; // response sent asynchronously from the write callback
            } else {
                ring_.incrementLoad(node_id_);
                auto result = store_.put(req.key, req.value, req.ttl);
                ring_.decrementLoad(node_id_);
                res.status = result.has_value() ? Errc::OK : result.error().code();
            }

            Event::trace("opcode completed",
                {{"opcode", std::to_string(static_cast<int>(req.opcode))},
                    {"key", req.key},
                    {"status", std::to_string(static_cast<int>(res.status))}});
            break;
        }
        case Opcode::Del: {
            ring_.incrementLoad(node_id_);
            store_.remove(req.key);
            ring_.decrementLoad(node_id_);
            res.status = Errc::OK;
            Event::trace("opcode completed",
                {{"opcode", std::to_string(static_cast<int>(req.opcode))},
                    {"key", req.key},
                    {"status", std::to_string(static_cast<int>(res.status))}});
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
            entry.setVersion(req.version);
            entry.writer_node_hash = req.writer_node_hash;
            // Apply the primary's absolute wall-clock expiry on the local steady
            // basis, so all replicas expire the key at the same instant.
            if (req.expires_at.has_value()) {
                entry.expires_at = toSteadyExpiry(clock_, *req.expires_at);
                entry.setHasTtl(true);
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
        case Opcode::AntiEntropyDigest: {
            if (anti_entropy_ != nullptr) {
                // Sender identity is best-effort (same as gossip); used only
                // for logging inside the anti-entropy manager.
                anti_entropy_->onDigestRequest(
                    std::string(node_id_), req, [this](const Response& r) {
                    sendResponse(r);
                    maybeRead();
                });
                return;
            }
            res.status = Errc::OK;
            break;
        }
        case Opcode::AntiEntropySync: {
            if (anti_entropy_ != nullptr) {
                anti_entropy_->onSyncRequest(std::string(node_id_), req, [this](const Response& r) {
                    sendResponse(r);
                    maybeRead();
                });
                return;
            }
            res.status = Errc::OK;
            break;
        }
        case Opcode::AdminInfo: {
            if (admin_ && admin_->info_getter) {
                res.value = admin_->info_getter();
            }
            break;
        }
        case Opcode::AdminCluster: {
            if (admin_ && admin_->cluster_getter) {
                res.value = admin_->cluster_getter();
            }
            break;
        }
        case Opcode::AdminRing: {
            if (admin_ && admin_->ring_getter) {
                res.value = admin_->ring_getter();
            }
            break;
        }
        case Opcode::AdminCompact: {
            if (admin_ && admin_->compact_trigger) {
                admin_->compact_trigger();
            }
            break;
        }
        case Opcode::AdminConfigReload: {
            if (admin_ && admin_->config_reload_trigger) {
                admin_->config_reload_trigger();
            }
            break;
        }
        case Opcode::AdminShutdown: {
            if (admin_ && admin_->shutdown_trigger) {
                admin_->shutdown_trigger();
            }
            break;
        }
        default: {
            std::unreachable();
        }
    }

    // Echo trace context back to the client for correlation.
    res.trace_id = req.trace_id;
    res.span_id = req.span_id;
    sendResponse(res);
    maybeRead();
}

void
TcpConnection::sendResponse(const Response& res) {
    Span span("tcp.sendResponse");
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
        Event::warn("encode failed",
            {{"opcode",
                pending_opcode_.has_value() ? std::to_string(std::to_underlying(*pending_opcode_))
                                            : "-1"}});
        if (metrics_) {
            metrics_->connectionMetrics().write_failures.fetch_add(1, std::memory_order_relaxed);
        }
        closeConnection("encode failed");
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
    if (!isWriting()) {
        doWrite();
    }
    // A drain with no pending request and nothing left to write (e.g. the
    // decode-failure path replies without a queued write) can finish here.
    if (isDraining() && !pending_opcode_.has_value() && write_queue_.empty() && !isWriting()) {
        closeConnection("drained");
    }
}

void
TcpConnection::doWrite() {
    Span span("tcp.doWrite");
    if (write_queue_.empty()) {
        setWriting(false);
        return;
    }

    setWriting(true);
    auto self = shared_from_this();
    auto& buf = write_queue_.front();
    auto on_write_done = [this, self](std::error_code ec) {
        if (ec) {
            if (ec == asio::error::broken_pipe || ec == asio::error::connection_reset
                || ec == asio::error::operation_aborted || ec == asio::error::bad_descriptor) {
                Event::debug("write failed", {{"err", ec.message()}});
            } else {
                Event::warn("write failed", {{"err", ec.message()}});
            }

            if (metrics_) {
                metrics_->connectionMetrics().write_failures.fetch_add(
                    1, std::memory_order_relaxed);
            }

            write_queue_.clear();
            write_queue_bytes_ = 0;
            setWriting(false);
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
            setWriting(false);
            if (isDraining()) {
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
