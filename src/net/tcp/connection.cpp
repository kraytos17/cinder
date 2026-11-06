#include <asio.hpp>
#include <cassert>
#include <chrono>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/hmac.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/net/tcp_connection.hpp"
#include "cinder/node/anti_entropy.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::async_read;
using asio::async_write;
using asio::buffer;
using std::chrono::duration_cast;
using std::chrono::nanoseconds;
using std::chrono::steady_clock;

namespace cinder::net {

    TcpConnection::TcpConnection(
        tcp::socket socket,
        CacheStore& store,
        const ConsistentHashRing& ring,
        std::string_view node_id,
        Clock& clock,
        CacheOperations& operations,
        ReplicationManager* repl,
        int replica_factor,
        ConsistencyMode mode,
        GossipManager* gossip,
        ConnectionCountGuard conn_counter,
        AntiEntropyManager* anti_entropy,
        std::string shared_secret,
        std::string client_shared_secret
#ifdef CINDER_ENABLE_TLS
        ,
        asio::ssl::context* ssl_ctx
#endif
    ) :
        socket_(std::move(socket)),
        strand_(socket_.get_executor()),
        idle_timer_(socket_.get_executor()),
        encode_buf_(512),
        read_buf_(std::make_unique<std::array<std::byte, K_BUFFER_SIZE>>()),
        store_(store),
        ring_(ring),
        clock_(clock),
        operations_(operations),
        repl_(repl),
        gossip_(gossip),
        anti_entropy_(anti_entropy),
        node_id_(node_id),
        replica_factor_(replica_factor),
        mode_(mode),
        shared_secret_(std::move(shared_secret)),
        client_shared_secret_(std::move(client_shared_secret)),
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
        // conn_counter_ guard decrements active_connections_ in its own
        // destructor
        if (metrics_) {
            metrics_->connectionMetrics().connections_closed.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }
    }

    void TcpConnection::start() {
        // Called from the accept loop on an arbitrary pool thread; all connection
        // state mutation happens on-strand.
        auto self = shared_from_this();
        asio::post(strand_, [self] mutable { self->startOnStrand(); });
    }

    void TcpConnection::startOnStrand() {
#ifdef CINDER_ENABLE_TLS
        if (ssl_stream_) {
            ssl_stream_->async_handshake(
                asio::ssl::stream_base::server,
                asio::bind_executor(strand_, [this, self = shared_from_this()](std::error_code ec) {
                if (ec) {
                    CINDER_WARN("TLS handshake failed", {"err", ec.message()});
                    if (metrics_) {
                        metrics_->connectionMetrics().connections_closed.fetch_add(
                            1,
                            std::memory_order_relaxed
                        );
                    }

                    closeConnection("TLS handshake failed");
                    return;
                }
                maybeRead();
            })
            );
            return;
        }
#endif
        std::error_code ec;
        auto ep = socket_.remote_endpoint(ec);
        if (!ec) {
            CINDER_INFO(
                "connection opened",
                {"peer", std::format("{}:{}", ep.address().to_string(), ep.port())}
            );
        } else {
            CINDER_INFO("connection opened", {"peer", "<unknown>"});
        }

        resetIdleTimer();
        maybeRead();
    }

    void TcpConnection::close() {
        Span span("tcp.close", captureTraceContext());
        auto self = shared_from_this();
        asio::post(strand_, [this, self] {
            closeConnection("server shutdown", asio::error::operation_aborted);
        });
    }

    void TcpConnection::drain() {
        auto self = shared_from_this();
        asio::post(strand_, [this, self] {
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

    void TcpConnection::maybeRead() {
        if (isDraining()
            || (!isReading()
                && write_queue_.size()
                < K_MAX_WRITE_QUEUE
                && write_queue_bytes_
                < K_MAX_WRITE_QUEUE_BYTES)) {
            setReading(true);
            doReadHeader();
        }
    }

    void TcpConnection::resetIdleTimer() {
        idle_timer_.expires_after(K_IDLE_TIMEOUT);
        std::weak_ptr<TcpConnection> weak = shared_from_this();
        idle_timer_.async_wait(asio::bind_executor(strand_, [weak](std::error_code ec) {
            if (auto self = weak.lock()) {
                self->onIdleTimeout(ec);
            }
        }));
    }

    void TcpConnection::onIdleTimeout(std::error_code ec) {
        if (ec) {
            return; // canceled or error — connection active or already closed
        }
        closeConnection("idle timeout");
    }

    void TcpConnection::closeConnection(const char* reason, std::error_code ec) {
        if (!socket_.is_open()) {
            return;
        }

        // Broken pipe / connection reset / EOF / operation aborted are
        // transient — peers closing or node shutdown. Normal lifecycle closes
        // (drained, idle timeout, server shutdown) are expected too. Log those
        // at debug, real failures at warn.
        bool transient = ec
            == asio::error::broken_pipe
            || ec
            == asio::error::connection_reset
            || ec
            == asio::error::eof
            || ec
            == asio::error::operation_aborted
            || std::string_view(reason)
            == "drained"
            || std::string_view(reason)
            == "idle timeout"
            || std::string_view(reason)
            == "server shutdown";
        if (transient) {
            if (ec) {
                CINDER_DEBUG("closing connection", {"reason", reason}, {"err", ec.message()});
            } else {
                CINDER_DEBUG("closing connection", {"reason", reason});
            }
        } else {
            CINDER_WARN("closing connection", {"reason", reason});
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

    void TcpConnection::doReadHeader() {
        auto self = shared_from_this();
#ifdef CINDER_ENABLE_TLS
        if (ssl_stream_) {
            async_read(
                *ssl_stream_,
                buffer(read_buf_->data(), K_FRAME_HEADER_SIZE),
                asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
                if (ec) {
                    closeConnection("peer closed", ec);
                    return;
                }
                onHeader(ec, K_FRAME_HEADER_SIZE);
            })
            );
            return;
        }
#endif
        async_read(
            socket_,
            buffer(read_buf_->data(), K_FRAME_HEADER_SIZE),
            asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
            if (ec) {
                closeConnection("peer closed", ec);
                return;
            }
            onHeader(ec, K_FRAME_HEADER_SIZE);
        })
        );
    }

    void TcpConnection::onHeader(std::error_code ec, size_t /*unused*/) {
        Span span("tcp.onHeader", captureTraceContext());
        if (ec) {
            closeConnection("peer closed", ec);
            return;
        }

        resetIdleTimer();
        if ((*read_buf_)[0] != std::byte{K_MAGIC} || (*read_buf_)[1] != std::byte{K_VERSION}) {
            CINDER_WARN(
                "bad protocol header",
                {"magic", std::format("{:#x}", std::to_integer<int>((*read_buf_)[0]))},
                {"version", std::format("{:#x}", std::to_integer<int>((*read_buf_)[1]))}
            );
            if (metrics_) {
                metrics_->connectionMetrics().decode_failures.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }

            closeConnection("bad protocol header");
            return;
        }

        uint32_t net_len = 0;
        std::memcpy(&net_len, &(*read_buf_)[3], sizeof(net_len));
        payload_len_ = std::byteswap(net_len);
        if (payload_len_
            > K_MAX_MESSAGE_SIZE
            || payload_len_
            + K_FRAME_HEADER_SIZE
            > K_BUFFER_SIZE) {
            CINDER_WARN("oversized payload", {"len", payload_len_});
            if (metrics_) {
                metrics_->connectionMetrics().decode_failures.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }

            closeConnection("oversized payload");
            return;
        }
        doReadPayload(payload_len_);
    }

    void TcpConnection::doReadPayload(size_t len) {
        auto self = shared_from_this();
#ifdef CINDER_ENABLE_TLS
        if (ssl_stream_) {
            async_read(
                *ssl_stream_,
                buffer(read_buf_->data() + K_FRAME_HEADER_SIZE, len),
                asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
                onPayload(ec, payload_len_);
            })
            );
            return;
        }
#endif
        async_read(
            socket_,
            buffer(read_buf_->data() + K_FRAME_HEADER_SIZE, len),
            asio::bind_executor(strand_, [this, self](std::error_code ec, size_t) {
            onPayload(ec, payload_len_);
        })
        );
    }

    void TcpConnection::onPayload(std::error_code ec, size_t bytes) {
        setReading(false);
        if (ec) {
            closeConnection("peer closed", ec);
            return;
        }

        resetIdleTimer();
        auto result =
            decode(std::span<const std::byte>(read_buf_->data(), K_FRAME_HEADER_SIZE + bytes));
        if (!result.has_value()) {
            CINDER_DEBUG("decode failed");
            if (metrics_) {
                metrics_->connectionMetrics().decode_failures.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }

            Response res{.status = Errc::InvalidArgument, .value = std::nullopt};
            sendResponse(res);
            return;
        }

        auto decoded = std::move(result.value());
        auto& req = decoded.req;
        // Generate structured trace-id at the connection entry point if not already
        // present. One fetch_add mints both IDs (trace takes the even slot,
        // span the odd): halves the globally-contended RMWs and keeps the two
        // counters off each other's cache line.
        if (req.trace_id == 0 || req.span_id == 0) {
            static std::atomic<uint64_t> s_next_id{1};
            uint64_t base = s_next_id.fetch_add(2, std::memory_order_relaxed);
            if (req.trace_id == 0) {
                req.trace_id = base;
            }
            if (req.span_id == 0) {
                req.span_id = base + 1;
            }
        }
        handleRequest(req, decoded.auth_token);
    }

    static auto getOperationName(Opcode op) -> std::string_view {
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

    auto TcpConnection::redirectValue(const NodeId& owner) -> std::string {
        std::string value = "moved to " + owner;
        if (addr_resolver_) {
            if (auto addr = addr_resolver_(owner);
                addr.has_value() && addr->second != 0 && !addr->first.empty()) {
                value += "@" + addr->first + ":" + std::to_string(addr->second);
            }
        }
        return value;
    }

    auto TcpConnection::classifyOpcode(Opcode op) -> OpcodeClass {
        switch (op) {
            case Opcode::Replicate:
            case Opcode::Hint:
            case Opcode::Gossip:
            case Opcode::AntiEntropyDigest:
            case Opcode::AntiEntropySync:
                return OpcodeClass::Replication;
            case Opcode::AdminInfo:
            case Opcode::AdminCluster:
            case Opcode::AdminRing:
            case Opcode::AdminCompact:
            case Opcode::AdminConfigReload:
            case Opcode::AdminShutdown:
                return OpcodeClass::Admin;
            default:
                return OpcodeClass::Client;
        }
    }

    void TcpConnection::recordOpcodeMetric(Opcode op) {
        if (metrics_ == nullptr) {
            return;
        }
        switch (op) {
            case Opcode::Get:
                metrics_->opcodeMetrics().client.gets.fetchAdd(1, std::memory_order_relaxed);
                break;
            case Opcode::Set:
                metrics_->opcodeMetrics().client.sets.fetchAdd(1, std::memory_order_relaxed);
                break;
            case Opcode::Del:
                metrics_->opcodeMetrics().client.dels.fetchAdd(1, std::memory_order_relaxed);
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
                    1,
                    std::memory_order_relaxed
                );
                break;
            case Opcode::AntiEntropyDigest:
                metrics_->opcodeMetrics().repl.anti_entropy_digest.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
                break;
            case Opcode::AntiEntropySync:
                metrics_->opcodeMetrics().repl.anti_entropy_sync.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
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
                    1,
                    std::memory_order_relaxed
                );
                break;
            case Opcode::AdminShutdown:
                metrics_->opcodeMetrics().admin.shutdown.fetch_add(1, std::memory_order_relaxed);
                break;
            default:
                break;
        }
    }

    void TcpConnection::denyRequest(const Request& req) {
        CINDER_WARN("auth failed", {"opcode", static_cast<int>(req.opcode)}, {"node", node_id_});
        if (metrics_) {
            metrics_->connectionMetrics().connections_closed.fetch_add(
                1,
                std::memory_order_relaxed
            );
        }

        // Status-only: a value body here would surface as fake data
        // through CacheClient::get(), which returns res.value directly.
        // Denials still echo the request trace so the caller can correlate.
        sendResponse({
            .status = Errc::PermissionDenied,
            .value = std::nullopt,
            .trace_id = req.trace_id,
            .span_id = req.span_id,
        });
        maybeRead();
    }

    auto TcpConnection::checkAuth(const Request& req, std::string_view auth_token, OpcodeClass cls)
        -> bool {
        const bool client_auth_enabled = !client_shared_secret_.empty();
        // Node authentication — validate auth token on internal opcodes when a
        // shared_secret is configured. Client opcodes (Get/Set/Del/Ping)
        // require the client credential only when client auth is enabled.
        if (cls == OpcodeClass::Replication && !shared_secret_.empty()) {
            if (!verifyAuthToken(shared_secret_, std::string(node_id_), auth_token)) {
                denyRequest(req);
                return false;
            }
        } else if (cls == OpcodeClass::Admin) {
            if (client_auth_enabled) {
                if (!verifyClientAuthToken(client_shared_secret_, auth_token)) {
                    denyRequest(req);
                    return false;
                }
            } else if (!shared_secret_.empty()) {
                if (!verifyAuthToken(shared_secret_, std::string(node_id_), auth_token)) {
                    denyRequest(req);
                    return false;
                }
            }
        } else if (client_auth_enabled) {
            // All remaining opcodes are client-facing (Get/GetVersioned/Set/Del/Ping).
            if (!verifyClientAuthToken(client_shared_secret_, auth_token)) {
                denyRequest(req);
                return false;
            }
        }
        return true;
    }

    auto TcpConnection::checkWriteOwnership(const Request& req) -> bool {
        // Reads are served from the local store when present — a replica holds a
        // copy and can keep serving reads after the primary fails (failover read).
        // Ownership only matters for writes and for read misses (redirect the
        // client to the ring owner).
        bool is_read = req.opcode == Opcode::Get || req.opcode == Opcode::GetVersioned;
        if (req.opcode == Opcode::Ping || is_read) {
            return true;
        }

        auto owner = ring_.getNode(req.key);
        if (owner == node_id_) {
            return true;
        }

        CINDER_DEBUG("redirect", {"key", req.key}, {"to", owner});
        sendRedirect(owner, req.trace_id, req.span_id);
        maybeRead();
        return false;
    }

    void TcpConnection::sendRedirect(const NodeId& owner, uint64_t trace_id, uint64_t span_id) {
        if (metrics_) {
            metrics_->connectionMetrics().redirects.fetch_add(1, std::memory_order_relaxed);
        }

        sendResponse({
            .status = Errc::NotReady,
            .value = redirectValue(owner),
            .trace_id = trace_id,
            .span_id = span_id,
        });
    }

    void TcpConnection::handleRequest(const Request& req, std::string_view auth_token) {
        // Entire request-handling chain (including the AntiEntropyManager
        // callbacks, which respond synchronously) runs here. All of
        // sendResponse/maybeRead assume strand confinement.
        assert(strand_.running_in_this_thread());
        pending_opcode_ = req.opcode;
        request_start_ = steady_clock::now();

        Span span(getOperationName(req.opcode), SpanContext{req.trace_id, req.span_id});
        CINDER_DEBUG(
            "request received",
            {"opcode", static_cast<int>(req.opcode)},
            {"key", req.key}
        );

        recordOpcodeMetric(req.opcode);
        // Replicate/Hint/Gossip/AntiEntropy*/Admin* are inter-node or admin messages
        // addressed to this node directly — skip the ring ownership check.
        const auto cls = classifyOpcode(req.opcode);
        const bool is_internal = cls != OpcodeClass::Client;
        if (!checkAuth(req, auth_token, cls)) {
            return;
        }
        if (cls == OpcodeClass::Client && !checkWriteOwnership(req)) {
            return;
        }

        Response res;
        if (dispatchRequest(req, res, is_internal)) {
            return;
        }

        // Echo trace context back to the client for correlation.
        res.trace_id = req.trace_id;
        res.span_id = req.span_id;
        sendResponse(res);
        maybeRead();
    }

    auto TcpConnection::dispatchRequest(const Request& req, Response& res, bool is_internal)
        -> bool {
        switch (req.opcode) {
            case Opcode::Get:
                handleGet(req, res, is_internal);
                return true;
            case Opcode::GetVersioned:
                return handleGetVersioned(req, res, is_internal);
            case Opcode::Set:
                handleSet(req);
                return true;
            case Opcode::Del:
                handleDelete(req);
                return true;
            case Opcode::Ping:
                res.status = Errc::OK;
                return false;
            case Opcode::Replicate:
            case Opcode::Hint:
                handleReplicate(req, res);
                return false;
            case Opcode::Gossip:
                handleGossip(req, res);
                return false;
            case Opcode::AntiEntropyDigest:
                return handleAntiEntropyDigest(req);
            case Opcode::AntiEntropySync:
                return handleAntiEntropySync(req);
            case Opcode::AdminInfo:
            case Opcode::AdminCluster:
            case Opcode::AdminRing:
            case Opcode::AdminCompact:
            case Opcode::AdminConfigReload:
            case Opcode::AdminShutdown:
                handleAdmin(req, res);
                return false;
            default: {
                std::unreachable();
            }
        }
        std::unreachable();
    }

    void TcpConnection::handleGet(const Request& req, Response& res, bool is_internal) {
        if (!is_internal) {
            auto self = shared_from_this();
            auto trace_id = req.trace_id;
            auto span_id = req.span_id;
            const bool quorum_read = replica_factor_ > 1 && repl_ != nullptr;
            if (quorum_read) {
                ring_.incrementLoad(node_id_);
            }

            operations_.getAsync(
                req.key,
                [this, self, key = req.key, trace_id, span_id, quorum_read](
                    Result<std::string> result
                ) {
                if (quorum_read) {
                    ring_.decrementLoad(node_id_);
                }
                // Operation completion may be inline or on the
                // replication completion executor. Marshal to the
                // connection strand before touching its state.
                asio::post(
                    strand_,
                    [this,
                     self,
                     key,
                     result = std::move(result),
                     trace_id,
                     span_id,
                     quorum_read] mutable {
                    Span async_span(
                        quorum_read ? "replication.read.quorum" : "tcp.get.local",
                        SpanContext{trace_id, span_id}
                    );

                    Response async_res;
                    if (result.has_value()) {
                        async_res.status = Errc::OK;
                        async_res.value = std::move(*result);
                    } else if (
                        result.error().code()
                        == Errc::NotFound
                        && !quorum_read
                        && ring_.getNode(key)
                        != node_id_
                    ) {
                        const auto owner = ring_.getNode(key);
                        async_res.value = redirectValue(owner);
                        async_res.status = Errc::NotReady;
                        if (metrics_) {
                            metrics_->connectionMetrics().redirects.fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                        }
                    } else {
                        async_res.status = result.error().code();
                    }

                    async_res.trace_id = trace_id;
                    async_res.span_id = span_id;
                    sendResponse(async_res);
                    maybeRead();
                }
                );
            },
                trace_id,
                span_id
            );
            return;
        }

        auto val = store_.get(req.key);
        if (val.has_value()) {
            res.status = Errc::OK;
            res.value = std::move(val);
        } else if (!is_internal && ring_.getNode(req.key) != node_id_) {
            sendRedirect(ring_.getNode(req.key), req.trace_id, req.span_id);
            maybeRead();
            return;
        } else {
            res.status = Errc::NotFound;
        }

        res.trace_id = req.trace_id;
        res.span_id = req.span_id;
        sendResponse(res);
        maybeRead();
    }

    auto TcpConnection::handleGetVersioned(const Request& req, Response& res, bool is_internal)
        -> bool {
        auto entry = store_.getVersioned(req.key);
        if (entry.has_value()) {
            res.status = Errc::OK;
            // Tombstones carry identity, not data: omit the value
            // but report the marker so quorum votes and repair see
            // the delete. Absent keys stay NotFound.
            if (!entry->isTombstone()) {
                res.value = std::move(entry->value);
            }

            res.version = entry->version();
            res.writer_node_hash = entry->writer_node_hash;
            res.tombstone = entry->isTombstone();
            if (entry->hasTtl()) {
                res.expires_at = toSystemExpiry(clock_, entry->expires_at);
            }
            return false;
        }
        if (!is_internal && ring_.getNode(req.key) != node_id_) {
            sendRedirect(ring_.getNode(req.key), req.trace_id, req.span_id);
            maybeRead();
            return true;
        }

        res.status = Errc::NotFound;
        return false;
    }

    void TcpConnection::handleSet(const Request& req) {
        ring_.incrementLoad(node_id_);
        auto self = shared_from_this();
        auto trace_id = req.trace_id;
        auto span_id = req.span_id;
        const bool replicated_write = replica_factor_ > 1 && repl_ != nullptr;
        operations_.setAsync(
            req.key,
            req.value,
            req.ttl,
            [this, self, key = req.key, replicated_write, trace_id, span_id](Result<void> result) {
            onWriteComplete(
                self,
                key,
                Opcode::Set,
                replicated_write,
                trace_id,
                span_id,
                std::move(result)
            );
        },
            trace_id,
            span_id
        );
        // response is sent on the connection strand
    }

    void TcpConnection::handleDelete(const Request& req) {
        ring_.incrementLoad(node_id_);
        auto self = shared_from_this();
        auto trace_id = req.trace_id;
        auto span_id = req.span_id;
        const bool replicated_delete = replica_factor_ > 1 && repl_ != nullptr;
        operations_.deleteAsync(
            req.key,
            [this, self, key = req.key, replicated_delete, trace_id, span_id](Result<void> result) {
            onWriteComplete(
                self,
                key,
                Opcode::Del,
                replicated_delete,
                trace_id,
                span_id,
                std::move(result)
            );
        },
            trace_id,
            span_id
        );
        // response is sent on the connection strand
    }

    void TcpConnection::handleReplicate(const Request& req, Response& res) {
        VersionedEntry entry;
        entry.value = req.value;
        entry.setVersion(req.version);
        entry.writer_node_hash = req.writer_node_hash;
        // Apply the primary's absolute wall-clock expiry on the local steady
        // basis, so all replicas expire the key at the same instant.
        // Tombstone GC deadlines ride the same field: the sender's
        // deadline converges the reap time across replicas.
        if (req.expires_at.has_value()) {
            entry.expires_at = toSteadyExpiry(clock_, *req.expires_at);
            entry.setHasTtl(true);
        }
        if (req.tombstone) {
            entry.setTombstone(true);
            if (!req.expires_at.has_value()) {
                // No sender deadline (hand-rolled peer or old sender):
                // fall back to a locally-computed GC deadline via the
                // versioned remove path, preserving the sender's
                // (version, writer) for LWW.
                auto del_res = store_.removeVersioned(req.key, req.version, req.writer_node_hash);
                res.status = del_res.has_value() ? Errc::OK : del_res.error().code();
                return;
            }
        }

        auto result = store_.putVersioned(req.key, std::move(entry));
        res.status = result.has_value() ? Errc::OK : result.error().code();
    }

    void TcpConnection::handleGossip(const Request& req, Response& res) {
        if (gossip_ != nullptr) {
            // Sender identity is best-effort: the connection knows only its
            // own node_id_; the sender's entry is always inside the payload.
            gossip_->handleMessage(std::string(node_id_), req);
        }
        res.status = Errc::OK;
    }

    auto TcpConnection::handleAntiEntropyDigest(const Request& req) -> bool {
        if (anti_entropy_ == nullptr) {
            return false;
        }

        // Sender identity is best-effort (same as gossip); used only
        // for logging inside the anti-entropy manager.
        auto respond = [this](const Response& r) {
            sendResponse(r);
            maybeRead();
        };
        if (req.key == "bucket-refinement-v3") {
            anti_entropy_->onBucketRefinement(std::string(node_id_), req, respond);
        } else {
            anti_entropy_->onDigestRequest(std::string(node_id_), req, respond);
        }
        return true;
    }

    auto TcpConnection::handleAntiEntropySync(const Request& req) -> bool {
        if (anti_entropy_ == nullptr) {
            return false;
        }

        anti_entropy_->onSyncRequest(std::string(node_id_), req, [this](const Response& r) {
            sendResponse(r);
            maybeRead();
        });
        return true;
    }

    void TcpConnection::handleAdmin(const Request& req, Response& res) {
        switch (req.opcode) {
            case Opcode::AdminInfo:
                if (admin_ && admin_->info_getter) {
                    res.value = admin_->info_getter();
                }
                break;
            case Opcode::AdminCluster:
                if (admin_ && admin_->cluster_getter) {
                    res.value = admin_->cluster_getter();
                }
                break;
            case Opcode::AdminRing:
                if (admin_ && admin_->ring_getter) {
                    res.value = admin_->ring_getter();
                }
                break;
            case Opcode::AdminCompact:
                if (admin_ && admin_->compact_trigger) {
                    admin_->compact_trigger();
                }
                break;
            case Opcode::AdminConfigReload:
                if (admin_ && admin_->config_reload_trigger) {
                    admin_->config_reload_trigger();
                }
                break;
            case Opcode::AdminShutdown:
                if (admin_ && admin_->shutdown_trigger) {
                    admin_->shutdown_trigger();
                }
                break;
            default:
                break;
        }
    }

    void TcpConnection::onWriteComplete(
        std::shared_ptr<TcpConnection> self,
        std::string key,
        Opcode opcode,
        bool replicated,
        uint64_t trace_id,
        uint64_t span_id,
        Result<void> result
    ) {
        ring_.decrementLoad(node_id_);
        // Replication completion may be inline or on another executor. Always
        // marshal connection-state access to this connection's strand.
        asio::post(
            strand_,
            [this,
             self = std::move(self),
             key = std::move(key),
             opcode,
             replicated,
             trace_id,
             span_id,
             result = std::move(result)] mutable {
            const bool is_set = opcode == Opcode::Set;
            Span async_span(
                replicated ? (is_set ? "replication.write.quorum" : "replication.delete.quorum")
                           : (is_set ? "tcp.set.local" : "tcp.delete.local"),
                SpanContext{trace_id, span_id}
            );

            Response async_res{
                .status = result.has_value() ? Errc::OK : result.error().code(),
                .value = std::nullopt,
            };

            async_res.trace_id = trace_id;
            async_res.span_id = span_id;
            if (!replicated) {
                CINDER_TRACE(
                    "opcode completed",
                    {"opcode", static_cast<int>(opcode)},
                    {"key", key},
                    {"status", static_cast<int>(async_res.status)}
                );
            }

            sendResponse(async_res);
            maybeRead();
        }
        );
    }

    void TcpConnection::sendResponse(const Response& res) {
        Span span("tcp.sendResponse", captureTraceContext());
        if (pending_opcode_.has_value() && metrics_) {
            auto elapsed_ns = static_cast<uint64_t>(
                duration_cast<nanoseconds>(steady_clock::now() - request_start_).count()
            );
            metrics_->opcodeMetrics().recordLatency(
                std::to_underlying(*pending_opcode_),
                elapsed_ns
            );
            pending_opcode_.reset();
        }

        // Encode into the scratch buffer, then hand ownership to the write queue.
        auto result = encodeInto(res, encode_buf_);
        if (!result.has_value()) {
            CINDER_WARN(
                "encode failed",
                {"opcode",
                 pending_opcode_.has_value() ? std::to_string(std::to_underlying(*pending_opcode_))
                                             : "-1"}
            );
            if (metrics_) {
                metrics_->connectionMetrics().write_failures.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
            }

            closeConnection("encode failed");
            return;
        }

        write_queue_.push_back(std::move(encode_buf_));
        write_queue_bytes_ += write_queue_.back().size();
        if (write_queue_bytes_ > K_MAX_WRITE_QUEUE_BYTES) {
            if (metrics_) {
                metrics_->connectionMetrics().write_failures.fetch_add(
                    1,
                    std::memory_order_relaxed
                );
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

    void TcpConnection::doWrite() {
        Span span("tcp.doWrite", captureTraceContext());
        if (write_queue_.empty()) {
            setWriting(false);
            return;
        }

        setWriting(true);
        auto self = shared_from_this();
        auto& buf = write_queue_.front();
        auto on_write_done = [this, self](std::error_code ec) {
            if (ec) {
                if (ec
                    == asio::error::broken_pipe
                    || ec
                    == asio::error::connection_reset
                    || ec
                    == asio::error::operation_aborted
                    || ec
                    == asio::error::bad_descriptor) {
                    CINDER_DEBUG("write failed", {"err", ec.message()});
                } else {
                    CINDER_WARN("write failed", {"err", ec.message()});
                }

                if (metrics_) {
                    metrics_->connectionMetrics().write_failures.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
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
            async_write(
                *ssl_stream_,
                buffer(buf.data(), buf.size()),
                asio::bind_executor(
                    strand_,
                    [on_write_done = std::move(on_write_done)](std::error_code ec, size_t) {
                on_write_done(ec);
            }
                )
            );
            return;
        }
#endif
        async_write(
            socket_,
            buffer(buf.data(), buf.size()),
            asio::bind_executor(
                strand_,
                [on_write_done = std::move(on_write_done)](std::error_code ec, size_t) {
            on_write_done(ec);
        }
            )
        );
    }
} // namespace cinder::net
