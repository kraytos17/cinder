#include "cinder/client/connection_pool.hpp"

#include <array>
#include <exception>
#include <string>
#include <utility>
#include <vector>

#include "cinder/common/hmac.hpp"
#include "cinder/common/tracing.hpp"

using asio::async_connect;
using asio::async_read;
using asio::async_write;
using asio::buffer;
using asio::error_code;
using asio::io_context;

namespace {
    auto poolError(bool timed_out, const char* phase, const error_code& ec) -> cinder::Error {
        if (timed_out) {
            return cinder::Error(cinder::Errc::Timeout, "RPC deadline exceeded");
        }
        return cinder::Error(cinder::Errc::NotReady, std::string(phase) + ": " + ec.message());
    }
} // namespace

namespace cinder {

    void ConnectionPool::closeConn(NodeConn& conn) {
        conn.connected = false;
        std::error_code ec;
#ifdef CINDER_ENABLE_TLS
        if (conn.stream) {
            conn.stream->lowest_layer().shutdown(tcp::socket::shutdown_both, ec);
            conn.stream->lowest_layer().close(ec);
            conn.stream.reset();
        }
#endif
        conn.socket.shutdown(tcp::socket::shutdown_both, ec);
        conn.socket.close(ec);
    }

    ConnectionPool::ConnectionPool(
        const ClusterConfig& config,
        io_context& io,
        std::string node_id,
        std::string shared_secret
#ifdef CINDER_ENABLE_TLS
        ,
        asio::ssl::context* ssl_ctx
#endif
    ) :
        io_(io),
        node_id_(std::move(node_id)),
        shared_secret_(std::move(shared_secret))
#ifdef CINDER_ENABLE_TLS
        ,
        ssl_ctx_(ssl_ctx)
#endif
        ,
        rpc_timeout_(milliseconds(config.rpc_timeout_ms)) {
        for (const auto& n : config.nodes) {
            node_addrs_.insert_or_assign(n.id, n);
        }
    }

    ConnectionPool::~ConnectionPool() {
        shutdown();
    }

    void ConnectionPool::shutdown() {
        std::scoped_lock lock(mu_);
        if (stopping_) {
            return;
        }

        stopping_ = true;
        for (auto& [id, pool] : pools_) {
            if (!pool) {
                continue;
            }
            for (auto& conn : pool->conns) {
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

        pools_.clear();
        node_addrs_.clear();
    }

    void ConnectionPool::addAddr(const NodeId& id, const std::string& host, uint16_t port) {
        std::scoped_lock lock(mu_);
        ClusterConfig::NodeConfig cfg{id, host, port};
        node_addrs_.insert_or_assign(id, cfg);
        // Refresh every pooled connection's address too: a stale addr on an
        // existing (possibly disconnected) NodeConn would make the next RPC
        // resolve the old endpoint. Only a disconnected entry can safely be
        // re-pointed; a live one is mid-exchange on the old peer.
        if (auto it = pools_.find(id); it != pools_.end() && it->second) {
            for (auto& conn : it->second->conns) {
                if (conn && !conn->connected) {
                    conn->addr = cfg;
                }
            }
        }
    }

    auto ConnectionPool::getOrCreateConn(const NodeId& node_id) -> NodeConn& {
        std::scoped_lock lock(mu_);
        auto it = pools_.find(node_id);
        if (it == pools_.end() || !it->second) {
            auto addr_it = node_addrs_.find(node_id);
            auto addr = (addr_it != node_addrs_.end()) ? addr_it->second
                                                       : ClusterConfig::NodeConfig{node_id, {}, 0};
            auto pool = std::make_unique<NodePool>(io_, addr);
            it = pools_.insert_or_assign(node_id, std::move(pool)).first;
        }

        auto& pool = *it->second;
        // Round-robin so consecutive callers land on different strands and
        // overlap; wrap with modulo so the cursor cannot run away.
        NodeConn& chosen = *pool.conns[pool.next % pool.conns.size()];
        pool.next = (pool.next + 1) % pool.conns.size();
        return chosen;
    }

    auto ConnectionPool::sendCoroutine(NodeConn& conn, std::vector<std::byte> data)
        -> asio::awaitable<Result<net::Response>> {
        Span span("pool.send");
        std::error_code ec;
        auto ex = co_await asio::this_coro::executor; // NOLINT

        // RPC deadline: on expiry cancel the socket so the pending I/O aborts with
        // operation_aborted; poolError() then maps that to Errc::Timeout.
        bool timed_out = false;
        asio::steady_timer timer(ex);
        if (rpc_timeout_.count() > 0) {
            timer.expires_after(rpc_timeout_);
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
        }
        if (!conn.connected) {
            CINDER_DEBUG("reconnecting node", {"node", conn.addr.host});
            tcp::resolver resolver(ex);
            auto endpoints = co_await resolver.async_resolve(
                conn.addr.host,
                std::to_string(conn.addr.port),
                asio::redirect_error(ec)
            );
            if (ec) {
                closeConn(conn);
                co_return err<net::Response>(poolError(timed_out, "resolve", ec));
            }
#ifdef CINDER_ENABLE_TLS
            if (ssl_ctx_) {
                conn.stream = std::make_unique<stream_type>(ex, *ssl_ctx_);
                co_await async_connect(
                    conn.stream->lowest_layer(),
                    endpoints,
                    asio::redirect_error(ec)
                );
                if (ec) {
                    closeConn(conn);
                    co_return err<net::Response>(poolError(timed_out, "connect", ec));
                }

                co_await conn.stream->async_handshake(
                    asio::ssl::stream_base::client,
                    asio::redirect_error(ec)
                );
                if (ec) {
                    closeConn(conn);
                    co_return err<net::Response>(poolError(timed_out, "tls handshake", ec));
                }
            } else {
                co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
                if (ec) {
                    closeConn(conn);
                    co_return err<net::Response>(poolError(timed_out, "connect", ec));
                }
            }
#else
            co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
            if (ec) {
                closeConn(conn);
                co_return err<net::Response>(poolError(timed_out, "connect", ec));
            }
#endif
            conn.connected = true;
            CINDER_DEBUG("connected to node", {"node", conn.addr.host});
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
            closeConn(conn);
            CINDER_WARN("send failed", {"node", conn.addr.host}, {"phase", "write"});
            co_return err<net::Response>(poolError(timed_out, "write", ec));
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
            closeConn(conn);
            co_return err<net::Response>(poolError(timed_out, "read header", ec));
        }
        if (header[0] != std::byte{net::K_MAGIC}) {
            closeConn(conn);
            // Framing desync on a reused connection (stale bytes / half-dead
            // peer) — the socket is already closed so the next RPC reconnects
            // clean. NotReady (not InvalidArgument) so the caller retries.
            co_return err<net::Response>(Error(Errc::NotReady, "bad magic in response"));
        }

        uint32_t payload_len = 0;
        std::memcpy(&payload_len, &header[3], sizeof(payload_len));
        payload_len = std::byteswap(payload_len);
        if (payload_len > net::K_MAX_MESSAGE_SIZE) {
            closeConn(conn);
            co_return err<net::Response>(Error(Errc::NotReady, "response payload too large"));
        }

        std::vector<std::byte> frame(net::K_FRAME_HEADER_SIZE + payload_len);
        std::memcpy(frame.data(), header.data(), net::K_FRAME_HEADER_SIZE);
        if (payload_len > 0) {
#ifdef CINDER_ENABLE_TLS
            if (conn.stream) {
                co_await async_read(
                    *conn.stream,
                    buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                    asio::redirect_error(ec)
                );
            } else {
                co_await async_read(
                    conn.socket,
                    buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                    asio::redirect_error(ec)
                );
            }
#else
            co_await async_read(
                conn.socket,
                buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                asio::redirect_error(ec)
            );
#endif
            if (ec) {
                closeConn(conn);
                co_return err<net::Response>(poolError(timed_out, "read payload", ec));
            }
        }

        timer.cancel();
        // A decode failure here means the bytes on the wire did not match the
        // frame we just read — same reused-socket desync class as the framing
        // checks above, so close and report it as transient. decodeResponse
        // itself keeps returning InvalidArgument (it is a pure function and
        // callers of it directly are validating their own input); the wire
        // path re-maps it.
        auto decoded = net::decodeResponse(frame);
        if (!decoded.has_value()) {
            closeConn(conn);
            // Re-map the wire desync to a retryable code while KEEPING the original
            // error as the cause, so the InvalidArgument + source_location from
            // decodeResponse survive for diagnosis.
            co_return std::unexpected(
                asRetryable(decoded.error(), "response decode failed on reused socket")
            );
        }
        co_return decoded;
    }

    auto ConnectionPool::sendBatchCoroutine(
        NodeConn& conn,
        std::vector<std::vector<std::byte>> all_data
    ) -> asio::awaitable<Result<std::vector<net::Response>>> {
        Span span("pool.send_batch");
        std::error_code ec;
        auto ex = co_await asio::this_coro::executor; // NOLINT
        bool timed_out = false;
        asio::steady_timer timer(ex);
        if (rpc_timeout_.count() > 0) {
            timer.expires_after(rpc_timeout_);
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
        }
        if (!conn.connected) {
            tcp::resolver resolver(ex);
            auto endpoints = co_await resolver.async_resolve(
                conn.addr.host,
                std::to_string(conn.addr.port),
                asio::redirect_error(ec)
            );
            if (ec) {
                closeConn(conn);
                co_return err<std::vector<net::Response>>(poolError(timed_out, "resolve", ec));
            }
#ifdef CINDER_ENABLE_TLS
            if (ssl_ctx_) {
                conn.stream = std::make_unique<stream_type>(ex, *ssl_ctx_);
                co_await async_connect(
                    conn.stream->lowest_layer(),
                    endpoints,
                    asio::redirect_error(ec)
                );
                if (ec) {
                    closeConn(conn);
                    co_return err<std::vector<net::Response>>(poolError(timed_out, "connect", ec));
                }

                co_await conn.stream->async_handshake(
                    asio::ssl::stream_base::client,
                    asio::redirect_error(ec)
                );
                if (ec) {
                    closeConn(conn);
                    co_return err<std::vector<net::Response>>(
                        poolError(timed_out, "tls handshake", ec)
                    );
                }
            } else {
                co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
                if (ec) {
                    closeConn(conn);
                    co_return err<std::vector<net::Response>>(poolError(timed_out, "connect", ec));
                }
            }
#else
            co_await async_connect(conn.socket, endpoints, asio::redirect_error(ec));
            if (ec) {
                closeConn(conn);
                co_return err<std::vector<net::Response>>(poolError(timed_out, "connect", ec));
            }
#endif
            conn.connected = true;
        }

#ifdef CINDER_ENABLE_TLS
        auto write_all = [&] -> asio::awaitable<void> {
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
        co_await write_all();
#else
        for (auto& data : all_data) {
            co_await async_write(conn.socket, buffer(data), asio::redirect_error(ec));
            if (ec) {
                break;
            }
        }
#endif
        if (ec) {
            closeConn(conn);
            co_return err<std::vector<net::Response>>(poolError(timed_out, "write", ec));
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
                closeConn(conn);
                co_return err<std::vector<net::Response>>(poolError(timed_out, "read header", ec));
            }
            if (header[0] != std::byte{net::K_MAGIC}) {
                closeConn(conn);
                co_return err<std::vector<net::Response>>(
                    Error(Errc::NotReady, "bad magic in response")
                );
            }

            uint32_t payload_len = 0;
            std::memcpy(&payload_len, &header[3], sizeof(payload_len));
            payload_len = std::byteswap(payload_len);
            if (payload_len > net::K_MAX_MESSAGE_SIZE) {
                closeConn(conn);
                co_return err<std::vector<net::Response>>(
                    Error(Errc::NotReady, "response payload too large")
                );
            }

            std::vector<std::byte> frame(net::K_FRAME_HEADER_SIZE + payload_len);
            std::memcpy(frame.data(), header.data(), net::K_FRAME_HEADER_SIZE);
            if (payload_len > 0) {
#ifdef CINDER_ENABLE_TLS
                if (conn.stream) {
                    co_await async_read(
                        *conn.stream,
                        buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                        asio::redirect_error(ec)
                    );
                } else {
                    co_await async_read(
                        conn.socket,
                        buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                        asio::redirect_error(ec)
                    );
                }
#else
                co_await async_read(
                    conn.socket,
                    buffer(frame.data() + net::K_FRAME_HEADER_SIZE, payload_len),
                    asio::redirect_error(ec)
                );
#endif
                if (ec) {
                    closeConn(conn);
                    co_return err<std::vector<net::Response>>(
                        poolError(timed_out, "read payload", ec)
                    );
                }
            }
            // Same reused-socket desync rationale as the single-RPC path: a
            // decode failure here is transient, and .value() would otherwise
            // throw std::bad_expected_access into the co_spawn handler.
            auto decoded = net::decodeResponse(frame);
            if (!decoded.has_value()) {
                closeConn(conn);
                // Preserve provenance (see the single-RPC path): keep the original
                // decode error as the cause while re-mapping to a retryable code.
                co_return std::unexpected(
                    asRetryable(decoded.error(), "batch response decode failed")
                );
            }
            responses.push_back(std::move(decoded.value()));
        }

        timer.cancel();
        co_return ok(std::move(responses));
    }

    void ConnectionPool::sendAsync(
        const NodeId& node_id,
        const net::Request& req,
        std::function<void(Result<net::Response>)> on_done
    ) {
        {
            std::scoped_lock lock(mu_);
            if (stopping_) {
                on_done(err<net::Response>(Error(Errc::NotReady, "pool shutting down")));
                return;
            }
            if (!node_addrs_.contains(node_id)) {
                // Not a permanent client error: we may simply not know this
                // node's address yet (mid-join, or a redirect we have not
                // followed). Transient so the caller retries — a follow-up
                // redirect or refreshAfterFailure re-resolves via the ring.
                on_done(err<net::Response>(Error(Errc::NotReady, "unknown node")));
                return;
            }
        }

        std::string auth_token;
        if (!shared_secret_.empty() && !node_id_.empty()) {
            auth_token = generateAuthToken(shared_secret_, node_id_);
        }

        auto encoded = net::encode(req, auth_token);
        if (!encoded.has_value()) {
            on_done(err<net::Response>(encoded.error()));
            return;
        }

        auto& conn = getOrCreateConn(node_id);
        asio::co_spawn(
            conn.strand,
            sendCoroutine(conn, std::move(encoded.value())),
            [cb = std::move(on_done)](std::exception_ptr ep, Result<net::Response> result) mutable {
            if (ep) {
                cb(err<net::Response>(Error(Errc::NotReady, "coroutine failed")));
                return;
            }
            cb(std::move(result));
        }
        );
    }

    void ConnectionPool::sendBatchAsync(
        const NodeId& node_id,
        const std::vector<net::Request>& reqs,
        std::function<void(Result<std::vector<net::Response>>)> on_done
    ) {
        if (reqs.empty()) {
            on_done(ok(std::vector<net::Response>{}));
            return;
        }

        {
            std::scoped_lock lock(mu_);
            if (stopping_) {
                on_done(
                    err<std::vector<net::Response>>(Error(Errc::NotReady, "pool shutting down"))
                );
                return;
            }
            if (!node_addrs_.contains(node_id)) {
                on_done(err<std::vector<net::Response>>(Error(Errc::NotReady, "unknown node")));
                return;
            }
        }

        std::vector<std::vector<std::byte>> all_data;
        all_data.reserve(reqs.size());
        std::string auth_token;
        if (!shared_secret_.empty() && !node_id_.empty()) {
            auth_token = generateAuthToken(shared_secret_, node_id_);
        }
        for (const auto& req : reqs) {
            auto encoded = net::encode(req, auth_token);
            if (!encoded.has_value()) {
                on_done(err<std::vector<net::Response>>(encoded.error()));
                return;
            }
            all_data.push_back(std::move(encoded.value()));
        }

        auto& conn = getOrCreateConn(node_id);
        asio::co_spawn(
            conn.strand,
            sendBatchCoroutine(conn, std::move(all_data)),
            [cb = std::move(
                 on_done
             )](std::exception_ptr ep, Result<std::vector<net::Response>> result) mutable {
            if (ep) {
                cb(err<std::vector<net::Response>>(Error(Errc::NotReady, "coroutine failed")));
                return;
            }
            cb(std::move(result));
        }
        );
    }

    auto ConnectionPool::send(const NodeId& node_id, const net::Request& req)
        -> Result<net::Response> {
        std::promise<Result<net::Response>> promise;
        auto future = promise.get_future();
        sendAsync(node_id, req, [&promise](Result<net::Response> result) {
            promise.set_value(std::move(result));
        });
        return future.get();
    }

    auto ConnectionPool::sendBatch(const NodeId& node_id, const std::vector<net::Request>& reqs)
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
