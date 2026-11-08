#pragma once

#include <ankerl/unordered_dense.h>
#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <atomic>
#include <chrono>
#include <flat_map>
#include <memory>
#include <mutex>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

using asio::io_context;
using asio::ip::tcp;
using std::chrono::milliseconds;

namespace cinder {

    // Real-network async Transport: sends requests to peer nodes over TCP. Each
    // target node gets a cached connection — one socket carrying one
    // request/response exchange at a time. The per-node strand decides *which*
    // coroutine runs next, but it cannot serialize the exchange itself: a
    // coroutine suspends mid-exchange at every co_await, letting the next
    // coroutine start overlapping async_reads/writes on the same socket and
    // corrupt framing. The per-connection gate (binary semaphore) closes that
    // hole: every exchange holds it for the full write/read sequence.
    // On any connection error the cached socket is closed; the next RPC reconnects.
    //
    // Inbound messages are NOT delivered through onMessage — the TcpServer serves
    // them directly (a replica applies Replicate writes to its local store). The
    // handler is retained only to satisfy the Transport interface.
    class TcpTransport final : public Transport {
    public:

        explicit TcpTransport(
            io_context& io,
            std::string node_id = {},
            std::string shared_secret = {}
#ifdef CINDER_ENABLE_TLS
            ,
            asio::ssl::context* ssl_ctx = nullptr
#endif
        );

        ~TcpTransport() override;

        TcpTransport(const TcpTransport&) = delete;
        auto operator=(const TcpTransport&) -> TcpTransport& = delete;
        TcpTransport(TcpTransport&&) = delete;
        auto operator=(TcpTransport&&) -> TcpTransport& = delete;

        void setConfig(const ClusterConfig& config);
        void addAddr(const NodeId& id, const std::string& host, uint16_t port);
        void sendAsync(
            const NodeId& to,
            const net::Request& req,
            cinder::Transport::SendCallback on_done
        ) override;
        void sendRequestAsync(
            const NodeId& to,
            const net::Request& req,
            cinder::Transport::RequestCallback on_done
        ) override;
        void onMessage(MessageHandler handler) override;

        // Set the per-RPC deadline. Must be called before any send.
        void setRpcTimeout(milliseconds timeout);

        // Close all cached sockets and cancel in-flight coroutines. Safe to call
        // multiple times. Must be called before the io_context is stopped.
        void shutdown();

    private:

        // Per-node cached connection. Each NodeConn owns a strand (fair
        // scheduling across waiters) plus a gate that serialises the actual
        // socket exchange — see the class comment.
        struct NodeConn {
            asio::strand<asio::io_context::executor_type> strand;
            ClusterConfig::NodeConfig addr{};
#ifdef CINDER_ENABLE_TLS
            std::unique_ptr<asio::ssl::stream<tcp::socket>> stream;
#endif
            tcp::socket socket;
            bool connected = false;
            asio::experimental::concurrent_channel<void(std::error_code)> gate;

            NodeConn(io_context& io, const ClusterConfig::NodeConfig& a) :
                strand(asio::make_strand(io)),
                addr(a),
                socket(io),
                gate(strand, 1) {
                // concurrent_channel starts empty: stock the single token so the
                // first exchange can proceed.
                (void)gate.try_send(std::error_code{});
            }
        };

        // Return the cached connection for `id`, creating one if it doesn't exist yet.
        // Shared ownership: send coroutines hold a copy, so a NodeConn (and
        // its gate) outlives shutdown() even if the map entry is destroyed
        // while an exchange is in flight.
        auto getOrCreateConn(const NodeId& id) -> std::shared_ptr<NodeConn>;

        // Coroutine running on a NodeConn's strand: check connected, reconnect if
        // needed, write request, read response, decode. On any error closes the
        // connection so the next RPC reconnects on a clean socket. The exchange
        // joins the request's trace when it carries one, else starts a fresh
        // root (background pings/gossip have no inbound parent).
        auto sendCoroutine(
            std::shared_ptr<NodeConn> conn,
            std::vector<std::byte> data,
            SpanContext parent
        ) -> asio::awaitable<Result<net::Response>>;

        // Mark unusable and release the socket. A failed exchange can leave stale
        // bytes or a half-dead connection behind; reusing it corrupts the next
        // response, so every error path goes through here.
        static void closeConn(const std::shared_ptr<NodeConn>& conn);

        io_context& io_;
        std::string node_id_;
        std::string shared_secret_;
#ifdef CINDER_ENABLE_TLS
        asio::ssl::context* ssl_ctx_ = nullptr;
#endif
        mutable std::mutex mu_;
        // Written by setRpcTimeout (config reload, any thread) and read by
        // sendCoroutine on io strands without taking mu_: plain member would
        // be a data race. Count-based atomic; the unit is milliseconds.
        std::atomic<milliseconds::rep> rpc_timeout_ms_{5000};
        // Set once by shutdown(); observed by in-flight coroutines. Atomic so
        // the check needs no lock on the send path.
        std::atomic<bool> stopping_{false};
        ankerl::unordered_dense::map<NodeId, std::shared_ptr<NodeConn>> conns_;
        std::flat_map<NodeId, ClusterConfig::NodeConfig> addrs_;
        MessageHandler handler_;
    };
} // namespace cinder
