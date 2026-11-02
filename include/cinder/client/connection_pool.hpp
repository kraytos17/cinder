#pragma once

#include <ankerl/unordered_dense.h>
#include <asio.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <chrono>
#include <flat_map>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/common/cluster_config.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/net/protocol.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {

    class ConnectionPool {
    public:

        ConnectionPool(
            const ClusterConfig& config,
            io_context& io,
            std::string node_id = {},
            std::string shared_secret = {},
            std::string client_shared_secret = {}
#ifdef CINDER_ENABLE_TLS
            ,
            asio::ssl::context* ssl_ctx = nullptr
#endif
        );

        ~ConnectionPool();
        ConnectionPool(const ConnectionPool&) = delete;
        auto operator=(const ConnectionPool&) -> ConnectionPool& = delete;
        ConnectionPool(ConnectionPool&&) = delete;
        auto operator=(ConnectionPool&&) -> ConnectionPool& = delete;

        // Synchronous send — blocks until response or error.
        auto send(const NodeId& node_id, const net::Request& req) -> Result<net::Response>;

        // Pipelined send: writes all requests back-to-back on one connection, then
        // reads all responses in order.
        auto sendBatch(const NodeId& node_id, const std::vector<net::Request>& reqs)
            -> Result<std::vector<net::Response>>;

        // Async send — callback fires on the io_context's executor.
        void sendAsync(
            const NodeId& node_id,
            const net::Request& req,
            std::move_only_function<void(Result<net::Response>)> on_done
        );

        // Async batch send — callback fires on the io_context's executor.
        void sendBatchAsync(
            const NodeId& node_id,
            const std::vector<net::Request>& reqs,
            std::move_only_function<void(Result<std::vector<net::Response>>)> on_done
        );

        void shutdown();

        // Register (or refresh) a node's address at runtime — learned from server
        // redirects for nodes the client was never configured with. Mirrors
        // TcpTransport::addAddr on the server side.
        void addAddr(const NodeId& id, const std::string& host, uint16_t port);

    private:

#ifdef CINDER_ENABLE_TLS
        using stream_type = asio::ssl::stream<tcp::socket>;
#endif

        // Per-node cached connection. Each NodeConn owns a strand (fair
        // scheduling across waiters) plus a gate that serialises the actual
        // socket exchange: the Cinder protocol is not multiplexed, and a
        // strand alone cannot serialize an exchange because every co_await
        // suspends — without the gate the next queued coroutine would start
        // overlapping async_reads/writes on the same socket and corrupt
        // framing. Distinct pool entries have distinct gates, so RPCs on
        // different entries genuinely overlap.
        struct NodeConn {
            asio::strand<io_context::executor_type> strand;
            ClusterConfig::NodeConfig addr{};
#ifdef CINDER_ENABLE_TLS
            std::unique_ptr<stream_type> stream;
#endif
            tcp::socket socket;
            bool connected = false;
            asio::experimental::concurrent_channel<void(std::error_code)> gate;

            NodeConn(io_context& io, const ClusterConfig::NodeConfig& a) :
                strand(asio::make_strand(io)),
                addr(a),
                socket(io),
                gate(strand, 1) {
                (void)gate.try_send(std::error_code{});
            }
        };

        // Round-robin pool of connections to ONE peer.
        //
        // Why more than one: the protocol is not multiplexed — a single socket
        // carries one request/response at a time, and NodeConn's gate
        // enforces that. With one connection per peer, every concurrent caller
        // to that peer queues behind the one in flight, so each pays a full RTT
        // of head-of-line blocking. Distinct connections have distinct gates,
        // so RPCs on different entries genuinely overlap; requests sharing an
        // entry still serialise, which is required for correctness.
        //
        // Small and fixed on purpose: this removes the serialisation point
        // without introducing an unbounded fan-out to every peer (which would
        // multiply fds and handshake cost across a large cluster). Callers are
        // round-robin, so a modest size is enough to overlap typical
        // concurrent-caller counts.
        static constexpr size_t K_POOL_SIZE = 4;

        struct NodePool {
            std::vector<std::shared_ptr<NodeConn>> conns;
            size_t next = 0; // round-robin cursor

            explicit NodePool(io_context& io, const ClusterConfig::NodeConfig& addr) {
                conns.reserve(K_POOL_SIZE);
                for (size_t i = 0; i < K_POOL_SIZE; ++i) {
                    conns.push_back(std::make_shared<NodeConn>(io, addr));
                }
            }
        };

        // Return the next connection for `node_id` (round-robin across the
        // per-node pool), creating the pool if needed. Shared ownership: send
        // coroutines hold a copy, so a NodeConn (and its gate) outlives
        // shutdown() even if the pool entry is cleared while in flight.
        // Caller must not hold mu_.
        auto getOrCreateConn(const NodeId& node_id) -> std::shared_ptr<NodeConn>;

        // Coroutine running on a NodeConn's strand: check connected, reconnect if
        // needed, write request(s), read response(s), decode. On any error closes
        // the connection so the next RPC reconnects on a clean socket.
        auto sendCoroutine(
            std::shared_ptr<NodeConn> conn,
            std::vector<std::byte> data,
            SpanContext parent
        ) -> asio::awaitable<Result<net::Response>>;

        // Mark unusable and release the socket. A failed exchange can leave stale
        // bytes or a half-dead connection behind; reusing it corrupts the next
        // response, so every error path goes through here.
        static void closeConn(const std::shared_ptr<NodeConn>& conn);

        // Batch coroutine: write all requests, read all responses.
        auto sendBatchCoroutine(
            std::shared_ptr<NodeConn> conn,
            std::vector<std::vector<std::byte>> all_data,
            SpanContext parent
        ) -> asio::awaitable<Result<std::vector<net::Response>>>;

        io_context& io_;
        std::string node_id_;
        std::string shared_secret_;
        std::string client_shared_secret_;
#ifdef CINDER_ENABLE_TLS
        asio::ssl::context* ssl_ctx_ = nullptr;
#endif
        std::chrono::milliseconds rpc_timeout_{0};
        mutable std::mutex mu_;
        bool stopping_ = false;
        ankerl::unordered_dense::map<NodeId, std::unique_ptr<NodePool>> pools_;
        std::flat_map<NodeId, ClusterConfig::NodeConfig> node_addrs_;
    };
} // namespace cinder
