#pragma once

#include <asio.hpp>
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
#include "cinder/net/protocol.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {

class ConnectionPool {
  public:

    ConnectionPool(const ClusterConfig& config, io_context& io
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
    void sendAsync(const NodeId& node_id, const net::Request& req,
        std::function<void(Result<net::Response>)> on_done);

    // Async batch send — callback fires on the io_context's executor.
    void sendBatchAsync(const NodeId& node_id, std::vector<net::Request> reqs,
        std::function<void(Result<std::vector<net::Response>>)> on_done);

    void shutdown();

  private:

#ifdef CINDER_ENABLE_TLS
    using stream_type = asio::ssl::stream<tcp::socket>;
#endif

    // Per-node cached connection. Each NodeConn owns a strand that serialises
    // concurrent RPCs targeting the same peer.
    struct NodeConn {
        asio::strand<io_context::executor_type> strand;
        ClusterConfig::NodeConfig addr{};
#ifdef CINDER_ENABLE_TLS
        std::unique_ptr<stream_type> stream;
#endif
        tcp::socket socket;
        bool connected = false;

        NodeConn(io_context& io, const ClusterConfig::NodeConfig& a)
            : strand(asio::make_strand(io)),
              addr(a),
              socket(io) {}
    };

    // Return the cached connection for `node_id`, creating one if needed.
    // Caller must not hold mu_ when calling this.
    auto getOrCreateConn(const NodeId& node_id) -> NodeConn&;

    // Coroutine running on a NodeConn's strand: check connected, reconnect if
    // needed, write request(s), read response(s), decode. On any error marks
    // the connection as disconnected so the next RPC reconnects.
    auto sendCoroutine(NodeConn& conn, std::vector<std::byte> data)
        -> asio::awaitable<Result<net::Response>>;

    // Batch coroutine: write all requests, read all responses.
    auto sendBatchCoroutine(NodeConn& conn, std::vector<std::vector<std::byte>> all_data)
        -> asio::awaitable<Result<std::vector<net::Response>>>;

    io_context& io_;
#ifdef CINDER_ENABLE_TLS
    asio::ssl::context* ssl_ctx_ = nullptr;
#endif
    mutable std::mutex mu_;
    bool stopping_ = false;
    std::unordered_map<NodeId, std::unique_ptr<NodeConn>> conns_;
    std::flat_map<NodeId, ClusterConfig::NodeConfig> node_addrs_;
};
} // namespace cinder
