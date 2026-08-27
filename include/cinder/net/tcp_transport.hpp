#pragma once

#include <asio.hpp>
#include <flat_map>
#include <memory>
#include <mutex>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {

// Real-network async Transport: sends requests to peer nodes over TCP. Each
// target node gets a cached connection protected by a per-node strand — RPCs to
// different nodes run concurrently, RPCs to the same node are serialized.
// On any connection error the cached socket is closed; the next RPC reconnects.
//
// Inbound messages are NOT delivered through onMessage — the TcpServer serves
// them directly (a replica applies Replicate writes to its local store). The
// handler is retained only to satisfy the Transport interface.
class TcpTransport final : public Transport {
  public:

    explicit TcpTransport(io_context& io
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
    void sendAsync(const NodeId& to, const net::Request& req,
        cinder::Transport::SendCallback on_done) override;
    void sendRequestAsync(const NodeId& to, const net::Request& req,
        cinder::Transport::RequestCallback on_done) override;
    void onMessage(MessageHandler handler) override;

    // Set the per-RPC deadline. Must be called before any send.
    void setRpcTimeout(std::chrono::milliseconds timeout);

    // Close all cached sockets and cancel in-flight coroutines. Safe to call
    // multiple times. Must be called before the io_context is stopped.
    void shutdown();

  private:

    // Per-node cached connection. Each NodeConn owns a strand that serialises
    // concurrent RPCs targeting the same peer.
    struct NodeConn {
        asio::strand<asio::io_context::executor_type> strand;
        ClusterConfig::NodeConfig addr{};
#ifdef CINDER_ENABLE_TLS
        std::unique_ptr<asio::ssl::stream<tcp::socket>> stream;
#endif
        tcp::socket socket;
        bool connected = false;

        NodeConn(io_context& io, const ClusterConfig::NodeConfig& a)
            : strand(asio::make_strand(io)),
              addr(a),
              socket(io) {}
    };

    // Return the cached connection for `id`, creating one if it doesn't exist yet.
    auto getOrCreateConn(const NodeId& id) -> NodeConn&;

    // Coroutine running on a NodeConn's strand: check connected, reconnect if
    // needed, write request, read response, decode. On any error marks the
    // connection as disconnected so the next RPC reconnects.
    auto sendCoroutine(NodeConn& conn, std::vector<std::byte> data)
        -> asio::awaitable<Result<net::Response>>;

    io_context& io_;
#ifdef CINDER_ENABLE_TLS
    asio::ssl::context* ssl_ctx_ = nullptr;
#endif
    mutable std::mutex mu_;
    std::chrono::milliseconds rpc_timeout_{5'000};
    bool stopping_ = false;
    std::unordered_map<NodeId, std::unique_ptr<NodeConn>> conns_;
    std::flat_map<NodeId, ClusterConfig::NodeConfig> addrs_;
    MessageHandler handler_;
};
} // namespace cinder
