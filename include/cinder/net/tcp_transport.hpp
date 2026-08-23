#pragma once

#include <asio.hpp>
#include <unordered_map>

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
// sendAsync/sendRequestAsync opens a fresh connection via a C++20 coroutine
// (resolve -> connect -> write -> read -> decode) and invokes the callback on
// the io thread. Never blocks the io thread.
//
// Inbound messages are NOT delivered through onMessage -- the TcpServer serves
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

    void setConfig(const ClusterConfig& config);
    void addAddr(const NodeId& id, const std::string& host, uint16_t port);
    void sendAsync(const NodeId& to, const net::Request& req,
        cinder::Transport::SendCallback on_done) override;
    void sendRequestAsync(const NodeId& to, const net::Request& req,
        cinder::Transport::RequestCallback on_done) override;
    void onMessage(MessageHandler handler) override;

  private:

    // Coroutine: resolve -> connect -> [SSL handshake]
    //  -> write request -> read response -> decode.
    auto sendCoroutine(std::string host, uint16_t port, std::vector<std::byte> data)
        -> asio::awaitable<Result<net::Response>>;

    io_context& io_;
#ifdef CINDER_ENABLE_TLS
    asio::ssl::context* ssl_ctx_ = nullptr;
#endif
    std::unordered_map<NodeId, ClusterConfig::NodeConfig> addrs_;
    MessageHandler handler_;
};
} // namespace cinder
