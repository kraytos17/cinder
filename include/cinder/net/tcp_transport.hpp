#pragma once

#include <asio.hpp>
#include <unordered_map>

#include "cinder/client/connection_pool.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

using asio::ip::tcp;

namespace cinder {

// Real-network async Transport: sends requests to peer nodes over TCP. Each
// sendAsync opens a fresh connection (async resolve/connect/write/read) and
// invokes the callback on the io thread with ok() iff the peer replied
// Errc::OK. Never blocks the io thread.
//
// Inbound messages are NOT delivered through onMessage — the TcpServer serves
// them directly (a replica applies Replicate writes to its local store). The
// handler is retained only to satisfy the Transport interface.
class TcpTransport final : public Transport {
  public:

    explicit TcpTransport(asio::io_context& io);

    // Seed the node→address table (id@host:port from a ClusterConfig).
    void setConfig(const ClusterConfig& config);

    void sendAsync(const NodeId& to, const net::Request& req,
        cinder::Transport::SendCallback on_done) override;
    void onMessage(MessageHandler handler) override;

  private:

    struct OutboundRequest {
        OutboundRequest(asio::io_context& io, const std::string& host, uint16_t port,
            cinder::Transport::SendCallback cb)
            : resolver(io),
              socket(io),
              host(host),
              port(port),
              on_done(std::move(cb)) {}

        tcp::resolver resolver;
        tcp::socket socket;
        std::string host;
        uint16_t port;
        cinder::Transport::SendCallback on_done;
        std::vector<std::byte> write_buf;
        std::array<std::byte, net::K_FRAME_HEADER_SIZE> header{};
        std::vector<std::byte> frame;
        bool done = false;
    };

    void start(OutboundRequest* self);
    void connect(OutboundRequest* self, const tcp::resolver::results_type& endpoints);
    void writeFrame(OutboundRequest* self);
    void readHeader(OutboundRequest* self);
    void readPayload(OutboundRequest* self, uint32_t payload_len);
    void finishDecode(OutboundRequest* self);
    void finish(OutboundRequest* self, Result<void> result);

    asio::io_context& io_;
    std::unordered_map<NodeId, ClusterConfig::NodeConfig> addrs_;
    MessageHandler handler_;
};
} // namespace cinder
