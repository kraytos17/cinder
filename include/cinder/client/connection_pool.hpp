#pragma once

#include <asio.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder {

struct ClusterConfig {
    struct NodeConfig {
        NodeId id;
        std::string host;
        uint16_t port = 0;
    };

    std::vector<NodeConfig> nodes;
};

class ConnectionPool {
  public:

    ConnectionPool(const ClusterConfig& config, io_context& io);
    ~ConnectionPool();
    ConnectionPool(const ConnectionPool&) = delete;
    auto operator=(const ConnectionPool&) -> ConnectionPool& = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    auto operator=(ConnectionPool&&) -> ConnectionPool& = delete;

    auto send(const NodeId& node_id, const net::Request& req) -> Result<net::Response>;

    // Pipelined send: writes all requests back-to-back on one connection, then
    // reads all responses in order. Responses correspond 1:1 to `reqs`.
    auto sendBatch(const NodeId& node_id, const std::vector<net::Request>& reqs)
        -> Result<std::vector<net::Response>>;

    void shutdown();

  private:

    struct PoolEntry {
        tcp::socket socket;
        bool connected = false;
        std::vector<std::byte> send_buf; // reused across sends on this connection
        std::vector<std::byte> recv_buf; // reused across receives
    };

    auto getOrConnect(const NodeId& node_id) -> Result<PoolEntry*>;
    static auto readExactly(tcp::socket& s, std::span<std::byte> buf) -> Result<void>;
    static auto sendFramed(PoolEntry& entry, const net::Request& req) -> Result<void>;
    static auto recvFramed(PoolEntry& entry) -> Result<net::Response>;

    io_context& io_;
    std::unordered_map<NodeId, PoolEntry> connections_;
    std::unordered_map<NodeId, ClusterConfig::NodeConfig> node_addrs_;
};
} // namespace cinder
