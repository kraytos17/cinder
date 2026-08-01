#pragma once

#include <asio.hpp>
#include <string>
#include <unordered_map>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

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

    ConnectionPool(const ClusterConfig& config, asio::io_context& io);
    ~ConnectionPool();
    ConnectionPool(const ConnectionPool&) = delete;
    auto operator=(const ConnectionPool&) -> ConnectionPool& = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    auto operator=(ConnectionPool&&) -> ConnectionPool& = delete;

    auto send(const NodeId& node_id, const net::Request& req) -> Result<net::Response>;
    void shutdown();

  private:

    struct PoolEntry {
        asio::ip::tcp::socket socket;
        bool connected = false;
    };

    auto getOrConnect(const NodeId& node_id) -> Result<PoolEntry*>;
    static auto readExactly(asio::ip::tcp::socket& s, std::span<std::byte> buf) -> Result<void>;
    static auto sendFramed(asio::ip::tcp::socket& s, const net::Request& req) -> Result<void>;
    static auto recvFramed(asio::ip::tcp::socket& s) -> Result<net::Response>;

    asio::io_context& io_;
    std::unordered_map<NodeId, PoolEntry> connections_;
    std::unordered_map<NodeId, ClusterConfig::NodeConfig> node_addrs_;
};
} // namespace cinder
