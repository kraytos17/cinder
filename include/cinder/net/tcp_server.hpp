#pragma once

#include <asio.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/tcp_connection.hpp"

namespace cinder::net {

class TcpServer {
  public:

    TcpServer(asio::io_context& io, uint16_t port, CacheStore& store,
        const ConsistentHashRing& ring, std::string node_id);
    ~TcpServer();

    auto start() -> Result<void>;
    void shutdown();

  private:

    void do_accept();

    asio::io_context& io_;
    asio::ip::tcp::acceptor acceptor_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    std::string node_id_;
    std::vector<std::shared_ptr<TcpConnection>> connections_;
};
} // namespace cinder::net
