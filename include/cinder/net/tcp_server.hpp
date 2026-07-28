#pragma once

#include "cinder/common/status.hpp"
#include "cinder/net/tcp_connection.hpp"

#include <asio.hpp>
#include <cstdint>
#include <memory>
#include <vector>

namespace cinder::net {

class TcpServer {
public:
    TcpServer(asio::io_context& io, uint16_t port, CacheStore& store);
    ~TcpServer();

    auto start() -> Result<void>;
    void shutdown();

private:
    void do_accept();

    asio::io_context& io_;
    asio::ip::tcp::acceptor acceptor_;
    CacheStore& store_;
    std::vector<std::shared_ptr<TcpConnection>> connections_;
};
} // namespace cinder::net
