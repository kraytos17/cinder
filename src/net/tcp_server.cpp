#include "cinder/net/tcp_server.hpp"

#include <asio.hpp>
#include <utility>

namespace cinder::net {

TcpServer::TcpServer(asio::io_context& io, uint16_t port, CacheStore& store,
    const ConsistentHashRing& ring, std::string node_id)
    : io_(io),
      acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
      store_(store),
      ring_(ring),
      node_id_(std::move(node_id)) {}

TcpServer::~TcpServer() {
    std::error_code ec;
    acceptor_.close(ec);
}

auto
TcpServer::start() -> Result<void> {
    do_accept();
    return ok();
}

void
TcpServer::shutdown() {
    acceptor_.close();
    connections_.clear();
}

void
TcpServer::do_accept() {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec) {
            auto conn = std::make_shared<TcpConnection>(std::move(socket), store_, ring_, node_id_);
            connections_.push_back(conn);
            conn->start();
        }
        if (acceptor_.is_open()) {
            do_accept();
        }
    });
}
} // namespace cinder::net
