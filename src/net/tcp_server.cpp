#include "cinder/net/tcp_server.hpp"

#include <asio.hpp>
#include <utility>

namespace cinder::net {

TcpServer::TcpServer(asio::io_context& io, uint16_t port, CacheStore& store)
    : io_(io),
      acceptor_(io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)),
      store_(store) {}

TcpServer::~TcpServer() {
    shutdown();
}

auto
TcpServer::start() -> Result<void> {
    do_accept();
    return ok();
}

void
TcpServer::shutdown() {
    acceptor_.close();
    for (auto& conn : connections_) {
        if (conn) {
            conn->start(); // no-op, just ensuring shared_ptr keeps it alive
        }
    }
    connections_.clear();
}

void
TcpServer::do_accept() {
    acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
        if (!ec) {
            auto conn = std::make_shared<TcpConnection>(std::move(socket), store_);
            connections_.push_back(conn);
            conn->start();
        }
        if (acceptor_.is_open()) {
            do_accept();
        }
    });
}
} // namespace cinder::net
