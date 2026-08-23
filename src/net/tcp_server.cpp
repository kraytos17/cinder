#include "cinder/net/tcp_server.hpp"

#include <asio.hpp>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder::net {

TcpServer::TcpServer(io_context& io, uint16_t port, CacheStore& store,
    const ConsistentHashRing& ring, std::string node_id, Clock& clock, ReplicationManager* repl,
    int replica_factor, ConsistencyMode mode, GossipManager* gossip
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : acceptor_(io, tcp::endpoint(tcp::v4(), port)),
      store_(store),
      ring_(ring),
      clock_(clock),
      node_id_(std::move(node_id)),
      repl_(repl),
      replica_factor_(replica_factor),
      mode_(mode),
      gossip_(gossip)
#ifdef CINDER_ENABLE_TLS
      ,
      ssl_ctx_(ssl_ctx)
#endif
{
    std::error_code ec;
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
}

TcpServer::~TcpServer() {
    std::error_code ec;
    acceptor_.close(ec);
}

auto
TcpServer::start() -> Result<void> {
    doAccept();
    return ok();
}

void
TcpServer::shutdown() {
    acceptor_.close();
    connections_.clear();
}

void
TcpServer::doAccept() {
    acceptor_.async_accept([this](std::error_code ec, tcp::socket socket) {
        if (!ec) {
            // Prune dead connections before accepting a new one.
            std::erase_if(connections_,
                [](const std::shared_ptr<TcpConnection>& c) { return !c->isAlive(); });

            auto conn = std::make_shared<TcpConnection>(std::move(socket),
                store_,
                ring_,
                node_id_,
                clock_,
                repl_,
                replica_factor_,
                mode_,
                gossip_
#ifdef CINDER_ENABLE_TLS
                ,
                ssl_ctx_
#endif
            );

            connections_.push_back(conn);
            conn->start();
        } else if (ec != asio::error::operation_aborted) {
            Logger::warn("cinder tcp_server: accept error: {}", ec.message());
        }
        if (acceptor_.is_open()) {
            doAccept();
        }
    });
}
} // namespace cinder::net
