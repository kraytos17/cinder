#include "cinder/net/tcp_server.hpp"

#include <asio.hpp>
#include <memory>
#include <utility>

#include "cinder/cluster/gossip.hpp"
#include "cinder/common/logger.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/net/http_parser.hpp"
#include "cinder/node/replication_manager.hpp"

using asio::io_context;
using asio::ip::tcp;

namespace cinder::net {

TcpServer::TcpServer(io_context& io, uint16_t port, CacheStore& store,
    const ConsistentHashRing& ring, std::string node_id, Clock& clock, ReplicationManager* repl,
    int replica_factor, ConsistencyMode mode, GossipManager* gossip, uint16_t metrics_port,
    MetricsCollector* metrics, std::function<std::string()> config_getter
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : strand_(asio::make_strand(io)),
      acceptor_(io, tcp::endpoint(tcp::v4(), port)),
      store_(store),
      ring_(ring),
      clock_(clock),
      node_id_(std::move(node_id)),
      repl_(repl),
      replica_factor_(replica_factor),
      mode_(mode),
      gossip_(gossip),
      metrics_(metrics)
#ifdef CINDER_ENABLE_TLS
      ,
      ssl_ctx_(ssl_ctx)
#endif
      ,
      config_getter_(std::move(config_getter)) {
    std::error_code ec;
    acceptor_.set_option(tcp::acceptor::reuse_address(true), ec);
    if (metrics_port > 0 && metrics_) {
        metrics_acceptor_ =
            std::make_unique<tcp::acceptor>(io, tcp::endpoint(tcp::v4(), metrics_port));
        metrics_acceptor_->set_option(tcp::acceptor::reuse_address(true), ec);
    }
}

TcpServer::~TcpServer() {
    std::error_code ec;
    acceptor_.close(ec);
    if (metrics_acceptor_) {
        metrics_acceptor_->close(ec);
    }
}

auto
TcpServer::start() -> Result<void> {
    std::error_code ec;
    auto ep = acceptor_.local_endpoint(ec);
    if (!ec) {
        Logger::info("cinder tcp_server: listening on port={}", ep.port());
    }

    asio::post(asio::bind_executor(strand_, [this]() { doAccept(); }));
    if (metrics_acceptor_) {
        auto mep = metrics_acceptor_->local_endpoint(ec);
        if (!ec) {
            Logger::info("cinder tcp_server: metrics HTTP listening on port={}", mep.port());
        }
        asio::post(asio::bind_executor(strand_, [this]() { doAcceptMetrics(); }));
    }
    return ok();
}

void
TcpServer::shutdown() {
    asio::post(asio::bind_executor(strand_, [this] {
        stopping_ = true;
        std::error_code ec;
        acceptor_.close(ec);
        if (metrics_acceptor_) {
            metrics_acceptor_->close(ec);
        }
        // Drain every connection: let in-flight requests (and their queued
        // responses) complete within K_DRAIN_TIMEOUT before force-closing.
        // Connections that are idle close immediately; the drain backstop on
        // each busy connection force-closes it after the grace period. Each
        // drain() captures its own shared_ptr, so clearing the vector is safe.
        for (auto& conn : connections_) {
            if (conn) {
                conn->drain();
            }
        }
        connections_.clear();
    }));
}

void
TcpServer::doAccept() {
    acceptor_.async_accept(
        asio::bind_executor(strand_, [this](std::error_code ec, tcp::socket socket) {
        if (!ec) {
            if (stopping_) {
                std::error_code close_ec;
                socket.close(close_ec);
                return;
            }

            std::erase_if(connections_,
                [](const std::shared_ptr<TcpConnection>& c) static { return !c->isAlive(); });

            if (active_connections_.load(std::memory_order_relaxed) >= K_MAX_CONNECTIONS) {
                std::error_code close_ec;
                socket.close(close_ec);
                Logger::warn(
                    "cinder tcp_server: rejecting connection, max={} reached", K_MAX_CONNECTIONS);
            } else {
                active_connections_.fetch_add(1, std::memory_order_relaxed);
                std::shared_ptr<std::atomic<size_t>> counter(&active_connections_,
                    [](auto*) {}); // TcpConnection dtor decrements; we own the counter

                auto conn = std::make_shared<TcpConnection>(std::move(socket),
                    store_,
                    ring_,
                    node_id_,
                    clock_,
                    repl_,
                    replica_factor_,
                    mode_,
                    gossip_,
                    counter
#ifdef CINDER_ENABLE_TLS
                    ,
                    ssl_ctx_
#endif
                );

                connections_.push_back(conn);
                conn->start();
            }
        } else if (ec != asio::error::operation_aborted) {
            Logger::warn("cinder tcp_server: accept error: {}", ec.message());
        }
        if (!stopping_ && acceptor_.is_open()) {
            doAccept();
        }
    }));
}

void
TcpServer::doAcceptMetrics() {
    metrics_acceptor_->async_accept(
        asio::bind_executor(strand_, [this](std::error_code ec, tcp::socket socket) {
        if (ec) {
            if (ec != asio::error::operation_aborted && !stopping_) {
                Logger::warn("cinder tcp_server: metrics accept error: {}", ec.message());
                doAcceptMetrics();
            }
            return;
        }

        auto buf = std::make_shared<std::array<char, 4'096>>();
        asio::async_read(socket,
            asio::buffer(*buf),
            asio::transfer_at_least(1),
            asio::bind_executor(strand_,
                [this, self = std::make_shared<tcp::socket>(std::move(socket)), buf](
                    std::error_code read_ec, std::size_t /*n*/) {
            std::string response;
            if (!read_ec) {
                auto req = parseHttpRequest(std::string_view(buf->data(), buf->size()));
                if (req.has_value() && req->method == "GET" && req->path == "/metrics") {
                    response = formatHttpResponse(metrics_->formatPrometheus());
                } else if (req.has_value() && req->method == "GET" && req->path == "/config"
                           && config_getter_) {
                    response = formatHttpResponse(config_getter_());
                } else {
                    response = formatHttp404();
                }
            } else {
                response = formatHttp404();
            }

            asio::async_write(*self,
                asio::buffer(response),
                [self](std::error_code /*write_ec*/, std::size_t /*written*/) {
                std::error_code close_ec;
                self->close(close_ec);
            });
        }));
        if (!stopping_ && metrics_acceptor_ && metrics_acceptor_->is_open()) {
            doAcceptMetrics();
        }
    }));
}
} // namespace cinder::net
