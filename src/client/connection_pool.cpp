#include "cinder/client/connection_pool.hpp"

#include <array>
#include <asio.hpp>
#include <charconv>
#include <string_view>

#include "cinder/net/protocol.hpp"

using asio::buffer;
using asio::connect;
using asio::io_context;
using asio::transfer_exactly;
using asio::write;
using asio::ip::tcp;

namespace cinder {

ConnectionPool::ConnectionPool(const ClusterConfig& config, io_context& io
#ifdef CINDER_ENABLE_TLS
    ,
    asio::ssl::context* ssl_ctx
#endif
    )
    : io_(io)
#ifdef CINDER_ENABLE_TLS
      ,
      ssl_ctx_(ssl_ctx)
#endif
{
    for (const auto& n : config.nodes) {
        node_addrs_[n.id] = n;
    }
}

ConnectionPool::~ConnectionPool() {
    shutdown();
}

auto
ConnectionPool::send(const NodeId& node_id, const net::Request& req) -> Result<net::Response> {
    auto entry_res = getOrConnect(node_id);
    if (!entry_res.has_value()) {
        return err<net::Response>(entry_res.error());
    }

    auto& entry = *entry_res.value();
    auto send_res = sendFramed(entry, req);
    if (!send_res.has_value()) {
        entry.connected = false;
        return err<net::Response>(send_res.error());
    }

    auto recv_res = recvFramed(entry);
    if (!recv_res.has_value()) {
        entry.connected = false;
        return err<net::Response>(recv_res.error());
    }
    return ok(std::move(recv_res.value()));
}

auto
ConnectionPool::sendBatch(const NodeId& node_id, const std::vector<net::Request>& reqs)
    -> Result<std::vector<net::Response>> {
    if (reqs.empty()) {
        return ok(std::vector<net::Response>{});
    }

    auto entry_res = getOrConnect(node_id);
    if (!entry_res.has_value()) {
        return err<std::vector<net::Response>>(entry_res.error());
    }

    auto& entry = *entry_res.value();
    for (const auto& req : reqs) {
        auto send_res = sendFramed(entry, req);
        if (!send_res.has_value()) {
            entry.connected = false;
            return err<std::vector<net::Response>>(send_res.error());
        }
    }

    // Read every response in the same order.
    std::vector<net::Response> responses;
    responses.reserve(reqs.size());
    for (size_t i = 0; i < reqs.size(); i++) {
        auto recv_res = recvFramed(entry);
        if (!recv_res.has_value()) {
            entry.connected = false;
            return err<std::vector<net::Response>>(recv_res.error());
        }
        responses.push_back(std::move(recv_res.value()));
    }
    return ok(std::move(responses));
}

void
ConnectionPool::shutdown() {
    for (auto& [id, entry] : connections_) {
        if (entry.connected) {
            std::error_code ec;
#ifdef CINDER_ENABLE_TLS
            if (entry.use_tls && entry.stream) {
                entry.stream->shutdown(ec);
            } else {
                entry.socket.shutdown(tcp::socket::shutdown_both, ec);
                entry.socket.close(ec);
            }
#else
            entry.socket.shutdown(tcp::socket::shutdown_both, ec);
            entry.socket.close(ec);
#endif
            entry.connected = false;
        }
    }

    connections_.clear();
    node_addrs_.clear();
}

auto
ConnectionPool::getOrConnect(const NodeId& node_id) -> Result<PoolEntry*> {
    auto addr_it = node_addrs_.find(node_id);
    if (addr_it == node_addrs_.end()) {
        return err<PoolEntry*>(Error(Errc::NotFound, "unknown node"));
    }

    auto conn_it = connections_.find(node_id);
    if (conn_it != connections_.end() && conn_it->second.connected) {
        return ok(&conn_it->second);
    }
    if (conn_it == connections_.end()) {
#ifdef CINDER_ENABLE_TLS
        PoolEntry entry(io_);
        if (ssl_ctx_) {
            entry.stream = std::make_unique<stream_type>(io_, *ssl_ctx_);
            entry.use_tls = true;
        }
        auto emplaced = connections_.emplace(node_id, std::move(entry));
#else
        PoolEntry entry(io_);
        auto emplaced = connections_.emplace(node_id, std::move(entry));
#endif
        conn_it = emplaced.first;
    }

    auto& entry = conn_it->second;
    if (entry.connected) {
        return ok(&entry);
    }

    std::error_code ec;
    auto& addr = addr_it->second;
    tcp::resolver resolver(io_);
    std::array<char, 6> port_buf{};
    auto [end, _] = std::to_chars(port_buf.data(), port_buf.data() + port_buf.size(), addr.port);
    auto endpoints =
        resolver.resolve(addr.host, std::string_view(port_buf.data(), end - port_buf.data()), ec);
    if (ec) {
        return err<PoolEntry*>(Error(Errc::NotReady, "resolve failed: " + ec.message()));
    }

#ifdef CINDER_ENABLE_TLS
    if (entry.use_tls) {
        asio::connect(entry.stream->lowest_layer(), endpoints, ec);
        if (ec) {
            return err<PoolEntry*>(Error(Errc::NotReady, "connect failed: " + ec.message()));
        }

        entry.stream->handshake(asio::ssl::stream_base::client, ec);
        if (ec) {
            return err<PoolEntry*>(Error(Errc::NotReady, "tls handshake failed: " + ec.message()));
        }
    } else {
        connect(entry.socket, endpoints, ec);
        if (ec) {
            return err<PoolEntry*>(Error(Errc::NotReady, "connect failed: " + ec.message()));
        }
    }
#else
    connect(entry.socket, endpoints, ec);
    if (ec) {
        return err<PoolEntry*>(Error(Errc::NotReady, "connect failed: " + ec.message()));
    }
#endif
    entry.connected = true;
    return ok(&entry);
}

auto
ConnectionPool::readExactly(PoolEntry& entry, std::span<std::byte> buf) -> Result<void> {
    std::error_code ec;
    size_t n = 0;
#ifdef CINDER_ENABLE_TLS
    if (entry.use_tls) {
        n = asio::read(
            *entry.stream, buffer(buf.data(), buf.size()), transfer_exactly(buf.size()), ec);
    } else {
        n = asio::read(
            entry.socket, buffer(buf.data(), buf.size()), transfer_exactly(buf.size()), ec);
    }
#else
    n = asio::read(entry.socket, buffer(buf.data(), buf.size()), transfer_exactly(buf.size()), ec);
#endif
    if (ec || n != buf.size()) {
        return err(Error(Errc::Timeout, "read failed: " + ec.message()));
    }
    return ok();
}

auto
ConnectionPool::sendFramed(PoolEntry& entry, const net::Request& req) -> Result<void> {
    auto encoded = net::encodeInto(req, entry.send_buf);
    if (!encoded.has_value()) {
        return err(encoded.error());
    }

    std::error_code ec;
#ifdef CINDER_ENABLE_TLS
    if (entry.use_tls) {
        asio::write(*entry.stream, buffer(entry.send_buf), ec);
    } else {
        write(entry.socket, buffer(entry.send_buf), ec);
    }
#else
    write(entry.socket, buffer(entry.send_buf), ec);
#endif
    if (ec) {
        return err(Error(Errc::Timeout, "write failed: " + ec.message()));
    }
    return ok();
}

auto
ConnectionPool::recvFramed(PoolEntry& entry) -> Result<net::Response> {
    std::array<std::byte, net::K_FRAME_HEADER_SIZE> header{};
    auto header_res = readExactly(entry, header);
    if (!header_res.has_value()) {
        return err<net::Response>(header_res.error());
    }
    if (header[0] != std::byte{net::K_MAGIC}) {
        return err<net::Response>(Error(Errc::InternalError, "bad magic in response"));
    }

    uint32_t payload_len = 0;
    std::memcpy(&payload_len, &header[3], sizeof(payload_len));
    payload_len = std::byteswap(payload_len);
    if (payload_len > net::K_MAX_MESSAGE_SIZE) {
        return err<net::Response>(Error(Errc::InternalError, "response payload too large"));
    }

    entry.recv_buf.resize(net::K_FRAME_HEADER_SIZE + payload_len);
    std::memcpy(entry.recv_buf.data(), header.data(), net::K_FRAME_HEADER_SIZE);

    auto payload_span = std::span(entry.recv_buf).subspan(net::K_FRAME_HEADER_SIZE);
    auto payload_res = readExactly(entry, payload_span);
    if (!payload_res.has_value()) {
        return err<net::Response>(payload_res.error());
    }
    return net::decodeResponse(entry.recv_buf);
}
} // namespace cinder
