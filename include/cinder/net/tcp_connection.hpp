#pragma once

#include <array>
#include <asio.hpp>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#include <optional>
#endif

#include "cinder/cluster/clock.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

using asio::ip::tcp;

namespace cinder {
class ReplicationManager;
class GossipManager;
} // namespace cinder

namespace cinder::net {

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
  public:

    static constexpr size_t K_BUFFER_SIZE = 65'536;
    static constexpr size_t K_MAX_WRITE_QUEUE = 64;

    TcpConnection(
        tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring, std::string node_id,
        Clock& clock, ReplicationManager* repl = nullptr, int replica_factor = 1,
        ConsistencyMode mode = ConsistencyMode::Async, GossipManager* gossip = nullptr
#ifdef CINDER_ENABLE_TLS
        ,
        asio::ssl::context* ssl_ctx = nullptr
#endif
    );

    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    auto operator=(const TcpConnection&) -> TcpConnection& = delete;
    TcpConnection(TcpConnection&&) = delete;
    auto operator=(TcpConnection&&) -> TcpConnection& = delete;

    void start();

    [[nodiscard]] auto isAlive() const -> bool { return socket_.is_open(); }

  private:

    void doReadHeader();
    void onHeader(std::error_code ec, size_t bytes);
    void doReadPayload(size_t len);
    void onPayload(std::error_code ec, size_t bytes);

    void handleRequest(const Request& req);
    void sendResponse(const Response& res);

    void doWrite();
    void onWrite(std::error_code ec, size_t bytes);
    void maybeRead();

    // Async read/write helpers that route through SSL when active.
    template <typename MutableBufferSequence, typename Handler>
    void doAsyncRead(const MutableBufferSequence& buf, Handler handler);
    template <typename ConstBufferSequence, typename Handler>
    void doAsyncWrite(const ConstBufferSequence& buf, Handler handler);

    tcp::socket socket_;
#ifdef CINDER_ENABLE_TLS
    std::optional<asio::ssl::stream<tcp::socket&>> ssl_stream_;
#endif
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    Clock& clock_;
    std::string node_id_;
    ReplicationManager* repl_;
    int replica_factor_;
    ConsistencyMode mode_;
    GossipManager* gossip_;

    std::array<std::byte, K_BUFFER_SIZE> read_buf_;
    size_t payload_len_ = 0;

    std::deque<std::vector<std::byte>> write_queue_;
    std::vector<std::byte> encode_buf_; // scratch; recycles write_queue_ capacity
    bool writing_ = false;
    bool reading_ = false;
};
} // namespace cinder::net
