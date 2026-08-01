#pragma once

#include <array>
#include <asio.hpp>
#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

namespace cinder::net {

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
  public:

    static constexpr size_t K_BUFFER_SIZE = 65'536;
    static constexpr size_t K_MAX_WRITE_QUEUE = 64;

    TcpConnection(asio::ip::tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
        std::string node_id);
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    auto operator=(const TcpConnection&) -> TcpConnection& = delete;
    TcpConnection(TcpConnection&&) = delete;
    auto operator=(TcpConnection&&) -> TcpConnection& = delete;

    void start();

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

    asio::ip::tcp::socket socket_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    std::string node_id_;

    std::array<std::byte, K_BUFFER_SIZE> read_buf_;
    size_t payload_len_ = 0;

    std::deque<std::vector<std::byte>> write_queue_;
    bool writing_ = false;
    bool reading_ = false;
};
} // namespace cinder::net
