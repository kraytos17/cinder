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

    static constexpr size_t kBufferSize = 65'536;
    static constexpr size_t kMaxWriteQueue = 64;

    TcpConnection(asio::ip::tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
        std::string node_id);
    ~TcpConnection();

    void start();

  private:

    void do_read_header();
    void on_header(std::error_code ec, size_t bytes);
    void do_read_payload(size_t len);
    void on_payload(std::error_code ec, size_t bytes);

    void handle_request(const Request& req);
    void send_response(const Response& res);

    void do_write();
    void on_write(std::error_code ec, size_t bytes);
    void maybe_read();

    asio::ip::tcp::socket socket_;
    CacheStore& store_;
    const ConsistentHashRing& ring_;
    std::string node_id_;

    std::array<std::byte, kBufferSize> read_buf_;
    size_t payload_len_ = 0;

    std::deque<std::vector<std::byte>> write_queue_;
    bool writing_ = false;
    bool reading_ = false;
};
} // namespace cinder::net
