#pragma once

#include <array>
#include <asio.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#ifdef CINDER_ENABLE_TLS
#include <asio/ssl.hpp>
#endif

#include <functional>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/types.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"
#include "cinder/net/protocol.hpp"
#include "cinder/store/cache_store.hpp"

using asio::ip::tcp;

namespace cinder {
class ReplicationManager;
class GossipManager;
class AntiEntropyManager;
} // namespace cinder

namespace cinder::net {

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
  public:

    // Maximum payload we'll buffer per connection. 1MB covers almost all of the
    // requests; large values are handled by the client library, not the server.
    static constexpr size_t K_BUFFER_SIZE = 1'048'576;
    static constexpr size_t K_MAX_WRITE_QUEUE = 64;
    // Hard memory ceiling for the outbound queue. When exceeded the connection
    // is closed instead of buffering unbounded responses for a slow client.
    static constexpr size_t K_MAX_WRITE_QUEUE_BYTES = 4 * 1'024 * 1'024;
    // No complete request/response on a connection for this long -> close it.
    static constexpr auto K_IDLE_TIMEOUT = std::chrono::seconds(30);
    // Grace period granted to in-flight requests when the server is shutting
    // down. After this, connections still holding a pending request are
    // force-closed. Reuses the idle-timer machinery as the backstop.
    static constexpr auto K_DRAIN_TIMEOUT = std::chrono::seconds(2);

    TcpConnection(tcp::socket socket, CacheStore& store, const ConsistentHashRing& ring,
        std::string_view node_id, Clock& clock, ReplicationManager* repl = nullptr,
        int replica_factor = 1, ConsistencyMode mode = ConsistencyMode::Async,
        GossipManager* gossip = nullptr,
        std::shared_ptr<std::atomic<size_t>> conn_counter = nullptr,
        AntiEntropyManager* anti_entropy = nullptr, std::string shared_secret = {}
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
    void startOnStrand();

    // Force-close the socket (cancels pending reads/writes and the idle timer).
    // Safe to call from any thread; the actual close runs on the connection's
    // strand so it never races with in-flight handlers.
    void close();

    // Graceful drain: stop reading new requests but let an in-flight request
    // (and its queued response) complete before closing. If no request is
    // pending the connection closes immediately; otherwise the idle timer is
    // re-armed with K_DRAIN_TIMEOUT as a backstop. Safe to call from any thread.
    void drain();

    void setMetrics(MetricsCollector* m) { metrics_ = m; }

    // Admin opcode handlers, supplied by TcpServer after construction.
    struct AdminCallbacks {
        std::function<std::string()> info_getter;
        std::function<std::string()> cluster_getter;
        std::function<std::string()> ring_getter;
        std::function<void()> compact_trigger;
        std::function<void()> config_reload_trigger;
        std::function<void()> shutdown_trigger;
    };

    void setAdminCallbacks(std::function<std::string()> info_getter,
        std::function<std::string()> cluster_getter, std::function<std::string()> ring_getter,
        std::function<void()> compact_trigger, std::function<void()> config_reload_trigger,
        std::function<void()> shutdown_trigger) {
        if (!admin_) {
            admin_ = std::make_unique<AdminCallbacks>();
        }

        admin_->info_getter = std::move(info_getter);
        admin_->cluster_getter = std::move(cluster_getter);
        admin_->ring_getter = std::move(ring_getter);
        admin_->compact_trigger = std::move(compact_trigger);
        admin_->config_reload_trigger = std::move(config_reload_trigger);
        admin_->shutdown_trigger = std::move(shutdown_trigger);
    }

    [[nodiscard]] auto isAlive() const -> bool { return socket_.is_open(); }

    [[nodiscard]] auto isWriting() const -> bool { return flags_ & 0x01U; }

    [[nodiscard]] auto isReading() const -> bool { return flags_ & 0x02U; }

    [[nodiscard]] auto isDraining() const -> bool { return flags_ & 0x04U; }

    void setWriting(bool v) { flags_ = static_cast<uint8_t>((flags_ & ~0x01U) | (v ? 0x01U : 0U)); }

    void setReading(bool v) { flags_ = static_cast<uint8_t>((flags_ & ~0x02U) | (v ? 0x02U : 0U)); }

    void setDraining(bool v) {
        flags_ = static_cast<uint8_t>((flags_ & ~0x04U) | (v ? 0x04U : 0U));
    }

  private:

    void doReadHeader();
    void onHeader(std::error_code ec, size_t bytes);
    void doReadPayload(size_t len);
    void onPayload(std::error_code ec, size_t bytes);

    void handleRequest(const Request& req, std::string_view auth_token);
    void sendResponse(const Response& res);

    void doWrite();
    void onWrite(std::error_code ec, size_t bytes);
    void maybeRead();

    void resetIdleTimer();
    void onIdleTimeout(std::error_code ec);
    void closeConnection(const char* reason, std::error_code ec = {});

    // Async read/write helpers that route through SSL when active.
    template <typename MutableBufferSequence, typename Handler>
    void doAsyncRead(const MutableBufferSequence& buf, Handler handler);
    template <typename ConstBufferSequence, typename Handler>
    void doAsyncWrite(const ConstBufferSequence& buf, Handler handler);

    alignas(64) tcp::socket socket_;
#ifdef CINDER_ENABLE_TLS
    std::optional<asio::ssl::stream<tcp::socket&>> ssl_stream_;
#endif
    size_t payload_len_ = 0;
    uint8_t flags_ = 0; // bit 0: writing_, bit 1: reading_, bit 2: draining_
    std::optional<Opcode> pending_opcode_;
    std::chrono::steady_clock::time_point request_start_{};
    // Serializes this connection's handler chain. Completion handlers from
    // the io_context thread pool are dispatched through this strand, so all
    // connection state below is accessed from a single logical thread.
    asio::strand<asio::any_io_executor> strand_;
    asio::steady_timer idle_timer_;

    CacheStore& store_;
    const ConsistentHashRing& ring_;
    Clock& clock_;
    ReplicationManager* repl_;
    GossipManager* gossip_;
    AntiEntropyManager* anti_entropy_ = nullptr;
    std::string_view node_id_;
    int replica_factor_;
    ConsistencyMode mode_;
    // Shared secret for HMAC-based node authentication. Empty disables auth.
    std::string shared_secret_;

    std::deque<std::vector<std::byte>> write_queue_;
    size_t write_queue_bytes_ = 0;
    std::vector<std::byte> encode_buf_; // Scratch buffer, recycled across writes.
    std::shared_ptr<std::atomic<size_t>> conn_counter_;
    MetricsCollector* metrics_ = nullptr;
    // Admin opcode callbacks, populated by TcpServer after construction.
    std::unique_ptr<AdminCallbacks> admin_;

    std::array<std::byte, K_BUFFER_SIZE> read_buf_{};
};
} // namespace cinder::net
