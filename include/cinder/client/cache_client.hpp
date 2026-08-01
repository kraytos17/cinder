#pragma once

#include <asio.hpp>
#include <chrono>
#include <optional>
#include <string>

#include "cinder/client/connection_pool.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"

namespace cinder {

class CacheClient {
  public:

    explicit CacheClient(ClusterConfig config);
    ~CacheClient();
    CacheClient(const CacheClient&) = delete;
    auto operator=(const CacheClient&) -> CacheClient& = delete;
    CacheClient(CacheClient&&) = delete;
    auto operator=(CacheClient&&) -> CacheClient& = delete;

    auto set(const std::string& key, const std::string& value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void>;
    auto get(const std::string& key) -> std::optional<std::string>;
    auto remove(const std::string& key) -> bool;

  private:

    auto routePrimary(std::string_view key) const -> NodeId;

    asio::io_context io_ctx_;
    ConsistentHashRing ring_;
    ConnectionPool pool_;
};
} // namespace cinder
