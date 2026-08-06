#pragma once

#include <asio.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

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

    // Fetch many keys in one round trip per owner node (client-side pipelining).
    // Returns a map of key → value for keys that were found; missing/errored
    // keys are absent.
    auto multiGet(const std::vector<std::string>& keys)
        -> std::unordered_map<std::string, std::string>;

  private:

    auto routePrimary(std::string_view key) const -> NodeId;
    auto sendToOwner(const std::string& key, const net::Request& req) -> Result<net::Response>;

    asio::io_context io_ctx_;
    ConsistentHashRing ring_;
    ConnectionPool pool_;
};

// Parse a server redirect hint ("moved to <node>") into a target node id.
// Returns nullopt for non-redirect values.
[[nodiscard]] auto
parseRedirect(std::string_view value) -> std::optional<NodeId>;
} // namespace cinder
