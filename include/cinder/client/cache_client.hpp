#pragma once

#include <asio.hpp>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"

using asio::io_context;
using std::chrono::milliseconds;

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
        std::optional<milliseconds> ttl = std::nullopt) -> Result<void>;
    auto get(const std::string& key) -> std::optional<std::string>;
    auto remove(const std::string& key) -> bool;

    // Fetch many keys in one round trip per owner node.
    // Returns a map of key → value for keys that were found; missing/errored
    // keys are absent.
    auto multiGet(const std::vector<std::string>& keys)
        -> std::unordered_map<std::string, std::string>;

  private:

    auto routePrimary(std::string_view key) const -> NodeId;
    auto sendToOwner(const std::string& key, const net::Request& req) -> Result<net::Response>;

    io_context io_ctx_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> io_work_;
    std::jthread io_thread_;
    ConsistentHashRing ring_;
    ConnectionPool pool_;
};

// Parse a server redirect hint ("moved to <node>") into a target node id.
// Returns nullopt for non-redirect values.
[[nodiscard]] inline constexpr auto
parseRedirect(std::string_view value) -> std::optional<NodeId> {
    constexpr std::string_view K_PREFIX = "moved to ";
    if (!value.starts_with(K_PREFIX)) {
        return std::nullopt;
    }
    return NodeId(value.substr(K_PREFIX.size()));
}
} // namespace cinder
