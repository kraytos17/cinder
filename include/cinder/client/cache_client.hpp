#pragma once

#include <asio.hpp>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cinder/client/connection_pool.hpp"
#include "cinder/hashing/consistent_hash_ring.hpp"

using asio::io_context;
using std::chrono::milliseconds;

namespace cinder {

// Pure exponential backoff in ms: base * 2^attempt, saturating at int max.
[[nodiscard]] inline constexpr auto
retryBackoff(int attempt, int base_ms) -> int {
    auto delay = static_cast<int64_t>(base_ms);
    for (int i = 0; i < attempt; ++i) {
        delay *= 2;
        if (delay > std::numeric_limits<int>::max()) {
            return std::numeric_limits<int>::max();
        }
    }
    return static_cast<int>(delay);
}

// Apply ±`jitter` (fraction, default 0.2) to a backoff delay so concurrent
// clients don't all retry in lockstep. jitter = 0 returns the exact delay.
[[nodiscard]] inline auto
jitterBackoff(int delay_ms, double jitter = 0.2) -> int {
    if (jitter <= 0.0 || delay_ms <= 0) {
        return delay_ms;
    }

    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist(1.0 - jitter, 1.0 + jitter);
    return std::max(1, static_cast<int>(delay_ms * dist(rng)));
}

// Whether a failed send/response should be retried: transport errors and
// responses carry Errc::Timeout or Errc::NotReady (node not ready / moved).
[[nodiscard]] inline auto
retryable(const Result<net::Response>& res) -> bool {
    if (!res.has_value()) {
        return res.error().code() == Errc::Timeout || res.error().code() == Errc::NotReady;
    }
    return res.value().status == Errc::Timeout || res.value().status == Errc::NotReady;
}

// Same policy for pipelined batch sends (multiGet). A successfully returned
// batch is not retried here; per-key misses are handled by the caller.
[[nodiscard]] inline auto
retryable(const Result<std::vector<net::Response>>& res) -> bool {
    if (!res.has_value()) {
        return res.error().code() == Errc::Timeout || res.error().code() == Errc::NotReady;
    }
    return false;
}

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
    // keys are absent. Keys whose owner moved (stale ring view) are followed
    // through redirects like single-key operations.
    auto multiGet(const std::vector<std::string>& keys)
        -> std::unordered_map<std::string, std::string>;

  private:

    // Bound on learned ownership overrides (see learnOwner).
    static constexpr size_t K_MAX_LEARNED_OWNERS = 4'096;

    auto routePrimary(std::string_view key) const -> NodeId;
    auto sendToOwner(const std::string& key, const net::Request& req) -> Result<net::Response>;

    // Owner resolution with learning: a redirect-learned owner wins over the
    // static ring, which may be stale after rebalances.
    auto preferredNode(const std::string& key) const -> NodeId;
    void learnOwner(const std::string& key, const NodeId& node);
    void forgetOwner(const std::string& key);

    io_context io_ctx_;
    std::unique_ptr<asio::executor_work_guard<asio::io_context::executor_type>> io_work_;
    std::jthread io_thread_;
    ConsistentHashRing ring_;
    ConnectionPool pool_;
    int max_retries_ = 2;
    int base_backoff_ms_ = 25;
    // Redirect-learned key → owner overrides. Bounded (cleared when full);
    // a wrong entry self-heals — it just yields another redirect, which
    // re-learns. Guarded: client entry points may run on user threads.
    mutable std::mutex learned_mu_;
    std::unordered_map<std::string, NodeId> learned_;
};

// Parse a server redirect hint ("moved to <node>" or
// "moved to <node>@<host>:<port>") into a target node id.
// Returns nullopt for non-redirect values.
[[nodiscard]] inline auto
parseRedirect(std::string_view value) -> std::optional<NodeId> {
    constexpr std::string_view K_PREFIX = "moved to ";
    if (!value.starts_with(K_PREFIX)) {
        return std::nullopt;
    }

    auto rest = value.substr(K_PREFIX.size());
    if (auto at = rest.find('@'); at != std::string_view::npos) {
        rest = rest.substr(0, at);
    }
    return NodeId(rest);
}

// A parsed redirect: the owner id plus, when the server knew it, a reachable
// address for nodes the client was never configured with (stale ring view
// after a join). Unparsable address parts degrade to id-only.
struct RedirectTarget {
    NodeId id;
    std::string host;
    uint16_t port = 0;

    [[nodiscard]] auto hasAddress() const -> bool { return port != 0 && !host.empty(); }
};

[[nodiscard]] inline auto
parseRedirectTarget(std::string_view value) -> std::optional<RedirectTarget> {
    auto id = parseRedirect(value);
    if (!id.has_value()) {
        return std::nullopt;
    }

    RedirectTarget target{.id = *id, .host = {}};
    constexpr std::string_view K_PREFIX = "moved to ";
    auto rest = value.substr(K_PREFIX.size());
    auto at = rest.find('@');
    if (at == std::string_view::npos) {
        return target;
    }

    auto addr = rest.substr(at + 1);
    auto colon = addr.rfind(':');
    if (colon == std::string_view::npos || colon == 0) {
        return target;
    }

    uint16_t port = 0;
    auto digits = addr.substr(colon + 1);
    auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), port);
    if (ec != std::errc() || ptr != digits.data() + digits.size() || port == 0) {
        return target;
    }

    target.host = std::string(addr.substr(0, colon));
    target.port = port;
    return target;
}
} // namespace cinder
