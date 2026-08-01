#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

class CacheStore {
  public:

    explicit CacheStore(Clock* clock = nullptr)
        : clock_(clock) {}

    virtual ~CacheStore() = default;

    CacheStore(const CacheStore&) = delete;
    auto operator=(const CacheStore&) -> CacheStore& = delete;
    CacheStore(CacheStore&&) = delete;
    auto operator=(CacheStore&&) -> CacheStore& = delete;

    virtual auto put(const std::string& key, std::string value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void> = 0;
    virtual auto get(const std::string& key) -> std::optional<std::string> = 0;
    virtual auto remove(const std::string& key) -> bool = 0;
    [[nodiscard]] virtual auto size() const -> size_t = 0;
    virtual auto evictExpired() -> size_t = 0;

    // Versioned write path — used by replication. Applies idempotently:
    // accepts iff incoming.version > local.version, or == with
    // writer_node_hash >= local.writer_node_hash (tiebreak).
    virtual auto putVersioned(const std::string& key, VersionedEntry entry) -> Result<void> = 0;
    virtual auto getVersioned(const std::string& key) -> std::optional<VersionedEntry> = 0;

  protected:

    // Injected clock for deterministic sims; nullptr → real steady_clock.
    auto now() const -> std::chrono::steady_clock::time_point {
        return clock_ ? clock_->now() : std::chrono::steady_clock::now();
    }

  private:

    Clock* clock_ = nullptr;
};
} // namespace cinder
