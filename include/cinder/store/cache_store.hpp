#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <string>

#include "cinder/common/status.hpp"

namespace cinder {

class CacheStore {
  public:

    virtual ~CacheStore() = default;

    virtual auto put(const std::string& key, std::string value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt) -> Result<void> = 0;
    virtual auto get(const std::string& key) -> std::optional<std::string> = 0;
    virtual auto remove(const std::string& key) -> bool = 0;
    [[nodiscard]] virtual auto size() const -> size_t = 0;
    virtual auto evict_expired() -> size_t = 0;
};
} // namespace cinder
