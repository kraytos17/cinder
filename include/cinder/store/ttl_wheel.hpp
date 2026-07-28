#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cinder {

class TtlWheel {
  public:

    static constexpr size_t kSlotCount = 256;

    TtlWheel() = default;

    void insert(const std::string& key, size_t ttl_ticks);
    void remove(const std::string& key);
    auto tick() -> std::vector<std::string>;

    [[nodiscard]] auto cursor() const -> size_t;

  private:

    std::vector<std::unordered_set<std::string>> wheel_{kSlotCount};
    size_t cursor_ = 0;
    std::unordered_map<std::string, size_t> key_to_slot_;
};
} // namespace cinder
