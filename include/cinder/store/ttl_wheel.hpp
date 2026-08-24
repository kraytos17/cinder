#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace cinder {

class TtlWheel {
  public:

    static constexpr size_t K_SLOT_COUNT = 256;

    TtlWheel() = default;

    void insert(const std::string& key, size_t ttl_ticks);
    void remove(const std::string& key);
    void tick(std::move_only_function<void(const std::string&)> on_expired);

    [[nodiscard]] auto cursor() const -> size_t;

  private:

    std::vector<std::unordered_set<std::string>> wheel_{K_SLOT_COUNT};
    size_t cursor_ = 0;
    std::unordered_map<std::string, size_t> key_to_slot_;
};
} // namespace cinder
