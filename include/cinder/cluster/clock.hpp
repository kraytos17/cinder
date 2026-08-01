#pragma once

#include <chrono>

namespace cinder {

class Clock {
  public:

    Clock() = default;
    virtual ~Clock() = default;
    Clock(const Clock&) = delete;
    auto operator=(const Clock&) -> Clock& = delete;
    Clock(Clock&&) = delete;
    auto operator=(Clock&&) -> Clock& = delete;

    virtual auto now() const -> std::chrono::steady_clock::time_point = 0;
};
} // namespace cinder
