#pragma once

#include <chrono>

namespace cinder {

using namespace std::chrono;

class Clock {
  public:

    Clock() = default;
    virtual ~Clock() = default;
    Clock(const Clock&) = delete;
    auto operator=(const Clock&) -> Clock& = delete;
    Clock(Clock&&) = delete;
    auto operator=(Clock&&) -> Clock& = delete;

    virtual auto now() const -> steady_clock::time_point = 0;

    // Wall-clock time. Defaults to system_clock::now(); tests override for
    // determinism (see SimClock).
    virtual auto nowSystem() const -> system_clock::time_point { return system_clock::now(); }
};

class RealClock final : public Clock {
  public:

    auto now() const -> steady_clock::time_point override { return steady_clock::now(); }
};

// Convert a steady-clock expiry to absolute wall-clock time. Sampled on the io
// thread; monotonic and wall clocks advance together, so the offset is stable
// within a single sample.
inline auto
toSystemExpiry(const Clock& clock, steady_clock::time_point t) -> system_clock::time_point {
    return clock.nowSystem() + (t - clock.now());
}

// Convert an absolute wall-clock expiry to the local steady basis, so a replica
// applies the same wall-clock expiry the primary computed (under network delay
// the remaining duration shrinks accordingly).
inline auto
toSteadyExpiry(const Clock& clock, system_clock::time_point t) -> steady_clock::time_point {
    return clock.now() + (t - clock.nowSystem());
}
} // namespace cinder
