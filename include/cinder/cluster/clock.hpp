#pragma once

#include <chrono>

namespace cinder {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;

class Clock {
  public:

    Clock() = default;
    virtual ~Clock() = default;
    Clock(const Clock&) = delete;
    auto operator=(const Clock&) -> Clock& = delete;
    Clock(Clock&&) = delete;
    auto operator=(Clock&&) -> Clock& = delete;

    virtual auto now() const -> steady_clock::time_point = 0;

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

// Convert a steady-clock time_point to wall-clock milliseconds since epoch.
// Used by persistence to store absolute TTLs on disk.
inline auto
toSystemMs(const Clock& clock, steady_clock::time_point t) -> uint64_t {
    auto sys = toSystemExpiry(clock, t);
    return static_cast<uint64_t>(duration_cast<milliseconds>(sys.time_since_epoch()).count());
}

// Null-tolerant variants for call sites holding an optional injected Clock*.
inline auto
toSystemMsOrDefault(const Clock* clock, steady_clock::time_point t) -> uint64_t {
    RealClock fallback;
    return toSystemMs(clock != nullptr ? *clock : static_cast<const Clock&>(fallback), t);
}

inline auto
nowSystemMs(const Clock* clock) -> uint64_t {
    RealClock fallback;
    const Clock& c = clock != nullptr ? *clock : static_cast<const Clock&>(fallback);
    return static_cast<uint64_t>(
        duration_cast<milliseconds>(c.nowSystem().time_since_epoch()).count());
}
} // namespace cinder
