#include "sim_clock.hpp"

SimClock::SimClock()
    : now_(std::chrono::steady_clock::now()),
      steady_base_(now_),
      system_base_(std::chrono::system_clock::now()) {}

auto
SimClock::now() const -> std::chrono::steady_clock::time_point {
    return now_;
}

auto
SimClock::nowSystem() const -> std::chrono::system_clock::time_point {
    // Advance wall time in lockstep with the simulated monotonic clock so the
    // sys↔steady conversion round-trips exactly (deterministic expiry).
    return system_base_ + (now_ - steady_base_);
}

void
SimClock::advance(std::chrono::milliseconds d) {
    now_ += d;
}
