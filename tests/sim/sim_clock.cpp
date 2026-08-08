#include "sim_clock.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;

SimClock::SimClock()
    : now_(steady_clock::now()),
      steady_base_(now_),
      system_base_(system_clock::now()) {}

auto
SimClock::now() const -> steady_clock::time_point {
    return now_;
}

auto
SimClock::nowSystem() const -> system_clock::time_point {
    // Advance wall time in lockstep with the simulated monotonic clock so the
    // sys↔steady conversion round-trips exactly (deterministic expiry).
    return system_base_ + (now_ - steady_base_);
}

void
SimClock::advance(milliseconds d) {
    now_ += d;
}
