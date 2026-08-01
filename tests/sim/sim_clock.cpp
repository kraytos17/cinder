#include "sim_clock.hpp"

SimClock::SimClock()
    : now_(std::chrono::steady_clock::now()) {}

auto
SimClock::now() const -> std::chrono::steady_clock::time_point {
    return now_;
}

void
SimClock::advance(std::chrono::milliseconds d) {
    now_ += d;
}
