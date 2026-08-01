#pragma once

#include <chrono>

#include "cinder/cluster/clock.hpp"

// Deterministic clock: time only moves when the test advances it. No real
// sleeps, so TTL/expiry behavior is fully reproducible.
class SimClock final : public cinder::Clock {
  public:
    SimClock();

    auto now() const -> std::chrono::steady_clock::time_point override;
    void advance(std::chrono::milliseconds d);

  private:
    std::chrono::steady_clock::time_point now_;
};
