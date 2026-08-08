#pragma once

#include <chrono>

#include "cinder/cluster/clock.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;

// Deterministic clock: time only moves when the test advances it. No real
// sleeps, so TTL/expiry behavior is fully reproducible.
class SimClock final : public cinder::Clock {
  public:
    SimClock();

    auto now() const -> steady_clock::time_point override;
    auto nowSystem() const -> system_clock::time_point override;
    void advance(milliseconds d);

  private:
    steady_clock::time_point now_;
    steady_clock::time_point steady_base_;
    system_clock::time_point system_base_;
};
