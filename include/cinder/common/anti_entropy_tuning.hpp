#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace cinder {
    using std::chrono::milliseconds;

    // Anti-entropy tuning knobs shared by Config and CacheNodeServerOptions.
    // Durations are the domain type (ServerOptions/AntiEntropyManager speak
    // milliseconds); the YAML boundary converts from ints.
    struct AntiEntropyTuning {
        milliseconds interval{30'000}; // 0 = disabled
        uint32_t buckets = 256;
        size_t fetch_bytes = 1U << 20U; // per-round repair fetch budget
        milliseconds full_rehash_interval{300'000};
    };
} // namespace cinder
