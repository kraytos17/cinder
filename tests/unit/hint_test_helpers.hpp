#pragma once

// Shared Hint-construction helper for the hint queue/journal unit tests
// (extracted verbatim from cluster_components_test.cpp when it was split
// by area). Same tokens in every TU that includes this header, and the
// free function is inline, so sharing is ODR-safe — including under
// unity builds, which merge these test TUs.

#include <chrono>
#include <cstdint>
#include <string>

#include "cinder/node/hint_queue.hpp"

namespace cinder {

    inline auto cctMakeHint(const std::string& target, const std::string& key, uint64_t version = 1)
        -> HintQueue::Hint {
        HintQueue::Hint h;
        h.target = target;
        h.req.key = key;
        h.req.version = version;
        h.expires_at = std::chrono::steady_clock::time_point::max();
        return h;
    }

} // namespace cinder
