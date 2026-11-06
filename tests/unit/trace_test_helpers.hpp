#pragma once

// Shared span assertions for unit tests that verify tracing behavior
// (extracted verbatim from cluster_components_test.cpp when it was split
// by area). Same tokens in every TU that includes this header, and all
// free functions are inline, so sharing is ODR-safe.

#include <memory>
#include <string_view>
#include <vector>

#include "cinder/common/tracing.hpp"

namespace cinder {

    struct TraceGuard {
        explicit TraceGuard(std::shared_ptr<Subscriber> sub) :
            prev_(globalSubscriber()) {
            setGlobalSubscriber(std::move(sub));
        }

        ~TraceGuard() {
            setGlobalSubscriber(prev_);
        }

        std::shared_ptr<Subscriber> prev_;
    };

    inline auto findSpan(
        const std::vector<RecordingSubscriber::RecordedSpan>& spans,
        std::string_view operation
    ) -> const RecordingSubscriber::RecordedSpan* {
        for (const auto& s : spans) {
            if (s.operation == operation) {
                return &s;
            }
        }
        return nullptr;
    }

    inline auto findAttr(const RecordingSubscriber::RecordedSpan& span, std::string_view key)
        -> const RecordingSubscriber::RecordedField* {
        for (const auto& a : span.attributes) {
            if (a.key == key) {
                return &a;
            }
        }
        return nullptr;
    }

} // namespace cinder
