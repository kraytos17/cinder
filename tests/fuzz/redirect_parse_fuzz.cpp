#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "cinder/client/cache_client.hpp"

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view input(reinterpret_cast<const char*>(data), size);
    auto target = cinder::parseRedirectTarget(input);
    if (!target.has_value()) {
        return 0;
    }
    // Invariants guaranteed by construction — a failure here is a parser bug.
    auto legacy = cinder::parseRedirect(input);
    assert(legacy.has_value() && *legacy == target->id);
    if (target->hasAddress()) {
        assert(!target->host.empty() && target->port != 0);
    }
    return 0;
}
