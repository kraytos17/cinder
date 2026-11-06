#pragma once

#include <charconv>
#include <concepts>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>

#include "cinder/common/cluster_config.hpp"

namespace cinder {

    template <typename T>
    concept UnsignedDigits = std::unsigned_integral<T>;

    template <UnsignedDigits T> inline auto parseUint(std::string_view digits, T& out) -> bool {
        const auto* first = digits.data();
        const auto* last = digits.data() + digits.size();
        auto [ptr, ec] = std::from_chars(first, last, out);
        return ec == std::errc{} && ptr == last;
    }

    // Parse a single "id@host:port" peer string into a NodeConfig. Returns false
    // on malformed input (no '@' separator, missing port, or a non-numeric or
    // out-of-range port). constexpr so it can be exercised at compile time.
    inline constexpr auto parsePeer(const std::string& peer, ClusterConfig::NodeConfig& out)
        -> bool {
        auto at = peer.rfind('@');
        if (at == std::string::npos) {
            return false;
        }

        auto colon = peer.find(':', at);
        if (colon == std::string::npos) {
            return false;
        }
        if (at == 0 || colon == at + 1) {
            return false;
        }

        uint16_t port = 0;
        std::string_view digits(peer.data() + colon + 1, peer.size() - colon - 1);
        if (!parseUint(digits, port)) {
            return false;
        }

        out.id = peer.substr(0, at);
        out.host = peer.substr(at + 1, colon - at - 1);
        out.port = port;
        return true;
    }
} // namespace cinder
