#include "cinder/common/hmac.hpp"

#include <cstdint>
#include <cstring>
#include <ctime>

#ifdef CINDER_ENABLE_CRYPTO
#include <openssl/evp.h>
#include <openssl/hmac.h>
#endif

namespace cinder {

#ifdef CINDER_ENABLE_CRYPTO
auto
computeHmacSha256(std::string_view key, std::string_view message) -> HmacDigest {
    HmacDigest digest{};
    unsigned int len = 0;
    HMAC(EVP_sha256(),
        key.data(),
        static_cast<int>(key.size()),
        reinterpret_cast<const unsigned char*>(message.data()),
        message.size(),
        reinterpret_cast<unsigned char*>(digest.data()),
        &len);
    return digest;
}
#else
auto
computeHmacSha256(std::string_view /*key*/, std::string_view /*message*/) -> HmacDigest {
    return HmacDigest{};
}
#endif

auto
generateAuthToken(std::string_view secret, const std::string& node_id) -> std::string {
    if (secret.empty()) {
        return {};
    }

    // Truncate time to 10-second windows for clock-skew tolerance.
    auto now = static_cast<uint64_t>(std::time(nullptr)) / 10;
    std::string data = node_id + ":" + std::to_string(now);
    auto digest = computeHmacSha256(secret, data);
    return std::string(reinterpret_cast<const char*>(digest.data()), digest.size());
}

auto
verifyAuthToken(std::string_view secret, const std::string& node_id, std::string_view token,
    int window_seconds) -> bool {
    if (secret.empty() || token.empty()) {
        return false;
    }
    if (token.size() != 32) {
        return false;
    }

    auto now = static_cast<uint64_t>(std::time(nullptr));
    int window_windows = window_seconds / 10 + 1; // +1 for rounding
    for (int i = 0; i <= window_windows; ++i) {
        uint64_t ts = (now / 10) - static_cast<uint64_t>(i);
        std::string data = node_id + ":" + std::to_string(ts);
        auto expected = computeHmacSha256(secret, data);
        if (std::memcmp(token.data(), expected.data(), 32) == 0) {
            return true;
        }
    }
    return false;
}
} // namespace cinder
