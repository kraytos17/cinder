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
    auto computeHmacSha256(std::string_view key, std::string_view message) -> HmacDigest {
        HmacDigest digest{};
        unsigned int len = 0;
        HMAC(
            EVP_sha256(),
            key.data(),
            static_cast<int>(key.size()),
            reinterpret_cast<const unsigned char*>(message.data()),
            message.size(),
            reinterpret_cast<unsigned char*>(digest.data()),
            &len
        );
        return digest;
    }
#else
    auto computeHmacSha256(std::string_view /*key*/, std::string_view /*message*/) -> HmacDigest {
        return HmacDigest{};
    }
#endif

    namespace {
        // Constant-time digest comparison to avoid leaking prefix-match length.
        auto constantTimeEqual(std::string_view a, std::string_view b) -> bool {
            if (a.size() != b.size()) {
                return false;
            }

            auto diff = static_cast<unsigned char>(0);
            for (size_t i = 0; i < a.size(); ++i) {
                const auto ca = static_cast<unsigned char>(a[i]);
                const auto cb = static_cast<unsigned char>(b[i]);
                diff = static_cast<unsigned char>(diff | static_cast<unsigned char>(ca ^ cb));
            }
            return diff == 0;
        }

        constexpr std::string_view K_CLIENT_AUTH_DOMAIN = "cinder-client-v1";
    } // namespace

    auto generateAuthToken(std::string_view secret, const std::string& node_id) -> std::string {
        if (secret.empty()) {
            return {};
        }
#ifndef CINDER_ENABLE_CRYPTO
        // Fail closed: without a real HMAC the stub digest is all zeros and
        // must never be issued as a credential.
        return {};
#else
        // Truncate time to 10-second windows for clock-skew tolerance.
        auto now = static_cast<uint64_t>(std::time(nullptr)) / 10;
        std::string data = node_id + ":" + std::to_string(now);
        auto digest = computeHmacSha256(secret, data);
        return std::string(reinterpret_cast<const char*>(digest.data()), digest.size());
#endif
    }

    auto verifyAuthToken(
        std::string_view secret,
        const std::string& node_id,
        std::string_view token,
        int window_seconds
    ) -> bool {
        if (secret.empty() || token.empty()) {
            return false;
        }
        if (token.size() != 32) {
            return false;
        }
#ifndef CINDER_ENABLE_CRYPTO
        return false;
#else
        auto now = static_cast<uint64_t>(std::time(nullptr));
        int window_windows = window_seconds / 10 + 1; // +1 for rounding
        for (int i = 0; i <= window_windows; ++i) {
            uint64_t ts = (now / 10) - static_cast<uint64_t>(i);
            std::string data = node_id + ":" + std::to_string(ts);
            auto expected = computeHmacSha256(secret, data);
            auto expected_view =
                std::string_view(reinterpret_cast<const char*>(expected.data()), expected.size());
            if (constantTimeEqual(token, expected_view)) {
                return true;
            }
        }
        return false;
#endif
    }

    auto generateClientAuthToken(std::string_view secret) -> std::string {
        if (secret.empty()) {
            return {};
        }
#ifndef CINDER_ENABLE_CRYPTO
        return {};
#else
        auto digest = computeHmacSha256(secret, K_CLIENT_AUTH_DOMAIN);
        return std::string(reinterpret_cast<const char*>(digest.data()), digest.size());
#endif
    }

    auto verifyClientAuthToken(std::string_view secret, std::string_view token) -> bool {
        if (secret.empty() || token.size() != 32) {
            return false;
        }
#ifndef CINDER_ENABLE_CRYPTO
        return false;
#else
        auto expected = computeHmacSha256(secret, K_CLIENT_AUTH_DOMAIN);
        auto expected_view =
            std::string_view(reinterpret_cast<const char*>(expected.data()), expected.size());
        return constantTimeEqual(token, expected_view);
#endif
    }
} // namespace cinder
