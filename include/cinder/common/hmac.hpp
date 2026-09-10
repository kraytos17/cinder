#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

namespace cinder {

// 32-byte HMAC-SHA256 digest.
using HmacDigest = std::array<std::byte, 32>;

// Compute HMAC-SHA256(key, message). Returns a 32-byte digest.
auto
computeHmacSha256(std::string_view key, std::string_view message) -> HmacDigest;

// Generate an auth token for node authentication.
// Token = HMAC-SHA256(shared_secret, node_id + ":" + unix_seconds).
// The timestamp is truncated to 10-second windows to allow clock skew.
auto
generateAuthToken(std::string_view secret, const std::string& node_id) -> std::string;

// Verify an auth token against the shared secret.
// Accepts tokens generated within the last `window_seconds` (default 30s).
auto
verifyAuthToken(std::string_view secret, const std::string& node_id, std::string_view token,
    int window_seconds = 30) -> bool;
} // namespace cinder
