#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder::net {

constexpr uint8_t K_MAGIC = 0xC1;
constexpr uint8_t K_VERSION = 3;
constexpr size_t K_MAX_MESSAGE_SIZE = 67'108'864;
constexpr size_t K_FRAME_HEADER_SIZE = 7;

// Request flags byte.
constexpr uint8_t K_FLAG_HAS_TTL = uint8_t{1} << 0U;
constexpr uint8_t K_FLAG_HAS_EXPIRES_AT = uint8_t{1} << 1U;

enum class Opcode : uint8_t {
    Get = 1,
    Set = 2,
    Del = 3,
    Ping = 4,
    Gossip = 5,
    Replicate = 6,
    Hint = 7,
};

struct Request {
    Opcode opcode = Opcode::Get;
    std::string key;
    std::string value;
    std::optional<std::chrono::milliseconds> ttl = std::nullopt;
    // Absolute wall-clock expiry — set on Replicate/Hint by the primary so every
    // replica expires the key at the same instant (delay-independent). Set from
    // clients uses the relative `ttl` instead.
    std::optional<std::chrono::system_clock::time_point> expires_at = std::nullopt;
    // Replication metadata — present on Set/Replicate/Hint writes.
    Version version = 0;
    uint64_t writer_node_hash = 0;
};

struct Response {
    Errc status = Errc::OK;
    std::optional<std::string> value;
};

auto
encode(const Request& req) -> Result<std::vector<std::byte>>;

// Encodes into a caller-provided buffer, reusing its capacity when possible
// (avoids a fresh allocation on hot paths).
auto
encodeInto(const Request& req, std::vector<std::byte>& out) -> Result<void>;

auto
decode(std::span<const std::byte> frame) -> Result<Request>;

auto
encode(const Response& res) -> Result<std::vector<std::byte>>;

auto
encodeInto(const Response& res, std::vector<std::byte>& out) -> Result<void>;

auto
decodeResponse(std::span<const std::byte> frame) -> Result<Response>;
} // namespace cinder::net
