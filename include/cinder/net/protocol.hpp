#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace cinder::net {

constexpr uint8_t K_MAGIC = 0xC1;
constexpr uint8_t K_VERSION = 5;
constexpr size_t K_MAX_MESSAGE_SIZE = 67'108'864;

// Magic + version + opcode + payload-length. The flags byte is the first
// payload byte (the length field counts it), so it is excluded here. Derived
// from the actual encode layout so the constant can never drift from the wire.
consteval auto
computeFrameHeaderSize() -> size_t {
    size_t n = 0;
    n += 2;                // magic + version
    n += 1;                // opcode
    n += sizeof(uint32_t); // payload length
    return n;
}

constexpr size_t K_FRAME_HEADER_SIZE = computeFrameHeaderSize();
static_assert(K_FRAME_HEADER_SIZE == 7);

// Request flags byte.
constexpr uint8_t K_FLAG_HAS_TTL = uint8_t{1} << 0U;
constexpr uint8_t K_FLAG_HAS_EXPIRES_AT = uint8_t{1} << 1U;
constexpr uint8_t K_FLAG_HAS_TRACE = uint8_t{1} << 2U; // trace_id + span_id (16 bytes)
constexpr uint8_t K_FLAG_HAS_AUTH = uint8_t{1} << 3U;  // auth_token (32 bytes, HMAC-SHA256)

enum class Opcode : uint8_t {
    Get = 1,
    Set = 2,
    Del = 3,
    Ping = 4,
    Gossip = 5,
    Replicate = 6,
    Hint = 7,
    GetVersioned = 8,
    AntiEntropyDigest = 9,
    AntiEntropySync = 10,
    // Admin opcodes (client-initiated, bypass ring ownership)
    AdminInfo = 11,
    AdminCluster = 12,
    AdminRing = 13,
    AdminCompact = 14,
    AdminConfigReload = 15,
    AdminShutdown = 16,
};

// The decode path validates `raw_opcode ∈ [Get, AdminShutdown]`; assert at
// compile time that this range covers exactly the declared opcodes with no
// gaps or overlap.
consteval auto
opcodeRangeCoverage() -> bool {
    const int min = std::to_underlying(Opcode::Get);
    const int max = std::to_underlying(Opcode::AdminShutdown);
    int count = 0;
    for (const auto op : {Opcode::Get,
             Opcode::Set,
             Opcode::Del,
             Opcode::Ping,
             Opcode::Gossip,
             Opcode::Replicate,
             Opcode::Hint,
             Opcode::GetVersioned,
             Opcode::AntiEntropyDigest,
             Opcode::AntiEntropySync,
             Opcode::AdminInfo,
             Opcode::AdminCluster,
             Opcode::AdminRing,
             Opcode::AdminCompact,
             Opcode::AdminConfigReload,
             Opcode::AdminShutdown}) {
        if (std::to_underlying(op) < min || std::to_underlying(op) > max) {
            return false;
        }
        ++count;
    }
    return count == max - min + 1;
}

static_assert(opcodeRangeCoverage());

struct Request {
    Opcode opcode = Opcode::Get;
    // Structured tracing — 64-bit trace-id + 64-bit span-id. When non-zero,
    // encoded on the wire via flag 0x04 (16 bytes after value).
    uint64_t trace_id = 0;
    uint64_t span_id = 0;
    std::string key;
    std::string value;
    Version version = 0;
    uint64_t writer_node_hash = 0;
    std::optional<milliseconds> ttl = std::nullopt;
    // Absolute wall-clock expiry — set on Replicate/Hint by the primary so every
    // replica expires the key at the same instant (delay-independent). Set from
    // clients uses the relative `ttl` instead.
    std::optional<system_clock::time_point> expires_at = std::nullopt;
};

// Decode result. The request carries pure message data; the auth token is a
// transport-layer credential extracted from flag 0x08 when present.
struct DecodedRequest {
    Request req;
    std::string auth_token;
};

struct Response {
    Errc status = Errc::OK;
    std::optional<std::string> value;
    // Present only on GetVersioned responses — carry LWW metadata.
    Version version = 0;
    uint64_t writer_node_hash = 0;
    // Absolute wall-clock expiry — carried in GetVersioned responses so read-repair
    // and quorum reads preserve TTL semantics across nodes.
    std::optional<system_clock::time_point> expires_at = std::nullopt;
    // Structured tracing — echoed back for correlation.
    uint64_t trace_id = 0;
    uint64_t span_id = 0;
};

auto
encode(const Request& req, std::string_view auth_token = {}) -> Result<std::vector<std::byte>>;

// Encodes into a caller-provided buffer, reusing its capacity when possible
auto
encodeInto(const Request& req, std::vector<std::byte>& out, std::string_view auth_token = {})
    -> Result<void>;

auto
decode(std::span<const std::byte> frame) -> Result<DecodedRequest>;

auto
encode(const Response& res) -> Result<std::vector<std::byte>>;

auto
encodeInto(const Response& res, std::vector<std::byte>& out) -> Result<void>;

auto
decodeResponse(std::span<const std::byte> frame) -> Result<Response>;
} // namespace cinder::net
