#pragma once

#include "cinder/common/status.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace cinder::net {

constexpr uint8_t kMagic = 0xC1;
constexpr uint8_t kVersion = 1;
constexpr size_t kMaxMessageSize = 67108864;
constexpr size_t kFrameHeaderSize = 7;

enum class Opcode : uint8_t {
    Get    = 1,
    Set    = 2,
    Del    = 3,
    Ping   = 4,
    Gossip = 5,
};

struct Request {
    Opcode opcode;
    std::string key;
    std::string value;
    std::optional<std::chrono::milliseconds> ttl;
};

struct Response {
    Errc status = Errc::OK;
    std::optional<std::string> value;
};

auto encode(const Request& req) -> Result<std::vector<std::byte>>;
auto decode(std::span<const std::byte> frame) -> Result<Request>;

auto encode(const Response& res) -> Result<std::vector<std::byte>>;
auto decode_response(std::span<const std::byte> frame) -> Result<Response>;
} // namespace cinder::net
