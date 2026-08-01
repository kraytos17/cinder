#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"

namespace cinder::net {

constexpr uint8_t K_MAGIC = 0xC1;
constexpr uint8_t K_VERSION = 1;
constexpr size_t K_MAX_MESSAGE_SIZE = 67'108'864;
constexpr size_t K_FRAME_HEADER_SIZE = 7;

enum class Opcode : uint8_t {
    Get = 1,
    Set = 2,
    Del = 3,
    Ping = 4,
    Gossip = 5,
};

struct Request {
    Opcode opcode = Opcode::Get;
    std::string key;
    std::string value;
    std::optional<std::chrono::milliseconds> ttl;
};

struct Response {
    Errc status = Errc::OK;
    std::optional<std::string> value;
};

auto
encode(const Request& req) -> Result<std::vector<std::byte>>;

auto
decode(std::span<const std::byte> frame) -> Result<Request>;

auto
encode(const Response& res) -> Result<std::vector<std::byte>>;

auto
decodeResponse(std::span<const std::byte> frame) -> Result<Response>;
} // namespace cinder::net
