#include "cinder/net/protocol.hpp"

#include <bit>
#include <cstring>
#include <span>
#include <utility>

namespace cinder::net {
using std::chrono::duration_cast;
using std::chrono::milliseconds;

// Network byte order is big-endian. On little-endian hosts
// values are byteswapped; on big-endian hosts the raw memcpy is already correct.
static auto
readBe32(const std::byte* buf) -> uint32_t {
    uint32_t val = 0;
    std::memcpy(&val, buf, sizeof(val));
    if constexpr (std::endian::native == std::endian::big) {
        return val;
    }
    return std::byteswap(val);
}

template <typename T>
static auto
toNet(T val) -> T {
    if constexpr (std::endian::native == std::endian::big) {
        return val;
    }
    return std::byteswap(val);
}

auto
encode(const Request& req) -> Result<std::vector<std::byte>> {
    std::vector<std::byte> buf;
    auto result = encodeInto(req, buf);
    if (!result.has_value()) {
        return err<std::vector<std::byte>>(result.error());
    }
    return ok(std::move(buf));
}

auto
encodeInto(const Request& req, std::vector<std::byte>& out) -> Result<void> {
    size_t payload_size = 0;
    payload_size += sizeof(uint32_t) + req.key.size();
    payload_size += sizeof(uint32_t) + req.value.size();
    payload_size += sizeof(uint64_t); // version
    payload_size += sizeof(uint64_t); // writer_node_hash
    if (req.ttl.has_value()) {
        payload_size += sizeof(uint32_t);
    }

    payload_size += sizeof(uint8_t);
    if (req.expires_at.has_value()) {
        payload_size += sizeof(uint64_t);
    }

    size_t total = K_FRAME_HEADER_SIZE + payload_size;
    if (total > K_MAX_MESSAGE_SIZE) {
        return err(Error(Errc::InvalidArgument, "message too large"));
    }

    out.resize(total); // reuses capacity across calls
    size_t off = 0;

    out[off++] = std::byte{K_MAGIC};
    out[off++] = std::byte{K_VERSION};
    out[off++] = std::byte{std::to_underlying(req.opcode)};

    uint32_t net_len = payload_size;
    net_len = toNet(net_len);
    std::memcpy(&out[off], &net_len, sizeof(net_len));
    off += sizeof(net_len);

    uint8_t flags = 0;
    if (req.ttl.has_value()) {
        flags |= K_FLAG_HAS_TTL;
    }
    if (req.expires_at.has_value()) {
        flags |= K_FLAG_HAS_EXPIRES_AT;
    }

    out[off++] = std::byte{flags};
    if (req.ttl.has_value()) {
        auto net_ttl = static_cast<uint32_t>(req.ttl->count());
        net_ttl = toNet(net_ttl);
        std::memcpy(&out[off], &net_ttl, sizeof(net_ttl));
        off += sizeof(net_ttl);
    }
    if (req.expires_at.has_value()) {
        uint64_t net_expiry = static_cast<uint64_t>(
            duration_cast<milliseconds>(req.expires_at->time_since_epoch()).count());
        net_expiry = toNet(net_expiry);
        std::memcpy(&out[off], &net_expiry, sizeof(net_expiry));
        off += sizeof(net_expiry);
    }

    uint64_t net_version = toNet(req.version);
    std::memcpy(&out[off], &net_version, sizeof(net_version));
    off += sizeof(net_version);

    uint64_t net_writer = toNet(req.writer_node_hash);
    std::memcpy(&out[off], &net_writer, sizeof(net_writer));
    off += sizeof(net_writer);

    auto net_key_len = static_cast<uint32_t>(req.key.size());
    net_key_len = toNet(net_key_len);
    std::memcpy(&out[off], &net_key_len, sizeof(net_key_len));
    off += sizeof(net_key_len);
    std::memcpy(&out[off], req.key.data(), req.key.size());
    off += req.key.size();

    auto net_val_len = static_cast<uint32_t>(req.value.size());
    net_val_len = toNet(net_val_len);
    std::memcpy(&out[off], &net_val_len, sizeof(net_val_len));
    off += sizeof(net_val_len);
    if (!req.value.empty()) {
        std::memcpy(&out[off], req.value.data(), req.value.size());
    }
    return ok();
}

auto
decode(std::span<const std::byte> frame) -> Result<Request> {
    if (frame.size() < K_FRAME_HEADER_SIZE) {
        return err<Request>(Error(Errc::InvalidArgument, "frame too small"));
    }
    if (frame[0] != std::byte{K_MAGIC}) {
        return err<Request>(Error(Errc::InvalidArgument, "bad magic"));
    }
    if (frame[1] != std::byte{K_VERSION}) {
        return err<Request>(Error(Errc::InvalidArgument, "bad version"));
    }

    uint32_t payload_len = readBe32(&frame[3]);
    if (payload_len > K_MAX_MESSAGE_SIZE) {
        return err<Request>(Error(Errc::InvalidArgument, "payload too large"));
    }
    if (K_FRAME_HEADER_SIZE + payload_len > frame.size()) {
        return err<Request>(Error(Errc::InvalidArgument, "truncated frame"));
    }

    Request req;
    uint8_t raw_opcode = std::to_underlying(frame[2]);
    if (raw_opcode < std::to_underlying(Opcode::Get)
        || raw_opcode > std::to_underlying(Opcode::Hint)) {
        return err<Request>(Error(Errc::InvalidArgument, "unknown opcode"));
    }

    req.opcode = static_cast<Opcode>(frame[2]);
    size_t off = K_FRAME_HEADER_SIZE;
    auto flags = static_cast<uint8_t>(frame[off++]);
    bool has_ttl = (flags & K_FLAG_HAS_TTL) != 0;
    bool has_expires_at = (flags & K_FLAG_HAS_EXPIRES_AT) != 0;
    if (has_ttl) {
        if (off + sizeof(uint32_t) > frame.size()) {
            return err<Request>(Error(Errc::InvalidArgument, "truncated ttl"));
        }

        uint32_t ttl_ms = readBe32(&frame[off]);
        off += sizeof(uint32_t);
        req.ttl = milliseconds(ttl_ms);
    }
    if (has_expires_at) {
        if (off + sizeof(uint64_t) > frame.size()) {
            return err<Request>(Error(Errc::InvalidArgument, "truncated expires_at"));
        }

        uint64_t net_expiry = 0;
        std::memcpy(&net_expiry, &frame[off], sizeof(net_expiry));
        net_expiry = toNet(net_expiry);
        off += sizeof(net_expiry);
        req.expires_at =
            std::chrono::system_clock::time_point(std::chrono::milliseconds(net_expiry));
    }
    if (off + sizeof(uint64_t) > frame.size()) {
        return err<Request>(Error(Errc::InvalidArgument, "truncated version"));
    }

    uint64_t net_version = 0;
    std::memcpy(&net_version, &frame[off], sizeof(net_version));
    req.version = toNet(net_version);
    off += sizeof(net_version);
    if (off + sizeof(uint64_t) > frame.size()) {
        return err<Request>(Error(Errc::InvalidArgument, "truncated writer"));
    }

    uint64_t net_writer = 0;
    std::memcpy(&net_writer, &frame[off], sizeof(net_writer));
    req.writer_node_hash = toNet(net_writer);
    off += sizeof(net_writer);
    if (off + sizeof(uint32_t) > frame.size()) {
        return err<Request>(Error(Errc::InvalidArgument, "truncated key length"));
    }

    uint32_t key_len = readBe32(&frame[off]);
    off += sizeof(uint32_t);
    if (key_len > 0) {
        req.key.assign(reinterpret_cast<const char*>(&frame[off]), key_len);
        off += key_len;
    }
    if (off + sizeof(uint32_t) > frame.size()) {
        return err<Request>(Error(Errc::InvalidArgument, "truncated value length"));
    }

    uint32_t val_len = readBe32(&frame[off]);
    off += sizeof(uint32_t);
    if (val_len > 0) {
        req.value.assign(reinterpret_cast<const char*>(&frame[off]), val_len);
    }
    return ok(std::move(req));
}

auto
encode(const Response& res) -> Result<std::vector<std::byte>> {
    std::vector<std::byte> buf;
    auto result = encodeInto(res, buf);
    if (!result.has_value()) {
        return err<std::vector<std::byte>>(result.error());
    }
    return ok(std::move(buf));
}

auto
encodeInto(const Response& res, std::vector<std::byte>& out) -> Result<void> {
    size_t payload_size = sizeof(uint8_t);
    payload_size += sizeof(uint32_t);
    if (res.value.has_value()) {
        payload_size += res.value->size();
    }

    size_t total = K_FRAME_HEADER_SIZE + payload_size;
    out.resize(total); // reuses capacity across calls
    size_t off = 0;

    out[off++] = std::byte{K_MAGIC};
    out[off++] = std::byte{K_VERSION};
    out[off++] = std::byte{0}; // response opcode unused

    uint32_t net_len = payload_size;
    net_len = toNet(net_len);
    std::memcpy(&out[off], &net_len, sizeof(net_len));

    off += sizeof(net_len);
    out[off++] = std::byte{std::to_underlying(res.status)};

    uint32_t has_val = res.value.has_value() ? 1 : 0;
    uint32_t net_has_val = toNet(has_val);

    std::memcpy(&out[off], &net_has_val, sizeof(net_has_val));
    off += sizeof(net_has_val);
    if (res.value.has_value()) {
        std::memcpy(&out[off], res.value->data(), res.value->size());
    }
    return ok();
}

auto
decodeResponse(std::span<const std::byte> frame) -> Result<Response> {
    if (frame.size() < K_FRAME_HEADER_SIZE) {
        return err<Response>(Error(Errc::InvalidArgument, "frame too small"));
    }
    if (frame[0] != std::byte{K_MAGIC}) {
        return err<Response>(Error(Errc::InvalidArgument, "bad magic"));
    }
    if (frame[1] != std::byte{K_VERSION}) {
        return err<Response>(Error(Errc::InvalidArgument, "bad version"));
    }

    uint32_t payload_len = readBe32(&frame[3]);
    if (K_FRAME_HEADER_SIZE + payload_len > frame.size()) {
        return err<Response>(Error(Errc::InvalidArgument, "truncated frame"));
    }

    Response res;
    size_t off = K_FRAME_HEADER_SIZE;

    res.status = static_cast<Errc>(frame[off++]);
    uint32_t has_val = readBe32(&frame[off]);
    off += sizeof(uint32_t);
    if (has_val) {
        size_t val_len = frame.size() - off;
        res.value = std::string(reinterpret_cast<const char*>(&frame[off]), val_len);
    }
    return ok(std::move(res));
}
} // namespace cinder::net
