#include "cinder/net/protocol.hpp"

#include <bit>
#include <cstring>
#include <span>
#include <utility>

namespace cinder::net {

static auto
readBe32(const std::byte* buf) -> uint32_t {
    uint32_t val = 0;
    std::memcpy(&val, buf, sizeof(val));
    return std::byteswap(val);
}

auto
encode(const Request& req) -> Result<std::vector<std::byte>> {
    size_t payload_size = 0;
    payload_size += sizeof(uint32_t) + req.key.size();
    payload_size += sizeof(uint32_t) + req.value.size();
    if (req.ttl.has_value()) {
        payload_size += sizeof(uint32_t);
    }

    payload_size += sizeof(uint8_t);
    size_t total = K_FRAME_HEADER_SIZE + payload_size;
    if (total > K_MAX_MESSAGE_SIZE) {
        return Result<std::vector<std::byte>>::err(
            Error(Errc::InvalidArgument, "message too large"));
    }

    std::vector<std::byte> buf(total);
    size_t off = 0;

    buf[off++] = std::byte{K_MAGIC};
    buf[off++] = std::byte{K_VERSION};
    buf[off++] = std::byte{static_cast<uint8_t>(req.opcode)};

    uint32_t net_len = payload_size;
    net_len = std::byteswap(net_len);
    std::memcpy(&buf[off], &net_len, sizeof(net_len));
    off += sizeof(net_len);

    uint8_t flags = req.ttl.has_value() ? 1 : 0;
    buf[off++] = std::byte{flags};
    if (req.ttl.has_value()) {
        auto net_ttl = static_cast<uint32_t>(req.ttl->count());
        net_ttl = std::byteswap(net_ttl);
        std::memcpy(&buf[off], &net_ttl, sizeof(net_ttl));
        off += sizeof(net_ttl);
    }

    auto net_key_len = static_cast<uint32_t>(req.key.size());
    net_key_len = std::byteswap(net_key_len);
    std::memcpy(&buf[off], &net_key_len, sizeof(net_key_len));
    off += sizeof(net_key_len);
    std::memcpy(&buf[off], req.key.data(), req.key.size());
    off += req.key.size();

    auto net_val_len = static_cast<uint32_t>(req.value.size());
    net_val_len = std::byteswap(net_val_len);
    std::memcpy(&buf[off], &net_val_len, sizeof(net_val_len));
    off += sizeof(net_val_len);
    if (!req.value.empty()) {
        std::memcpy(&buf[off], req.value.data(), req.value.size());
    }
    return ok(std::move(buf));
}

auto
decode(std::span<const std::byte> frame) -> Result<Request> {
    if (frame.size() < K_FRAME_HEADER_SIZE) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "frame too small"));
    }
    if (frame[0] != std::byte{K_MAGIC}) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "bad magic"));
    }
    if (frame[1] != std::byte{K_VERSION}) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "bad version"));
    }

    uint32_t payload_len = readBe32(&frame[3]);
    if (payload_len > K_MAX_MESSAGE_SIZE) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "payload too large"));
    }
    if (K_FRAME_HEADER_SIZE + payload_len > frame.size()) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "truncated frame"));
    }

    Request req;
    req.opcode = static_cast<Opcode>(frame[2]);

    size_t off = K_FRAME_HEADER_SIZE;
    auto flags = static_cast<uint8_t>(frame[off++]);
    bool has_ttl = (flags & 1U) != 0;
    if (has_ttl) {
        if (off + sizeof(uint32_t) > frame.size()) {
            return Result<Request>::err(Error(Errc::InvalidArgument, "truncated ttl"));
        }

        uint32_t ttl_ms = readBe32(&frame[off]);
        off += sizeof(uint32_t);
        req.ttl = std::chrono::milliseconds(ttl_ms);
    }
    if (off + sizeof(uint32_t) > frame.size()) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "truncated key length"));
    }

    uint32_t key_len = readBe32(&frame[off]);
    off += sizeof(uint32_t);
    if (key_len > 0) {
        req.key.assign(reinterpret_cast<const char*>(&frame[off]), key_len);
        off += key_len;
    }
    if (off + sizeof(uint32_t) > frame.size()) {
        return Result<Request>::err(Error(Errc::InvalidArgument, "truncated value length"));
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
    size_t payload_size = sizeof(uint8_t);
    payload_size += sizeof(uint32_t);
    if (res.value.has_value()) {
        payload_size += res.value->size();
    }

    size_t total = K_FRAME_HEADER_SIZE + payload_size;
    std::vector<std::byte> buf(total);
    size_t off = 0;

    buf[off++] = std::byte{K_MAGIC};
    buf[off++] = std::byte{K_VERSION};
    buf[off++] = std::byte{0}; // response opcode unused

    uint32_t net_len = payload_size;
    net_len = std::byteswap(net_len);
    std::memcpy(&buf[off], &net_len, sizeof(net_len));

    off += sizeof(net_len);
    buf[off++] = std::byte{static_cast<uint8_t>(res.status)};

    uint32_t has_val = res.value.has_value() ? 1 : 0;
    uint32_t net_has_val = std::byteswap(has_val);

    std::memcpy(&buf[off], &net_has_val, sizeof(net_has_val));
    off += sizeof(net_has_val);
    if (res.value.has_value()) {
        std::memcpy(&buf[off], res.value->data(), res.value->size());
    }
    return ok(std::move(buf));
}

auto
decodeResponse(std::span<const std::byte> frame) -> Result<Response> {
    if (frame.size() < K_FRAME_HEADER_SIZE) {
        return Result<Response>::err(Error(Errc::InvalidArgument, "frame too small"));
    }
    if (frame[0] != std::byte{K_MAGIC}) {
        return Result<Response>::err(Error(Errc::InvalidArgument, "bad magic"));
    }
    if (frame[1] != std::byte{K_VERSION}) {
        return Result<Response>::err(Error(Errc::InvalidArgument, "bad version"));
    }

    uint32_t payload_len = readBe32(&frame[3]);
    if (K_FRAME_HEADER_SIZE + payload_len > frame.size()) {
        return Result<Response>::err(Error(Errc::InvalidArgument, "truncated frame"));
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
