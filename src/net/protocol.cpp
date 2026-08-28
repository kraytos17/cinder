#include "cinder/net/protocol.hpp"

#include <array>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <limits>
#include <span>
#include <utility>

#include "cinder/net/byte_reader.hpp"
#include "cinder/net/byte_writer.hpp"

namespace cinder::net {
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::system_clock;

// Network byte order is big-endian. Assembling the bytes big-endian yields the
// host-order value on any endianness (a single load+bswap on x86) and is fully
// constant-evaluable (unlike memcpy).
static constexpr auto
readBe32(const std::byte* buf) -> uint32_t {
    return static_cast<uint32_t>(std::to_integer<uint8_t>(buf[0])) << 24U
           | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[1])) << 16U
           | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[2])) << 8U
           | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[3]));
}

template <typename T>
static constexpr auto
toNet(T val) -> T {
    if constexpr (std::endian::native == std::endian::big) {
        return val;
    }
    return std::byteswap(val);
}

static_assert(toNet(toNet(uint32_t{0x01020304})) == uint32_t{0x01020304});

// The four bytes are the big-endian encoding of 0x01020304; readBe32 converts
// to host order, yielding the same value on both endians.
constexpr std::array<std::byte, 4> K_NET_BE_BYTES = {
    std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}};

static_assert(readBe32(K_NET_BE_BYTES.data()) == 0x01020304U);

// Reads a value from the ByteReader. Returns an error if the read fails
// (truncated input), propagating the failure to the caller.
template <typename T>
static auto
mustRead(ByteReader& r) -> Result<T> {
    return r.read<T>();
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
    ByteWriter w(out);

    w.writeByte(K_MAGIC);
    w.writeByte(K_VERSION);
    w.writeByte(std::to_underlying(req.opcode));
    w.write(static_cast<uint32_t>(payload_size));

    uint8_t flags = 0;
    if (req.ttl.has_value()) {
        flags |= K_FLAG_HAS_TTL;
    }
    if (req.expires_at.has_value()) {
        flags |= K_FLAG_HAS_EXPIRES_AT;
    }

    w.writeByte(flags);
    if (req.ttl.has_value()) {
        w.write(static_cast<uint32_t>(req.ttl->count()));
    }
    if (req.expires_at.has_value()) {
        w.write(static_cast<uint64_t>(
            duration_cast<milliseconds>(req.expires_at->time_since_epoch()).count()));
    }

    w.write(req.version);
    w.write(req.writer_node_hash);

    w.write(static_cast<uint32_t>(req.key.size()));
    w.writeString(req.key);
    w.write(static_cast<uint32_t>(req.value.size()));
    if (!req.value.empty()) {
        w.writeString(req.value);
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

    ByteReader r(frame);
    {
        auto v = mustRead<uint8_t>(r);
        if (!v) {
            return err<Request>(v.error());
        }
    } // magic
    {
        auto v = mustRead<uint8_t>(r);
        if (!v) {
            return err<Request>(v.error());
        }
    } // version

    Request req;
    auto raw_opcode = mustRead<uint8_t>(r);
    if (!raw_opcode) {
        return err<Request>(raw_opcode.error());
    }
    if (*raw_opcode < std::to_underlying(Opcode::Get)
        || *raw_opcode > std::to_underlying(Opcode::GetVersioned)) {
        return err<Request>(Error(Errc::InvalidArgument, "unknown opcode"));
    }

    req.opcode = static_cast<Opcode>(*raw_opcode);
    {
        auto v = mustRead<uint32_t>(r);
        if (!v) {
            return err<Request>(v.error());
        }
    }

    auto flags = mustRead<uint8_t>(r);
    if (!flags) {
        return err<Request>(flags.error());
    }

    bool has_ttl = (*flags & K_FLAG_HAS_TTL) != 0;
    bool has_expires_at = (*flags & K_FLAG_HAS_EXPIRES_AT) != 0;
    if (has_ttl) {
        auto ttl = mustRead<uint32_t>(r);
        if (!ttl) {
            return err<Request>(ttl.error());
        }
        req.ttl = milliseconds(*ttl);
    }
    if (has_expires_at) {
        auto expires_at = mustRead<uint64_t>(r);
        if (!expires_at) {
            return err<Request>(expires_at.error());
        }
        // Guard against overflow when milliseconds → nanoseconds (×1'000'000)
        // inside the system_clock::time_point conversion.
        constexpr auto K_MAX_MS = std::numeric_limits<int64_t>::max() / 1'000'000;
        if (*expires_at > K_MAX_MS) {
            return err<Request>(Error(Errc::InvalidArgument, "expires_at overflow"));
        }
        req.expires_at = system_clock::time_point(milliseconds(*expires_at));
    }

    auto version = mustRead<uint64_t>(r);
    if (!version) {
        return err<Request>(version.error());
    }

    req.version = *version;
    auto writer_node_hash = mustRead<uint64_t>(r);
    if (!writer_node_hash) {
        return err<Request>(writer_node_hash.error());
    }

    req.writer_node_hash = *writer_node_hash;
    auto key_len = mustRead<uint32_t>(r);
    if (!key_len) {
        return err<Request>(key_len.error());
    }
    if (*key_len > 0) {
        auto key_bytes = r.readBytes(*key_len);
        if (!key_bytes) {
            return err<Request>(key_bytes.error());
        }
        req.key.assign(reinterpret_cast<const char*>(key_bytes->data()), *key_len);
    }

    auto val_len = mustRead<uint32_t>(r);
    if (!val_len) {
        return err<Request>(val_len.error());
    }
    if (*val_len > 0) {
        auto val_bytes = r.readBytes(*val_len);
        if (!val_bytes) {
            return err<Request>(val_bytes.error());
        }
        req.value.assign(reinterpret_cast<const char*>(val_bytes->data()), *val_len);
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
    uint8_t flags = 0;
    bool has_version_meta = res.version != 0 || res.writer_node_hash != 0;
    if (has_version_meta) {
        flags |= 0x01U;
    }
    if (res.expires_at.has_value()) {
        flags |= K_FLAG_HAS_EXPIRES_AT;
    }

    size_t payload_size = sizeof(uint8_t); // status
    payload_size += sizeof(uint8_t);       // flags
    payload_size += sizeof(uint32_t);      // has_val
    if (has_version_meta) {
        payload_size += sizeof(uint64_t); // version
        payload_size += sizeof(uint64_t); // writer_node_hash
    }
    if (res.expires_at.has_value()) {
        payload_size += sizeof(uint64_t);
    }
    if (res.value.has_value()) {
        payload_size += res.value->size();
    }

    size_t total = K_FRAME_HEADER_SIZE + payload_size;
    out.resize(total); // reuses capacity across calls
    ByteWriter w(out);

    w.writeByte(K_MAGIC);
    w.writeByte(K_VERSION);
    w.writeByte(0); // response opcode unused
    w.write(static_cast<uint32_t>(payload_size));

    w.writeByte(std::to_underlying(res.status));
    w.writeByte(flags);
    if (has_version_meta) {
        w.write(res.version);
        w.write(res.writer_node_hash);
    }
    if (res.expires_at.has_value()) {
        w.write(static_cast<uint64_t>(
            duration_cast<milliseconds>(res.expires_at->time_since_epoch()).count()));
    }

    w.write(static_cast<uint32_t>(res.value.has_value() ? 1 : 0));
    if (res.value.has_value()) {
        w.writeString(*res.value);
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

    ByteReader r(frame);
    // Skip header — already validated above.
    {
        auto v = mustRead<uint8_t>(r);
        if (!v) {
            return err<Response>(v.error());
        }
    } // magic
    {
        auto v = mustRead<uint8_t>(r);
        if (!v) {
            return err<Response>(v.error());
        }
    } // version
    {
        auto v = mustRead<uint8_t>(r);
        if (!v) {
            return err<Response>(v.error());
        }
    } // opcode (unused for responses)
    {
        auto v = mustRead<uint32_t>(r);
        if (!v) {
            return err<Response>(v.error());
        }
    } // payload_len

    Response res;
    auto status = mustRead<uint8_t>(r);
    if (!status) {
        return err<Response>(status.error());
    }

    res.status = static_cast<Errc>(*status);
    // Flags byte — present from protocol v3 onwards. Bit 0 = version metadata.
    uint8_t flags = 0;
    if (r.remaining() > 0) {
        auto flags_raw = mustRead<uint8_t>(r);
        if (!flags_raw) {
            return err<Response>(flags_raw.error());
        }
        flags = *flags_raw;
    }
    if (flags & 0x01U) {
        auto version = mustRead<uint64_t>(r);
        if (!version) {
            return err<Response>(version.error());
        }

        res.version = *version;
        auto writer_node_hash = mustRead<uint64_t>(r);
        if (!writer_node_hash) {
            return err<Response>(writer_node_hash.error());
        }
        res.writer_node_hash = *writer_node_hash;
    }
    if (flags & K_FLAG_HAS_EXPIRES_AT) {
        auto expires_at = mustRead<uint64_t>(r);
        if (!expires_at) {
            return err<Response>(expires_at.error());
        }

        constexpr auto K_MAX_MS = std::numeric_limits<int64_t>::max() / 1'000'000;
        if (*expires_at > K_MAX_MS) {
            return err<Response>(Error(Errc::InvalidArgument, "expires_at overflow"));
        }
        res.expires_at = system_clock::time_point(milliseconds(*expires_at));
    }

    auto has_val = mustRead<uint32_t>(r);
    if (!has_val) {
        return err<Response>(has_val.error());
    }
    if (*has_val) {
        size_t val_len = r.remaining();
        if (val_len > 0) {
            auto val_bytes = r.readBytes(val_len);
            if (!val_bytes) {
                return err<Response>(val_bytes.error());
            }
            res.value = std::string(reinterpret_cast<const char*>(val_bytes->data()), val_len);
        }
    }
    return ok(std::move(res));
}
} // namespace cinder::net
