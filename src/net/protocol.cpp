#include "cinder/net/protocol.hpp"

#include <array>
#include <bit>
#include <cassert>
#include <concepts>
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
    static constexpr auto readBe32(const std::byte* buf) -> uint32_t {
        return static_cast<uint32_t>(std::to_integer<uint8_t>(buf[0]))
            << 24U
            | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[1]))
            << 16U
            | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[2]))
            << 8U
            | static_cast<uint32_t>(std::to_integer<uint8_t>(buf[3]));
    }

    template <typename T> static constexpr auto toNet(T val) -> T {
        if constexpr (std::endian::native == std::endian::big) {
            return val;
        }
        return std::byteswap(val);
    }

    static_assert(toNet(toNet(uint32_t{0x01020304})) == uint32_t{0x01020304});

    // The four bytes are the big-endian encoding of 0x01020304; readBe32 converts
    // to host order, yielding the same value on both endians.
    constexpr std::array<std::byte, 4> K_NET_BE_BYTES = {
        std::byte{0x01},
        std::byte{0x02},
        std::byte{0x03},
        std::byte{0x04},
    };

    static_assert(readBe32(K_NET_BE_BYTES.data()) == 0x01020304U);

    // Validated, bounded view of one complete wire frame. The caller may pass
    // a larger buffer (e.g. a coalesced read); payload never includes bytes
    // beyond the declared length. Stream framing remains the TCP adapter's job.
    struct FrameView {
        uint8_t opcode = 0;
        std::span<const std::byte> payload;
    };

    static auto parseFrame(std::span<const std::byte> frame) -> Result<FrameView> {
        if (frame.size() < K_FRAME_HEADER_SIZE) {
            return err<FrameView>(Error(Errc::InvalidArgument, "frame too small"));
        }
        if (frame[0] != std::byte{K_MAGIC}) {
            return err<FrameView>(Error(Errc::InvalidArgument, "bad magic"));
        }
        if (frame[1] != std::byte{K_VERSION}) {
            return err<FrameView>(Error(Errc::InvalidArgument, "bad version"));
        }

        const uint32_t payload_size = readBe32(&frame[3]);
        constexpr size_t K_MAX_PAYLOAD_SIZE = K_MAX_MESSAGE_SIZE - K_FRAME_HEADER_SIZE;
        if (payload_size > K_MAX_PAYLOAD_SIZE) {
            return err<FrameView>(Error(Errc::InvalidArgument, "payload too large"));
        }

        const size_t payload_size_t = static_cast<size_t>(payload_size);
        if (payload_size_t > frame.size() - K_FRAME_HEADER_SIZE) {
            return err<FrameView>(Error(Errc::InvalidArgument, "truncated frame"));
        }
        return ok(
            FrameView{
                .opcode = std::to_integer<uint8_t>(frame[2]),
                .payload = frame.subspan(K_FRAME_HEADER_SIZE, payload_size_t),
            }
        );
    }

    static auto frameSizeForPayload(size_t payload_size) -> Result<size_t> {
        constexpr size_t K_MAX_PAYLOAD_SIZE = K_MAX_MESSAGE_SIZE - K_FRAME_HEADER_SIZE;
        if (payload_size > K_MAX_PAYLOAD_SIZE) {
            return err<size_t>(Error(Errc::InvalidArgument, "message too large"));
        }
        return K_FRAME_HEADER_SIZE + payload_size;
    }

    constexpr size_t K_MAX_PAYLOAD_SIZE = K_MAX_MESSAGE_SIZE - K_FRAME_HEADER_SIZE;

    // Add one encoded field size without overflowing or exceeding the wire
    // message budget. Used by both encoders so their size calculation and
    // frame validator share the same total-size invariant.
    static auto addPayloadSize(size_t& payload_size, size_t field_size) -> bool {
        if (payload_size > K_MAX_PAYLOAD_SIZE || field_size > K_MAX_PAYLOAD_SIZE - payload_size) {
            return false;
        }

        payload_size += field_size;
        return true;
    }

    template <std::integral T> static auto mustRead(ByteReader& reader) -> Result<T> {
        return reader.read<T>();
    }

    auto encode(const Request& req, std::string_view auth_token) -> Result<std::vector<std::byte>> {
        std::vector<std::byte> buf;
        auto result = encodeInto(req, buf, auth_token);
        if (!result.has_value()) {
            return err<std::vector<std::byte>>(result.error());
        }
        return ok(std::move(buf));
    }

    auto encodeInto(const Request& req, std::vector<std::byte>& out, std::string_view auth_token)
        -> Result<void> {
        size_t payload_size = 0;
        const auto add = [&](size_t n) {
            return addPayloadSize(payload_size, n);
        };
        if (req.key.size()
            > std::numeric_limits<uint32_t>::max()
            || req.value.size()
            > std::numeric_limits<uint32_t>::max()
            || !add(sizeof(uint32_t))
            || !add(req.key.size())
            || !add(sizeof(uint32_t))
            || !add(req.value.size())
            || !add(sizeof(uint64_t) * 2)
            || (req.ttl.has_value() && !add(sizeof(uint32_t)))
            || !add(sizeof(uint8_t))
            || (req.expires_at.has_value() && !add(sizeof(uint64_t)))
            || (req.opcode
                == Opcode::CounterAdjust
                && (!add(sizeof(uint64_t)) || !add(sizeof(uint8_t))))) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }

        bool has_trace = req.trace_id != 0;
        if (has_trace && !add(sizeof(uint64_t) * 2)) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }

        bool has_auth = !auth_token.empty();
        if (has_auth) {
            if (auth_token.size() != 32) {
                return err(Error(Errc::InvalidArgument, "auth token must be 32 bytes"));
            }
            if (!add(32)) {
                return err(Error(Errc::InvalidArgument, "message too large"));
            }
        }

        auto total = frameSizeForPayload(payload_size);
        if (!total) {
            return err(total.error());
        }

        out.resize(*total); // reuses capacity across calls
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
        if (req.tombstone) {
            flags |= K_FLAG_HAS_TOMBSTONE;
        }
        if (has_trace) {
            flags |= K_FLAG_HAS_TRACE;
        }
        if (has_auth) {
            flags |= K_FLAG_HAS_AUTH;
        }

        w.writeByte(flags);
        if (req.ttl.has_value()) {
            w.write(static_cast<uint32_t>(req.ttl->count()));
        }
        if (req.expires_at.has_value()) {
            w.write(
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(req.expires_at->time_since_epoch()).count()
                )
            );
        }

        w.write(req.version);
        w.write(req.writer_node_hash);
        w.write(static_cast<uint32_t>(req.key.size()));
        w.writeString(req.key);
        w.write(static_cast<uint32_t>(req.value.size()));
        if (!req.value.empty()) {
            w.writeString(req.value);
        }
        if (req.opcode == Opcode::CounterAdjust) {
            w.write(req.counter_delta);
            w.writeByte(req.counter_decrement ? uint8_t{1} : uint8_t{0});
        }
        if (has_trace) {
            w.write(req.trace_id);
            w.write(req.span_id);
        }
        if (has_auth) {
            w.writeString(auth_token);
        }

        assert(w.offset() == out.size());
        return ok();
    }

    auto decode(std::span<const std::byte> frame) -> Result<DecodedRequest> {
        auto parsed_frame = parseFrame(frame);
        if (!parsed_frame) {
            return err<DecodedRequest>(parsed_frame.error());
        }

        const FrameView& view = *parsed_frame;
        ByteReader r(view.payload);
        DecodedRequest decoded;
        Request& req = decoded.req;
        const uint8_t raw_opcode = view.opcode;
        if (raw_opcode
            < std::to_underlying(Opcode::Get)
            || raw_opcode
            > std::to_underlying(Opcode::CounterAntiEntropy)) {
            return err<DecodedRequest>(Error(Errc::InvalidArgument, "unknown opcode"));
        }

        req.opcode = static_cast<Opcode>(raw_opcode);
        auto flags = mustRead<uint8_t>(r);
        if (!flags) {
            return err<DecodedRequest>(flags.error());
        }

        bool has_ttl = (*flags & K_FLAG_HAS_TTL) != 0;
        bool has_expires_at = (*flags & K_FLAG_HAS_EXPIRES_AT) != 0;
        req.tombstone = (*flags & K_FLAG_HAS_TOMBSTONE) != 0;
        if (has_ttl) {
            auto ttl = mustRead<uint32_t>(r);
            if (!ttl) {
                return err<DecodedRequest>(ttl.error());
            }
            req.ttl = milliseconds(*ttl);
        }
        if (has_expires_at) {
            auto expires_at = mustRead<uint64_t>(r);
            if (!expires_at) {
                return err<DecodedRequest>(expires_at.error());
            }

            // Guard against overflow when milliseconds → nanoseconds (×1'000'000)
            // inside the system_clock::time_point conversion.
            constexpr auto K_MAX_MS = std::numeric_limits<int64_t>::max() / 1'000'000;
            if (*expires_at > K_MAX_MS) {
                return err<DecodedRequest>(Error(Errc::InvalidArgument, "expires_at overflow"));
            }
            req.expires_at = system_clock::time_point(milliseconds(*expires_at));
        }

        auto version = mustRead<uint64_t>(r);
        if (!version) {
            return err<DecodedRequest>(version.error());
        }

        req.version = *version;
        auto writer_node_hash = mustRead<uint64_t>(r);
        if (!writer_node_hash) {
            return err<DecodedRequest>(writer_node_hash.error());
        }

        req.writer_node_hash = *writer_node_hash;
        auto key_len = mustRead<uint32_t>(r);
        if (!key_len) {
            return err<DecodedRequest>(key_len.error());
        }
        if (*key_len > 0) {
            auto key_view = r.readStringView(*key_len);
            if (!key_view) {
                return err<DecodedRequest>(key_view.error());
            }
            req.key.assign(*key_view);
        }

        auto val_len = mustRead<uint32_t>(r);
        if (!val_len) {
            return err<DecodedRequest>(val_len.error());
        }
        if (*val_len > 0) {
            auto val_view = r.readStringView(*val_len);
            if (!val_view) {
                return err<DecodedRequest>(val_view.error());
            }
            req.value.assign(*val_view);
        }
        if (req.opcode == Opcode::CounterAdjust) {
            auto delta = mustRead<uint64_t>(r);
            auto decrement = mustRead<uint8_t>(r);
            if (!delta || !decrement || *decrement > 1) {
                return err<DecodedRequest>(Error(Errc::InvalidArgument, "invalid counter adjust"));
            }

            req.counter_delta = *delta;
            req.counter_decrement = *decrement != 0;
        }
        if (req.opcode
            == Opcode::CounterAdjust
            || req.opcode
            == Opcode::CounterMerge
            || req.opcode
            == Opcode::CounterGet) {
            if (req.ttl.has_value()
                || req.expires_at.has_value()
                || req.tombstone
                || req.version
                != 0
                || req.writer_node_hash
                != 0) {
                return err<DecodedRequest>(
                    Error(Errc::InvalidArgument, "counter opcode has LWW metadata")
                );
            }
        }
        // Trace context — optional trailing data gated by flag 0x04.
        if ((*flags & K_FLAG_HAS_TRACE) != 0 && r.remaining() >= sizeof(uint64_t) * 2) {
            auto tid = mustRead<uint64_t>(r);
            auto sid = mustRead<uint64_t>(r);
            if (tid && sid) {
                req.trace_id = *tid;
                // Mirror the encoder (which gates span on trace_id != 0): a
                // zero trace carries no span, so decode canonicalizes the
                // same way and decode->encode->decode is a fixed point.
                req.span_id = *tid == 0 ? 0 : *sid;
            }
        }
        // Auth token — optional trailing data gated by flag 0x08.
        if ((*flags & K_FLAG_HAS_AUTH) != 0 && r.remaining() >= 32) {
            auto auth_bytes = r.readBytes(32);
            if (auth_bytes) {
                decoded.auth_token.assign(asChars(*auth_bytes));
            }
        }
        return ok(std::move(decoded));
    }

    auto encode(const Response& res) -> Result<std::vector<std::byte>> {
        std::vector<std::byte> buf;
        auto result = encodeInto(res, buf);
        if (!result.has_value()) {
            return err<std::vector<std::byte>>(result.error());
        }
        return ok(std::move(buf));
    }

    auto encodeInto(const Response& res, std::vector<std::byte>& out) -> Result<void> {
        uint8_t flags = 0;
        bool has_version_meta = res.version != 0 || res.writer_node_hash != 0;
        if (has_version_meta) {
            flags |= 0x01U;
        }
        if (res.expires_at.has_value()) {
            flags |= K_FLAG_HAS_EXPIRES_AT;
        }
        if (res.tombstone) {
            flags |= K_FLAG_HAS_TOMBSTONE;
        }

        bool has_trace = res.trace_id != 0;
        if (has_trace) {
            flags |= K_FLAG_HAS_TRACE;
        }

        size_t payload_size = 0;
        const auto add = [&](size_t n) {
            return addPayloadSize(payload_size, n);
        };
        if (!add(sizeof(uint8_t) * 2 + sizeof(uint32_t))) { // status, flags, has_val
            return err(Error(Errc::InvalidArgument, "message too large"));
        }
        if (has_version_meta && !add(sizeof(uint64_t) * 2)) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }
        if (res.expires_at.has_value() && !add(sizeof(uint64_t))) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }
        if (res.value.has_value()
            && (res.value->size()
                > std::numeric_limits<uint32_t>::max()
                || !add(res.value->size()))) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }
        if (has_trace && !add(sizeof(uint64_t) * 2)) {
            return err(Error(Errc::InvalidArgument, "message too large"));
        }

        auto total = frameSizeForPayload(payload_size);
        if (!total) {
            return err(total.error());
        }

        out.resize(*total); // reuses capacity across calls
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
            w.write(
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(res.expires_at->time_since_epoch()).count()
                )
            );
        }

        w.write(static_cast<uint32_t>(res.value.has_value() ? 1 : 0));
        if (res.value.has_value()) {
            w.writeString(*res.value);
        }
        if (has_trace) {
            w.write(res.trace_id);
            w.write(res.span_id);
        }

        assert(w.offset() == out.size());
        return ok();
    }

    auto decodeResponse(std::span<const std::byte> frame) -> Result<Response> {
        auto parsed_frame = parseFrame(frame);
        if (!parsed_frame) {
            return err<Response>(parsed_frame.error());
        }

        const FrameView& view = *parsed_frame;
        ByteReader r(view.payload);
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

        res.tombstone = (flags & K_FLAG_HAS_TOMBSTONE) != 0;
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
            // Account for trace context bytes that follow the value.
            size_t trace_size = (flags & K_FLAG_HAS_TRACE) ? (sizeof(uint64_t) * 2) : 0;
            size_t remaining = r.remaining();
            // Bytes after payload_len are already excluded by FrameView. Keep
            // the legacy short-trace rule: reserve trace bytes only when the
            // complete 16-byte field is present; otherwise value length is 0.
            size_t val_len = remaining > trace_size ? remaining - trace_size : 0;
            // Materialize even when empty: has_val set with zero bytes means
            // "present but empty", which must survive as value="" rather
            // than collapsing to nullopt (missing).
            auto val_view = r.readStringView(val_len);
            if (!val_view) {
                return err<Response>(val_view.error());
            }
            res.value = std::string(*val_view);
        }
        if ((flags & K_FLAG_HAS_TRACE) != 0 && r.remaining() >= sizeof(uint64_t) * 2) {
            auto tid = mustRead<uint64_t>(r);
            auto sid = mustRead<uint64_t>(r);
            if (tid && sid) {
                res.trace_id = *tid;
                // Mirror the encoder (which gates span on trace_id != 0):
                // see the request decoder for the fixed-point rationale.
                res.span_id = *tid == 0 ? 0 : *sid;
            }
        }
        return ok(std::move(res));
    }
} // namespace cinder::net
