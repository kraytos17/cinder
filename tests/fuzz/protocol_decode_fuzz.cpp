#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/net/protocol.hpp"

namespace {

// Decode rejects expires_at past int64max/1e6 ms; keep FDP-built expiries
// inside the accepted range, at millisecond precision so the ms-truncating
// encoder roundtrips exactly.
constexpr int64_t K_MAX_EXPIRY_MS = 9'000'000'000'000LL;

static auto msTime(int64_t ms) -> std::chrono::system_clock::time_point {
    return std::chrono::system_clock::time_point(std::chrono::milliseconds(ms));
}

static auto makeRequest(FuzzedDataProvider& fdp) -> cinder::net::Request {
    using Op = cinder::net::Opcode;
    cinder::net::Request req;
    req.opcode = fdp.PickValueInArray(
        {Op::Get,
         Op::Set,
         Op::Del,
         Op::Ping,
         Op::Gossip,
         Op::Replicate,
         Op::Hint,
         Op::GetVersioned,
         Op::AntiEntropyDigest,
         Op::AntiEntropySync,
         Op::AdminInfo,
         Op::AdminCluster,
         Op::AdminRing,
         Op::AdminCompact,
         Op::AdminConfigReload,
         Op::AdminShutdown});
    req.trace_id = fdp.ConsumeIntegral<uint64_t>();
    // Encoder gates span on trace_id != 0; a lone span would not survive.
    req.span_id = req.trace_id == 0 ? 0 : fdp.ConsumeIntegral<uint64_t>();
    req.key = fdp.ConsumeRandomLengthString(1024);
    req.value = fdp.ConsumeBool() ? fdp.ConsumeRandomLengthString(1024)
                                  : fdp.ConsumeRandomLengthString(65'536);
    req.version = fdp.ConsumeIntegral<cinder::Version>();
    req.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
    if (fdp.ConsumeBool()) {
        // u32 on the wire: bound the count so the cast is lossless.
        req.ttl = std::chrono::milliseconds(fdp.ConsumeIntegral<uint32_t>());
    }
    if (fdp.ConsumeBool()) {
        req.expires_at =
            msTime(fdp.ConsumeIntegralInRange<int64_t>(0, K_MAX_EXPIRY_MS));
    }
    return req;
}

static auto makeAuth(FuzzedDataProvider& fdp) -> std::string {
    // Encoder accepts empty or exactly 32 bytes — nothing else is encodable.
    // ConsumeBytesAsString returns fewer bytes near end-of-input, so gate on
    // availability first.
    if (!fdp.ConsumeBool() || fdp.remaining_bytes() < 32) {
        return {};
    }
    return fdp.ConsumeBytesAsString(32);
}

static void checkRequestEqual(
    const cinder::net::Request& a,
    const cinder::net::Request& b) {
    assert(a.opcode == b.opcode);
    assert(a.trace_id == b.trace_id);
    assert(a.span_id == b.span_id);
    assert(a.key == b.key);
    assert(a.value == b.value);
    assert(a.version == b.version);
    assert(a.writer_node_hash == b.writer_node_hash);
    assert(a.ttl == b.ttl);
    assert(a.expires_at == b.expires_at);
}

static void requestRoundtrip(FuzzedDataProvider& fdp) {
    auto req = makeRequest(fdp);
    auto auth = makeAuth(fdp);
    auto first = cinder::net::encode(req, auth);
    assert(first.has_value());
    auto decoded = cinder::net::decode(
        std::span<const std::byte>(first->data(), first->size())
    );
    assert(decoded.has_value());
    checkRequestEqual(decoded->req, req);
    assert(decoded->auth_token == auth);
    // Canonical encoding: decoding and re-encoding is a fixed point.
    std::vector<std::byte> second;
    auto res = cinder::net::encodeInto(decoded->req, second, decoded->auth_token);
    assert(res.has_value());
    assert(second == *first);
}

static auto makeResponse(FuzzedDataProvider& fdp) -> cinder::net::Response {
    cinder::net::Response res;
    res.status = fdp.PickValueInArray(
        {cinder::Errc::OK,
         cinder::Errc::NotFound,
         cinder::Errc::CapacityExceeded,
         cinder::Errc::InvalidArgument,
         cinder::Errc::TtlExpired,
         cinder::Errc::NotSupported,
         cinder::Errc::InternalError,
         cinder::Errc::Timeout,
         cinder::Errc::NotReady});
    if (fdp.ConsumeBool()) {
        res.value = fdp.ConsumeRandomLengthString(1024);
    }
    // Encoder emits version-meta iff either field is nonzero; both decode
    // back exactly either way.
    res.version = fdp.ConsumeIntegral<cinder::Version>();
    res.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
    if (fdp.ConsumeBool()) {
        res.expires_at =
            msTime(fdp.ConsumeIntegralInRange<int64_t>(0, K_MAX_EXPIRY_MS));
    }
    res.trace_id = fdp.ConsumeIntegral<uint64_t>();
    res.span_id = res.trace_id == 0 ? 0 : fdp.ConsumeIntegral<uint64_t>();
    return res;
}

static void checkResponseEqual(
    const cinder::net::Response& a,
    const cinder::net::Response& b) {
    assert(a.status == b.status);
    assert(a.value == b.value);
    assert(a.version == b.version);
    assert(a.writer_node_hash == b.writer_node_hash);
    assert(a.expires_at == b.expires_at);
    assert(a.trace_id == b.trace_id);
    assert(a.span_id == b.span_id);
}

static void responseRoundtrip(FuzzedDataProvider& fdp) {
    auto res = makeResponse(fdp);
    auto first = cinder::net::encode(res);
    assert(first.has_value());
    auto decoded = cinder::net::decodeResponse(
        std::span<const std::byte>(first->data(), first->size())
    );
    assert(decoded.has_value());
    checkResponseEqual(*decoded, res);
    std::vector<std::byte> second;
    auto enc = cinder::net::encodeInto(*decoded, second);
    assert(enc.has_value());
    assert(second == *first);
}

static void raw(FuzzedDataProvider& fdp) {
    // Arbitrary bytes through both decoders: never crash, only errors.
    auto bytes = fdp.ConsumeRemainingBytes<uint8_t>();
    auto span = std::span<const std::byte>(
        reinterpret_cast<const std::byte*>(bytes.data()), bytes.size()
    );
    (void)cinder::net::decode(span); // NOLINT
    (void)cinder::net::decodeResponse(span); // NOLINT
}

static void mutate(FuzzedDataProvider& fdp) {
    // Corrupt a valid encoding (flip/truncate): the parser must reject or
    // decode without crashing. No content oracle — a flipped key byte is a
    // different *valid* message — but every accepted decode must itself
    // re-encode to a fixed point (canonical form).
    const bool is_request = fdp.ConsumeBool();
    std::vector<std::byte> bytes;
    if (is_request) {
        auto enc = cinder::net::encode(makeRequest(fdp), makeAuth(fdp));
        assert(enc.has_value());
        bytes = std::move(*enc);
    } else {
        auto enc = cinder::net::encode(makeResponse(fdp));
        assert(enc.has_value());
        bytes = std::move(*enc);
    }
    if (bytes.empty()) {
        return;
    }
    const size_t at = fdp.ConsumeIntegralInRange<size_t>(0, bytes.size() - 1);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            bytes[at] ^= static_cast<std::byte>(1U << fdp.ConsumeIntegralInRange(0, 7));
            break;
        case 1:
            bytes[at] ^= static_cast<std::byte>(fdp.ConsumeIntegral<uint8_t>());
            break;
        default:
            bytes.resize(at);
            break;
    }
    auto span = std::span<const std::byte>(bytes.data(), bytes.size());
    if (is_request) {
        auto dec = cinder::net::decode(span);
        if (dec.has_value()) {
            std::vector<std::byte> re;
            auto enc = cinder::net::encodeInto(dec->req, re, dec->auth_token);
            assert(enc.has_value());
            auto back = cinder::net::decode(
                std::span<const std::byte>(re.data(), re.size())
            );
            assert(back.has_value());
            checkRequestEqual(back->req, dec->req);
        }
    } else {
        auto dec = cinder::net::decodeResponse(span);
        if (dec.has_value()) {
            std::vector<std::byte> re;
            auto enc = cinder::net::encodeInto(*dec, re);
            assert(enc.has_value());
            auto back = cinder::net::decodeResponse(
                std::span<const std::byte>(re.data(), re.size())
            );
            assert(back.has_value());
            checkResponseEqual(*back, *dec);
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 3)) {
        case 0:
            requestRoundtrip(fdp);
            break;
        case 1:
            responseRoundtrip(fdp);
            break;
        case 2:
            raw(fdp);
            break;
        default:
            mutate(fdp);
            break;
    }
    return 0;
}
