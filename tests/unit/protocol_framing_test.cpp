#include <gtest/gtest.h>
#include <vector>

#include "cinder/net/protocol.hpp"

using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace cinder::net {
namespace {

TEST(ProtocolTest, EncodeGetRequest) {
    Request req;
    req.opcode = Opcode::Get;
    req.key = "hello";

    auto result = encode(req);
    ASSERT_TRUE(result.has_value());

    auto& buf = result.value();
    ASSERT_GE(buf.size(), K_FRAME_HEADER_SIZE);
    EXPECT_EQ(buf[0], std::byte{K_MAGIC});
    EXPECT_EQ(buf[1], std::byte{K_VERSION});
    EXPECT_EQ(buf[2], std::byte{static_cast<uint8_t>(Opcode::Get)});
}

TEST(ProtocolTest, EncodeDecodeGet) {
    Request req;
    req.opcode = Opcode::Get;
    req.key = "mykey";

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().opcode, Opcode::Get);
    EXPECT_EQ(decoded.value().key, "mykey");
    EXPECT_TRUE(decoded.value().value.empty());
    EXPECT_FALSE(decoded.value().ttl.has_value());
}

TEST(ProtocolTest, EncodeDecodeSet) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "k";
    req.value = "v";
    req.ttl = milliseconds(5'000);

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().opcode, Opcode::Set);
    EXPECT_EQ(decoded.value().key, "k");
    EXPECT_EQ(decoded.value().value, "v");
    ASSERT_TRUE(decoded.value().ttl.has_value());
    EXPECT_EQ(decoded.value().ttl->count(), 5'000);
}

TEST(ProtocolTest, EncodeDecodeDel) {
    Request req;
    req.opcode = Opcode::Del;
    req.key = "delete_me";

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().opcode, Opcode::Del);
    EXPECT_EQ(decoded.value().key, "delete_me");
}

TEST(ProtocolTest, DecodeBadMagic) {
    std::vector<std::byte> buf(K_FRAME_HEADER_SIZE, std::byte{0});
    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeUnknownOpcode) {
    std::vector<std::byte> buf(K_FRAME_HEADER_SIZE, std::byte{0});
    buf[0] = std::byte{K_MAGIC};
    buf[1] = std::byte{K_VERSION};
    buf[2] = std::byte{0};
    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());

    buf[2] = std::byte{200};
    result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeTruncatedFrame) {
    std::vector<std::byte> buf(3, std::byte{K_MAGIC});
    buf[0] = std::byte{K_MAGIC};
    buf[1] = std::byte{K_VERSION};
    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, EncodeDecodeResponseOk) {
    Response res;
    res.status = Errc::OK;
    res.value = "result_value";

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::OK);
    ASSERT_TRUE(decoded.value().value.has_value());
    EXPECT_EQ(*decoded.value().value, "result_value");
}

TEST(ProtocolTest, EncodeDecodeResponseNotFound) {
    Response res;
    res.status = Errc::NotFound;

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::NotFound);
    EXPECT_FALSE(decoded.value().value.has_value());
}

TEST(ProtocolTest, EncodeIntoRequestMatchesEncode) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "some-key";
    req.value = "some-value";
    req.ttl = milliseconds(5'000);
    req.version = 42;
    req.writer_node_hash = 0xDEADBEEF;

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    std::vector<std::byte> buf;
    auto into = encodeInto(req, buf);
    ASSERT_TRUE(into.has_value());
    EXPECT_EQ(buf, encoded.value());
}

TEST(ProtocolTest, EncodeIntoRequestReusesBuffer) {
    Request small;
    small.opcode = Opcode::Get;
    small.key = "a";

    Request large;
    large.opcode = Opcode::Set;
    large.key = "k" + std::string(100, 'x');
    large.value = std::string(200, 'y');
    large.ttl = milliseconds(1'000);

    std::vector<std::byte> buf;
    ASSERT_TRUE(encodeInto(small, buf).has_value());
    auto small_bytes = buf;

    ASSERT_TRUE(encodeInto(large, buf).has_value());
    auto large_bytes = buf;

    // Buffer reuse must not corrupt output for either size.
    auto small_expected = encode(small);
    auto large_expected = encode(large);
    ASSERT_TRUE(small_expected.has_value());
    ASSERT_TRUE(large_expected.has_value());
    EXPECT_EQ(small_bytes, small_expected.value());
    EXPECT_EQ(large_bytes, large_expected.value());
}

TEST(ProtocolTest, EncodeIntoResponseMatchesEncode) {
    Response res;
    res.status = Errc::OK;
    res.value = "result_value";

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    std::vector<std::byte> buf;
    auto into = encodeInto(res, buf);
    ASSERT_TRUE(into.has_value());
    EXPECT_EQ(buf, encoded.value());
}

TEST(ProtocolTest, EncodeDecodeExpiresAt) {
    Request req;
    req.opcode = Opcode::Replicate;
    req.key = "k";
    req.value = "v";
    req.version = 7;
    req.writer_node_hash = 42;
    req.expires_at = system_clock::time_point(milliseconds(1'700'000'000'123));

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded.value().expires_at.has_value());
    EXPECT_EQ(*decoded.value().expires_at, *req.expires_at);
    EXPECT_FALSE(decoded.value().ttl.has_value());
}

TEST(ProtocolTest, EncodeDecodeExpiresAtAbsent) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "k";
    req.value = "v";
    req.ttl = milliseconds(5'000);

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_FALSE(decoded.value().expires_at.has_value());
    ASSERT_TRUE(decoded.value().ttl.has_value());
    EXPECT_EQ(decoded.value().ttl->count(), 5'000);
}

TEST(ProtocolTest, EncodeDecodeGetVersionedRequest) {
    Request req;
    req.opcode = Opcode::GetVersioned;
    req.key = "my-key";

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());
    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().opcode, Opcode::GetVersioned);
    EXPECT_EQ(decoded.value().key, "my-key");
}

TEST(ProtocolTest, EncodeDecodeVersionedResponse) {
    Response res;
    res.status = Errc::OK;
    res.value = "some-data";
    res.version = 42;
    res.writer_node_hash = 0xDEADBEEF;

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::OK);
    EXPECT_EQ(decoded.value().version, 42);
    EXPECT_EQ(decoded.value().writer_node_hash, 0xDEADBEEF);
    ASSERT_TRUE(decoded.value().value.has_value());
    EXPECT_EQ(*decoded.value().value, "some-data");
}

TEST(ProtocolTest, EncodeDecodeVersionedResponseNoValue) {
    Response res;
    res.status = Errc::NotFound;
    res.version = 99;
    res.writer_node_hash = 7;

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::NotFound);
    EXPECT_EQ(decoded.value().version, 99);
    EXPECT_EQ(decoded.value().writer_node_hash, 7);
    EXPECT_FALSE(decoded.value().value.has_value());
}

TEST(ProtocolTest, PlainResponseNoVersionMeta) {
    Response res;
    res.status = Errc::OK;
    res.value = "val";

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::OK);
    EXPECT_EQ(decoded.value().version, 0);
    EXPECT_EQ(decoded.value().writer_node_hash, 0);
    ASSERT_TRUE(decoded.value().value.has_value());
    EXPECT_EQ(*decoded.value().value, "val");
}

TEST(ProtocolTest, DecodeOversizedPayload) {
    // payload_len > K_MAX_MESSAGE_SIZE should return error
    std::vector<std::byte> frame(K_FRAME_HEADER_SIZE);
    frame[0] = std::byte{K_MAGIC};
    frame[1] = std::byte{K_VERSION};
    frame[2] = std::byte{static_cast<uint8_t>(Opcode::Set)};
    // Write a huge payload_len (big-endian)
    uint32_t huge_len = 0xFFFFFFFF;
    std::memcpy(&frame[3], &huge_len, sizeof(huge_len));

    auto result = decode(frame);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), Errc::InvalidArgument);
}

TEST(ProtocolTest, EncodeEmptyKey) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "";
    req.value = "value";

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().key, "");
    EXPECT_EQ(decoded.value().value, "value");
}

TEST(ProtocolTest, EncodeBothTtlAndExpiresAt) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "k";
    req.value = "v";
    req.ttl = milliseconds(5'000);
    req.expires_at = system_clock::time_point(milliseconds(1'700'000'000'000));

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    // Both should be present in decoded request
    EXPECT_TRUE(decoded.value().ttl.has_value());
    EXPECT_TRUE(decoded.value().expires_at.has_value());
}

TEST(ProtocolTest, DecodeTruncatedResponse) {
    // Response frame cut short after header — decoder should reject it
    Response res;
    res.status = Errc::OK;
    res.value = "hello world";

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    // Truncate to just header + status (no value, no has_val field)
    std::vector<std::byte> truncated(
        encoded.value().begin(), encoded.value().begin() + K_FRAME_HEADER_SIZE + 1);
    // Fix payload_len to match truncation
    uint32_t trunc_len = 1; // just status byte
    std::memcpy(&truncated[3], &trunc_len, sizeof(trunc_len));

    auto decoded = decodeResponse(truncated);
    // Truncated frame is invalid — decoder should reject it
    EXPECT_FALSE(decoded.has_value());
}

TEST(ProtocolTest, DecodeResponseNoValue) {
    Response res;
    res.status = Errc::NotFound;

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::NotFound);
    EXPECT_FALSE(decoded.value().value.has_value());
}

TEST(ProtocolTest, DecodeVersionedResponseWithAllFlags) {
    Response res;
    res.status = Errc::OK;
    res.value = "val";
    res.version = 42;
    res.writer_node_hash = 99;
    res.expires_at = system_clock::time_point(milliseconds(1'700'000'000'000));

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decodeResponse(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::OK);
    EXPECT_EQ(decoded.value().version, 42);
    EXPECT_EQ(decoded.value().writer_node_hash, 99);
    EXPECT_TRUE(decoded.value().expires_at.has_value());
}

TEST(ProtocolTest, DecodeTruncatedAfterFlagsNoTtlRoom) {
    // Header claims payload_len that includes flags byte but not the ttl.
    // mustRead must return an error, not crash.
    std::vector<std::byte> frame(K_FRAME_HEADER_SIZE + 2);
    frame[0] = std::byte{K_MAGIC};
    frame[1] = std::byte{K_VERSION};
    frame[2] = std::byte{static_cast<uint8_t>(Opcode::Set)};
    uint32_t payload_len = 2; // only flags + 1 extra byte
    std::memcpy(&frame[3], &payload_len, sizeof(payload_len));
    frame[7] = std::byte{K_FLAG_HAS_TTL}; // flags say TTL is present
    frame[8] = std::byte{0x00};           // but no room for the 4-byte ttl

    auto result = decode(frame);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeTruncatedAfterOpcode) {
    // Header says payload_len = 1, but decode needs to read flags (1 byte).
    // After opcode + payload_len the reader has 0 bytes left.
    std::vector<std::byte> frame(K_FRAME_HEADER_SIZE + 1);
    frame[0] = std::byte{K_MAGIC};
    frame[1] = std::byte{K_VERSION};
    frame[2] = std::byte{static_cast<uint8_t>(Opcode::Set)};
    uint32_t payload_len = 1; // one byte only
    std::memcpy(&frame[3], &payload_len, sizeof(payload_len));
    frame[7] = std::byte{0x00}; // the one payload byte

    auto result = decode(frame);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeExpiresAtOverflow) {
    // expires_at = 0xFFFFFFFFFFFFFFFF should be rejected, not overflow.
    Request req;
    req.opcode = Opcode::Set;
    req.key = "k";
    req.expires_at = system_clock::time_point(milliseconds(1'700'000'000'000));

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    // Overwrite the expires_at field in the encoded frame with max uint64_t.
    // The expires_at field starts after header(7) + flags(1) + expires_at present.
    // We need to find it: header(7) + flags(1) = offset 8, then expires_at(8 bytes).
    auto& buf = encoded.value();
    ASSERT_GE(buf.size(), K_FRAME_HEADER_SIZE + 9);
    uint64_t max_val = 0xFFFFFFFFFFFFFFFF;
    std::memcpy(&buf[K_FRAME_HEADER_SIZE + 1], &max_val, sizeof(max_val));

    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeResponseExpiresAtOverflow) {
    Response res;
    res.status = Errc::OK;
    res.expires_at = system_clock::time_point(milliseconds(1'700'000'000'000));
    res.version = 1;
    res.writer_node_hash = 1;

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.value().size() >= K_FRAME_HEADER_SIZE + 18);

    // Overwrite the expires_at field in the encoded response.
    // Response layout: header(7) + status(1) + flags(1) + version(8) + writer_hash(8)
    //   + expires_at(8) + has_val(4) + ...
    // expires_at is at offset 7 + 1 + 1 + 8 + 8 = 25
    auto& buf = encoded.value();
    uint64_t max_val = 0xFFFFFFFFFFFFFFFF;
    std::memcpy(&buf[K_FRAME_HEADER_SIZE + 18], &max_val, sizeof(max_val));

    auto result = decodeResponse(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeResponseTruncatedAfterStatus) {
    // Valid header but payload only has status byte — no flags, no has_val.
    std::vector<std::byte> frame(K_FRAME_HEADER_SIZE + 1);
    frame[0] = std::byte{K_MAGIC};
    frame[1] = std::byte{K_VERSION};
    frame[2] = std::byte{0};
    uint32_t payload_len = 1; // just status
    std::memcpy(&frame[3], &payload_len, sizeof(payload_len));
    frame[7] = std::byte{0x00}; // status = OK

    auto result = decodeResponse(frame);
    EXPECT_FALSE(result.has_value());
}
} // namespace
} // namespace cinder::net
