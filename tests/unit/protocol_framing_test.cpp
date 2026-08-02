#include <gtest/gtest.h>
#include <vector>

#include "cinder/net/protocol.hpp"

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
    req.ttl = std::chrono::milliseconds(5'000);

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
    req.ttl = std::chrono::milliseconds(5'000);
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
    large.ttl = std::chrono::milliseconds(1'000);

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
} // namespace
} // namespace cinder::net
