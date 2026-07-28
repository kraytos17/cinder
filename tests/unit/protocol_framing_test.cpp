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
    ASSERT_GE(buf.size(), kFrameHeaderSize);
    EXPECT_EQ(buf[0], std::byte{kMagic});
    EXPECT_EQ(buf[1], std::byte{kVersion});
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
    std::vector<std::byte> buf(kFrameHeaderSize, std::byte{0});
    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, DecodeTruncatedFrame) {
    std::vector<std::byte> buf(3, std::byte{kMagic});
    buf[0] = std::byte{kMagic};
    buf[1] = std::byte{kVersion};
    auto result = decode(buf);
    EXPECT_FALSE(result.has_value());
}

TEST(ProtocolTest, EncodeDecodeResponseOk) {
    Response res;
    res.status = Errc::OK;
    res.value = "result_value";

    auto encoded = encode(res);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode_response(encoded.value());
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

    auto decoded = decode_response(encoded.value());
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded.value().status, Errc::NotFound);
    EXPECT_FALSE(decoded.value().value.has_value());
}
} // namespace
} // namespace cinder::net
