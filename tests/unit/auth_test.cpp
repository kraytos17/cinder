#include <gtest/gtest.h>
#include <string>

#include "cinder/common/hmac.hpp"
#include "cinder/net/protocol.hpp"

using namespace cinder;
using namespace cinder::net;

TEST(HmacTest, ComputeHmacSha256) {
    auto digest = computeHmacSha256("secret", "message");
    EXPECT_EQ(digest.size(), 32U);
    auto digest2 = computeHmacSha256("secret", "message");
    EXPECT_EQ(digest, digest2);
}

TEST(HmacTest, DifferentKeysProduceDifferentDigests) {
    auto d1 = computeHmacSha256("key1", "message");
    auto d2 = computeHmacSha256("key2", "message");
    EXPECT_NE(d1, d2);
}

TEST(HmacTest, DifferentMessagesProduceDifferentDigests) {
    auto d1 = computeHmacSha256("key", "msg1");
    auto d2 = computeHmacSha256("key", "msg2");
    EXPECT_NE(d1, d2);
}

TEST(HmacTest, GenerateAuthToken) {
    auto token = generateAuthToken("my-secret", "node1");
    EXPECT_EQ(token.size(), 32U);
    auto token2 = generateAuthToken("my-secret", "node1");
    EXPECT_EQ(token, token2);
}

TEST(HmacTest, GenerateAuthTokenEmptySecret) {
    auto token = generateAuthToken("", "node1");
    EXPECT_TRUE(token.empty());
}

TEST(HmacTest, VerifyAuthToken) {
    std::string secret = "cluster-secret";
    std::string node_id = "node1";
    auto token = generateAuthToken(secret, node_id);
    EXPECT_TRUE(verifyAuthToken(secret, node_id, token));
}

TEST(HmacTest, VerifyAuthTokenWrongSecret) {
    auto token = generateAuthToken("secret-a", "node1");
    EXPECT_FALSE(verifyAuthToken("secret-b", "node1", token));
}

TEST(HmacTest, VerifyAuthTokenWrongNode) {
    auto token = generateAuthToken("secret", "node1");
    EXPECT_FALSE(verifyAuthToken("secret", "node2", token));
}

TEST(HmacTest, VerifyAuthTokenEmptyToken) {
    EXPECT_FALSE(verifyAuthToken("secret", "node1", {}));
}

TEST(HmacTest, VerifyAuthTokenTooShort) {
    EXPECT_FALSE(verifyAuthToken("secret", "node1", "short"));
}

TEST(HmacTest, VerifyAuthTokenTooLong) {
    std::string long_token(64, 'x');
    EXPECT_FALSE(verifyAuthToken("secret", "node1", long_token));
}

TEST(ProtocolAuthTest, RequestEncodeDecodeWithAuth) {
    Request req;
    req.opcode = Opcode::Replicate;
    req.key = "test-key";
    req.value = "test-value";
    std::string auth_token(32, '\xAB'); // 32-byte dummy token

    auto encoded = encode(req, auth_token);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->req.opcode, Opcode::Replicate);
    EXPECT_EQ(decoded->req.key, "test-key");
    EXPECT_EQ(decoded->req.value, "test-value");
    EXPECT_EQ(decoded->auth_token.size(), 32U);
    EXPECT_EQ(decoded->auth_token, auth_token);
}

TEST(ProtocolAuthTest, RequestEncodeDecodeWithoutAuth) {
    Request req;
    req.opcode = Opcode::Get;
    req.key = "my-key";

    auto encoded = encode(req);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->req.opcode, Opcode::Get);
    EXPECT_EQ(decoded->req.key, "my-key");
    EXPECT_TRUE(decoded->auth_token.empty());
}

TEST(ProtocolAuthTest, RequestWithAuthAndTrace) {
    Request req;
    req.opcode = Opcode::AntiEntropySync;
    req.key = "sync-key";
    req.trace_id = 42;
    req.span_id = 99;
    std::string auth_token(32, '\xCD');

    auto encoded = encode(req, auth_token);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->req.trace_id, 42U);
    EXPECT_EQ(decoded->req.span_id, 99U);
    EXPECT_EQ(decoded->auth_token, auth_token);
}

TEST(ProtocolAuthTest, RequestWithAuthAndTtl) {
    Request req;
    req.opcode = Opcode::Set;
    req.key = "ttl-key";
    req.value = "ttl-value";
    req.ttl = std::chrono::milliseconds(5'000);
    std::string auth_token(32, '\xEF');

    auto encoded = encode(req, auth_token);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->req.ttl.has_value());
    EXPECT_EQ(decoded->req.ttl->count(), 5'000);
    EXPECT_EQ(decoded->auth_token, auth_token);
}

TEST(ProtocolAuthTest, InvalidAuthTokenSizeRejected) {
    Request req;
    req.opcode = Opcode::Replicate;
    req.key = "k";
    auto encoded = encode(req, std::string("short"));
    EXPECT_FALSE(encoded.has_value());
}

TEST(ProtocolAuthTest, AuthTokenSurvivesRoundTrip) {
    std::string secret = "test-secret-123";
    std::string node_id = "node-alpha";
    auto token = generateAuthToken(secret, node_id);

    Request req;
    req.opcode = Opcode::Gossip;
    req.key = "gossip";

    auto encoded = encode(req, token);
    ASSERT_TRUE(encoded.has_value());

    auto decoded = decode(*encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(verifyAuthToken(secret, node_id, decoded->auth_token));
}
