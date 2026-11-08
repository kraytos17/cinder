#include <array>
#include <gtest/gtest.h>
#include <limits>
#include <vector>

#include "cinder/net/byte_reader.hpp"
#include "cinder/net/protocol.hpp"

using std::chrono::milliseconds;
using std::chrono::system_clock;

namespace cinder::net {
    namespace {

        TEST(ByteReaderTest, OversizedLengthsFailWithoutAdvancing) {
            const std::array<std::byte, 1> data{std::byte{0x2A}};
            ByteReader reader(data);

            auto value = reader.read<uint64_t>();
            EXPECT_FALSE(value.has_value());
            EXPECT_EQ(reader.offset(), 0U);

            auto bytes = reader.readBytes(std::numeric_limits<size_t>::max());
            EXPECT_FALSE(bytes.has_value());
            EXPECT_EQ(reader.offset(), 0U);

            auto string = reader.readStringView(std::numeric_limits<size_t>::max());
            EXPECT_FALSE(string.has_value());
            EXPECT_EQ(reader.offset(), 0U);
            EXPECT_EQ(reader.remaining(), 1U);
        }

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
            EXPECT_EQ(decoded.value().req.opcode, Opcode::Get);
            EXPECT_EQ(decoded.value().req.key, "mykey");
            EXPECT_TRUE(decoded.value().req.value.empty());
            EXPECT_FALSE(decoded.value().req.ttl.has_value());
        }

        TEST(ProtocolTest, EncodeDecodeSet) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = "k";
            req.value = "v";
            req.ttl = milliseconds(5000);

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded.value().req.opcode, Opcode::Set);
            EXPECT_EQ(decoded.value().req.key, "k");
            EXPECT_EQ(decoded.value().req.value, "v");
            ASSERT_TRUE(decoded.value().req.ttl.has_value());
            EXPECT_EQ(decoded.value().req.ttl->count(), 5000);
        }

        TEST(ProtocolTest, EncodeDecodeDel) {
            Request req;
            req.opcode = Opcode::Del;
            req.key = "delete_me";

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded.value().req.opcode, Opcode::Del);
            EXPECT_EQ(decoded.value().req.key, "delete_me");
        }

        TEST(ProtocolTest, EncodeDecodeCounterAdjust) {
            Request req;
            req.opcode = Opcode::CounterAdjust;
            req.key = "views";
            req.counter_delta = 42;
            req.counter_decrement = true;

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decode(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->req.opcode, Opcode::CounterAdjust);
            EXPECT_EQ(decoded->req.key, "views");
            EXPECT_EQ(decoded->req.counter_delta, 42U);
            EXPECT_TRUE(decoded->req.counter_decrement);
        }

        TEST(ProtocolTest, CounterMergeAndGetOpcodesRoundTrip) {
            for (Opcode opcode : {Opcode::CounterMerge, Opcode::CounterGet}) {
                Request req;
                req.opcode = opcode;
                req.key = "counter-key";
                req.value = opcode == Opcode::CounterMerge ? "CRDC-state" : "";
                auto encoded = encode(req);
                ASSERT_TRUE(encoded.has_value());
                auto decoded = decode(*encoded);
                ASSERT_TRUE(decoded.has_value());
                EXPECT_EQ(decoded->req.opcode, opcode);
                EXPECT_EQ(decoded->req.key, req.key);
                EXPECT_EQ(decoded->req.value, req.value);
            }
        }

        TEST(ProtocolTest, CounterAdjustRejectsInvalidDirection) {
            Request req;
            req.opcode = Opcode::CounterAdjust;
            req.key = "views";
            req.counter_delta = 1;
            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            // Final request-body byte is CounterAdjust's decrement boolean.
            encoded->back() = std::byte{2};
            auto decoded = decode(*encoded);
            EXPECT_FALSE(decoded.has_value());
            EXPECT_EQ(decoded.error().code(), Errc::InvalidArgument);
        }

        TEST(ProtocolTest, CounterAdjustRejectsTruncatedSuffix) {
            Request req;
            req.opcode = Opcode::CounterAdjust;
            req.key = "views";
            req.counter_delta = 2;
            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            encoded->resize(encoded->size() - 1);
            // Keep the frame length consistent with truncation so decode
            // reaches the required direction-byte check.
            const uint32_t payload_size =
                static_cast<uint32_t>(encoded->size() - K_FRAME_HEADER_SIZE);
            encoded->at(3) = std::byte{static_cast<uint8_t>(payload_size >> 24U)};
            encoded->at(4) = std::byte{static_cast<uint8_t>(payload_size >> 16U)};
            encoded->at(5) = std::byte{static_cast<uint8_t>(payload_size >> 8U)};
            encoded->at(6) = std::byte{static_cast<uint8_t>(payload_size)};
            auto decoded = decode(*encoded);
            EXPECT_FALSE(decoded.has_value());
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

        TEST(ProtocolTest, EncodeDecodeResponseEmptyValueStaysPresent) {
            // Present-but-empty must survive the wire as value="", not
            // collapse to nullopt (missing) — quorum readers treat nullopt
            // as absent.
            Response res;
            res.status = Errc::OK;
            res.value = "";

            auto encoded = encode(res);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decodeResponse(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            ASSERT_TRUE(decoded.value().value.has_value());
            EXPECT_TRUE(decoded.value().value->empty());
        }

        TEST(ProtocolTest, EncodeIntoRequestMatchesEncode) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = "some-key";
            req.value = "some-value";
            req.ttl = milliseconds(5000);
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
            large.ttl = milliseconds(1000);

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
            EXPECT_TRUE(decoded.value().req.expires_at.has_value());
            EXPECT_EQ(*decoded.value().req.expires_at, *req.expires_at);
            EXPECT_FALSE(decoded.value().req.ttl.has_value());
        }

        TEST(ProtocolTest, EncodeDecodeExpiresAtAbsent) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = "k";
            req.value = "v";
            req.ttl = milliseconds(5000);

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_FALSE(decoded.value().req.expires_at.has_value());
            ASSERT_TRUE(decoded.value().req.ttl.has_value());
            EXPECT_EQ(decoded.value().req.ttl->count(), 5000);
        }

        TEST(ProtocolTest, EncodeDecodeGetVersionedRequest) {
            Request req;
            req.opcode = Opcode::GetVersioned;
            req.key = "my-key";

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded.value().req.opcode, Opcode::GetVersioned);
            EXPECT_EQ(decoded.value().req.key, "my-key");
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

        TEST(ProtocolTest, EncodeDecodeTombstoneFlags) {
            Request req;
            req.opcode = Opcode::Replicate;
            req.key = "k";
            req.version = 9;
            req.writer_node_hash = 3;
            req.tombstone = true;
            req.expires_at = system_clock::time_point(milliseconds(123'456));

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded.value().req.opcode, Opcode::Replicate);
            EXPECT_EQ(decoded.value().req.key, "k");
            EXPECT_TRUE(decoded.value().req.tombstone);
            EXPECT_EQ(decoded.value().req.version, 9);
            ASSERT_TRUE(decoded.value().req.expires_at.has_value());

            // Tombstone votes ride GetVersioned responses without a value.
            Response res;
            res.status = Errc::OK;
            res.value = std::nullopt;
            res.version = 9;
            res.writer_node_hash = 3;
            res.tombstone = true;

            auto encoded_res = encode(res);
            ASSERT_TRUE(encoded_res.has_value());

            auto decoded_res = decodeResponse(encoded_res.value());
            ASSERT_TRUE(decoded_res.has_value());
            EXPECT_EQ(decoded_res.value().status, Errc::OK);
            EXPECT_TRUE(decoded_res.value().tombstone);
            EXPECT_EQ(decoded_res.value().version, 9);
            EXPECT_FALSE(decoded_res.value().value.has_value());
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

        TEST(ProtocolTest, RequestDecoderIgnoresBytesPastDeclaredFrame) {
            Request request;
            request.opcode = Opcode::Get;
            request.key = "k";
            auto encoded = encode(request);
            ASSERT_TRUE(encoded.has_value());

            // TCP passes one complete frame, but the public span decoder is
            // deliberately frame-oriented too: a larger caller buffer must
            // not make trailing bytes part of this message.
            encoded->push_back(std::byte{0xAA});
            encoded->push_back(std::byte{0xBB});
            auto decoded = decode(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->req.key, "k");
        }

        TEST(ProtocolTest, ResponseDecoderIgnoresBytesPastDeclaredFrame) {
            Response response;
            response.status = Errc::OK;
            response.value = "v";
            auto encoded = encode(response);
            ASSERT_TRUE(encoded.has_value());

            encoded->push_back(std::byte{0xAA});
            auto decoded = decodeResponse(*encoded);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_TRUE(decoded->value.has_value());
            EXPECT_EQ(*decoded->value, "v");
        }

        TEST(ProtocolTest, ResponseDecoderRejectsOversizedDeclaredPayload) {
            std::vector<std::byte> frame(K_FRAME_HEADER_SIZE);
            frame[0] = std::byte{K_MAGIC};
            frame[1] = std::byte{K_VERSION};
            frame[2] = std::byte{0};
            // Network-order 0xFFFFFFFF in payload_len.
            frame[3] = std::byte{0xFF};
            frame[4] = std::byte{0xFF};
            frame[5] = std::byte{0xFF};
            frame[6] = std::byte{0xFF};

            auto decoded = decodeResponse(frame);
            ASSERT_FALSE(decoded.has_value());
            EXPECT_EQ(decoded.error().code(), Errc::InvalidArgument);
        }

        TEST(ProtocolTest, EncodeEmptyKey) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = {};
            req.value = "value";

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded.value().req.key, "");
            EXPECT_EQ(decoded.value().req.value, "value");
        }

        TEST(ProtocolTest, EncodeBothTtlAndExpiresAt) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = "k";
            req.value = "v";
            req.ttl = milliseconds(5000);
            req.expires_at = system_clock::time_point(milliseconds(1'700'000'000'000));

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());

            auto decoded = decode(encoded.value());
            ASSERT_TRUE(decoded.has_value());
            // Both should be present in decoded request
            EXPECT_TRUE(decoded.value().req.ttl.has_value());
            EXPECT_TRUE(decoded.value().req.expires_at.has_value());
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
                encoded.value().begin(),
                encoded.value().begin() + K_FRAME_HEADER_SIZE + 1
            );
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
            frame[8] = std::byte{0x00}; // but no room for the 4-byte ttl

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

        TEST(ProtocolTest, HeaderOnlyFrameExhaustsProvableReads) {
            // Pins the expectRead invariant: the 4 fixed header fields
            // (magic, version, opcode, payload_len) are covered by the 7-byte
            // header guard, so they must never fail. A frame that is exactly the
            // header must therefore fail on the FIRST payload-dependent read
            // (flags), not while skipping the header.
            std::vector<std::byte> frame(K_FRAME_HEADER_SIZE);
            frame[0] = std::byte{K_MAGIC};
            frame[1] = std::byte{K_VERSION};
            frame[2] = std::byte{static_cast<uint8_t>(Opcode::Set)};
            uint32_t payload_len = 0; // no payload at all
            std::memcpy(&frame[3], &payload_len, sizeof(payload_len));

            auto result = decode(frame);
            ASSERT_FALSE(result.has_value());
            // The failure must come from the payload-dependent flags read, whose
            // message is "truncated" — proving all four header reads succeeded
            // and none of the header rejections fired.
            EXPECT_EQ(result.error().message(), "truncated");
        }

        TEST(ProtocolTest, ResponseHeaderOnlyFrameReachesStatusRead) {
            // Same invariant for the response decoder: a header-only frame must
            // get past the skipped header and fail on the payload's first byte
            // (status), proving the header skips cannot fail.
            std::vector<std::byte> frame(K_FRAME_HEADER_SIZE);
            frame[0] = std::byte{K_MAGIC};
            frame[1] = std::byte{K_VERSION};
            frame[2] = std::byte{0};
            uint32_t payload_len = 0;
            std::memcpy(&frame[3], &payload_len, sizeof(payload_len));

            auto result = decodeResponse(frame);
            ASSERT_FALSE(result.has_value());
            // Fails on the status byte read, not any header-level rejection.
            EXPECT_EQ(result.error().message(), "truncated");
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

        TEST(ProtocolTest, EncodeDecodeRequestWithTrace) {
            Request req;
            req.opcode = Opcode::Set;
            req.key = "key";
            req.value = "val";
            req.trace_id = 0xDEADBEEF;
            req.span_id = 0x12345678;

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decode(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->req.trace_id, 0xDEADBEEF);
            EXPECT_EQ(decoded->req.span_id, 0x12345678);
        }

        TEST(ProtocolTest, EncodeDecodeResponseWithTrace) {
            Response res;
            res.status = Errc::OK;
            res.value = "hello";
            res.trace_id = 0xAABBCCDD;
            res.span_id = 0x55667788;

            auto encoded = encode(res);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decodeResponse(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->trace_id, 0xAABBCCDD);
            EXPECT_EQ(decoded->span_id, 0x55667788);
        }

        TEST(ProtocolTest, RequestWithoutTraceHasZeroIds) {
            Request req;
            req.opcode = Opcode::Get;
            req.key = "key";

            auto encoded = encode(req);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decode(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->req.trace_id, 0);
            EXPECT_EQ(decoded->req.span_id, 0);
        }

        TEST(ProtocolTest, ResponseWithoutTraceHasZeroIds) {
            Response res;
            res.status = Errc::OK;

            auto encoded = encode(res);
            ASSERT_TRUE(encoded.has_value());
            auto decoded = decodeResponse(*encoded);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->trace_id, 0);
            EXPECT_EQ(decoded->span_id, 0);
        }
    } // namespace
} // namespace cinder::net
