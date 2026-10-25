#include <gtest/gtest.h>

#include "cinder/client/cache_client.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {
    namespace {

        TEST(CacheClientTest, RoutesSingleNode) {
            ClusterConfig config;
            config.nodes.push_back({"node1", "127.0.0.1", 7000});

            CacheClient client(config);
            // Can't test actual network in unit test, just construction
            SUCCEED();
        }

        TEST(CacheClientTest, RoutePrimaryIsConsistent) {
            ConsistentHashRing ring(150);
            ring.addNode("a");
            ring.addNode("b");

            std::string key = "test-key";
            auto first = ring.getNode(key);
            for (int i = 0; i < 100; i++) {
                EXPECT_EQ(ring.getNode(key), first);
            }
        }

        TEST(CacheClientTest, RoutePrimaryDistributes) {
            ConsistentHashRing ring(150);
            ring.addNode("a");
            ring.addNode("b");

            std::set<std::string> nodes;
            for (int i = 0; i < 1000; i++) {
                nodes.insert(ring.getNode(std::to_string(i)));
            }
            EXPECT_GE(nodes.size(), 2);
        }

        TEST(CacheClientTest, ParseRedirect) {
            EXPECT_EQ(parseRedirect("moved to node2"), std::optional<NodeId>("node2"));
            EXPECT_EQ(parseRedirect("moved to  node2 "), std::optional<NodeId>(" node2 "));
            EXPECT_EQ(
                parseRedirect("moved to node3@127.0.0.1:17992"),
                std::optional<NodeId>("node3")
            );
            EXPECT_FALSE(parseRedirect("OK").has_value());
            EXPECT_FALSE(parseRedirect("").has_value());
            EXPECT_FALSE(parseRedirect("moved to").has_value());
        }

        TEST(CacheClientTest, ParseRedirectTarget) {
            auto legacy = parseRedirectTarget("moved to node2");
            ASSERT_TRUE(legacy.has_value());
            EXPECT_EQ(legacy->id, "node2");
            EXPECT_FALSE(legacy->hasAddress());

            auto full = parseRedirectTarget("moved to node3@127.0.0.1:17992");
            ASSERT_TRUE(full.has_value());
            EXPECT_EQ(full->id, "node3");
            EXPECT_TRUE(full->hasAddress());
            EXPECT_EQ(full->host, "127.0.0.1");
            EXPECT_EQ(full->port, 17'992);

            // Unparsable address degrades to id-only; non-redirects stay nullopt.
            auto bad_port = parseRedirectTarget("moved to node3@127.0.0.1:notaport");
            ASSERT_TRUE(bad_port.has_value());
            EXPECT_EQ(bad_port->id, "node3");
            EXPECT_FALSE(bad_port->hasAddress());

            auto no_colon = parseRedirectTarget("moved to node3@nodomain");
            ASSERT_TRUE(no_colon.has_value());
            EXPECT_EQ(no_colon->id, "node3");
            EXPECT_FALSE(no_colon->hasAddress());

            EXPECT_FALSE(parseRedirectTarget("OK").has_value());
            EXPECT_FALSE(parseRedirectTarget("").has_value());
            EXPECT_FALSE(parseRedirectTarget("moved to").has_value());
        }

        TEST(RetryBackoffTest, ExponentialSchedule) {
            EXPECT_EQ(retryBackoff(0, 25), 25);
            EXPECT_EQ(retryBackoff(1, 25), 50);
            EXPECT_EQ(retryBackoff(2, 25), 100);
            EXPECT_EQ(retryBackoff(3, 25), 200);
        }

        TEST(RetryBackoffTest, SaturationAtIntMax) {
            EXPECT_EQ(retryBackoff(30, 25), std::numeric_limits<int>::max());
            EXPECT_EQ(retryBackoff(100, 1), std::numeric_limits<int>::max());
        }

        TEST(RetryBackoffTest, ZeroBase) {
            EXPECT_EQ(retryBackoff(5, 0), 0);
        }

        TEST(JitterBackoffTest, ZeroJitterReturnsExactDelay) {
            EXPECT_EQ(jitterBackoff(25, 0.0), 25);
            EXPECT_EQ(jitterBackoff(100, 0.0), 100);
        }

        TEST(JitterBackoffTest, ZeroDelayReturnsZero) {
            EXPECT_EQ(jitterBackoff(0, 0.2), 0);
        }

        TEST(JitterBackoffTest, JitterWithinBounds) {
            for (int i = 0; i < 1000; ++i) {
                int d = jitterBackoff(100, 0.2);
                EXPECT_GE(d, 80);
                EXPECT_LE(d, 120);
            }
        }

        TEST(RetryableTest, TimeoutErrorIsRetryable) {
            auto res = err<net::Response>(Error(Errc::Timeout, "timed out"));
            EXPECT_TRUE(retryable(res));
        }

        TEST(RetryableTest, NotReadyErrorIsRetryable) {
            auto res = err<net::Response>(Error(Errc::NotReady, "not ready"));
            EXPECT_TRUE(retryable(res));
        }

        TEST(RetryableTest, NotFoundIsNotRetryable) {
            auto res = err<net::Response>(Error(Errc::NotFound));
            EXPECT_FALSE(retryable(res));
        }

        TEST(RetryableTest, CapacityExceededIsNotRetryable) {
            auto res = err<net::Response>(Error(Errc::CapacityExceeded));
            EXPECT_FALSE(retryable(res));
        }

        TEST(RetryableTest, OkResponseIsNotRetryable) {
            net::Response resp;
            resp.status = Errc::OK;
            auto res = ok<net::Response>(std::move(resp));
            EXPECT_FALSE(retryable(res));
        }

        TEST(RetryableTest, TimeoutResponseStatusIsRetryable) {
            net::Response resp;
            resp.status = Errc::Timeout;
            auto res = ok<net::Response>(std::move(resp));
            EXPECT_TRUE(retryable(res));
        }

        TEST(RetryableTest, NotReadyResponseStatusIsRetryable) {
            net::Response resp;
            resp.status = Errc::NotReady;
            auto res = ok<net::Response>(std::move(resp));
            EXPECT_TRUE(retryable(res));
        }

        TEST(RetryableBatchTest, TransportErrorIsRetryable) {
            auto res = err<std::vector<net::Response>>(Error(Errc::Timeout));
            EXPECT_TRUE(retryable(res));
        }

        TEST(RetryableBatchTest, SuccessfulBatchIsNotRetryable) {
            std::vector<net::Response> batch;
            batch.push_back(net::Response{.status = Errc::OK});
            auto res = ok<std::vector<net::Response>>(std::move(batch));
            EXPECT_FALSE(retryable(res));
        }

        // asRetryable() is what the wire paths use to turn a decode desync into a
        // retryable error WITHOUT losing the original. The old hand-rolled form,
        // `Error(Errc::NotReady, e.message())`, produced a retryable error but
        // discarded the code and source_location of the real failure — so a
        // recurring desync could never be traced back to decodeResponse.
        TEST(AsRetryableTest, RemapsCodeButKeepsOriginalAsCause) {
            Error decode_failure(Errc::InvalidArgument, "bad magic");
            Error mapped = asRetryable(decode_failure, "response decode failed");

            // Caller-visible: retryable.
            EXPECT_EQ(mapped.code(), Errc::NotReady);
            EXPECT_EQ(mapped.message(), "response decode failed");

            // Provenance: the real failure survives, code and message intact.
            ASSERT_NE(mapped.cause(), nullptr);
            EXPECT_EQ(mapped.cause()->code(), Errc::InvalidArgument);
            EXPECT_EQ(mapped.cause()->message(), "bad magic");
        }

        TEST(AsRetryableTest, ResultOfAsRetryableIsRetryable) {
            // The whole point of the remap: retryable() must say yes.
            auto res =
                err<net::Response>(asRetryable(Error(Errc::InvalidArgument, "truncated"), "ctx"));
            EXPECT_TRUE(retryable(res));
            ASSERT_FALSE(res.has_value());
            EXPECT_EQ(res.error().code(), Errc::NotReady);
            ASSERT_NE(res.error().cause(), nullptr);
            EXPECT_EQ(res.error().cause()->code(), Errc::InvalidArgument);
        }

        TEST(AsRetryableTest, PreservesOriginalSourceLocation) {
            // The old form invented a location at the remap site, erasing where
            // the failure actually originated.
            Error decode_failure(Errc::InvalidArgument, "bad magic");
            const auto original_line = decode_failure.location().line();
            Error mapped = asRetryable(decode_failure, "remapped");

            ASSERT_NE(mapped.cause(), nullptr);
            EXPECT_EQ(mapped.cause()->location().line(), original_line);
            EXPECT_EQ(
                mapped.cause()->location().file_name(),
                decode_failure.location().file_name()
            );
        }

        // Exhaustive classification of every Errc against retryable(). The
        // predicate is a two-value allow-list (Timeout, NotReady), so the real
        // risk is an *under*-retried code: a transient condition mapped to a
        // "permanent" value fails the request outright with no second attempt.
        // That is exactly the bug class behind the redirect-test flake, where a
        // reused-socket framing desync surfaced as InvalidArgument.
        //
        // This table is the contract: every Errc member appears exactly once,
        // and each entry documents why it is classified that way. Adding a new
        // Errc member without adding a row here should be caught in review.
        TEST(RetryableTest, ErrcClassificationTable) {
            struct Case {
                Errc code;
                bool retryable_expected;
                const char* why;
            };

            const Case cases[] = {
                // Transient: worth another attempt.
                {Errc::Timeout, true, "deadline expired -- peer may still be alive"},
                {Errc::NotReady, true, "node unavailable/moved/unknown -- re-resolve and retry"},

                // Permanent: retrying the same request against the same target
                // cannot change the outcome (a redirect/del/refresh path handles
                // recovery instead, where applicable).
                {Errc::OK, false, "success -- nothing to retry"},
                {Errc::NotFound, false, "key genuinely absent"},
                {Errc::CapacityExceeded, false, "value exceeds store capacity"},
                {Errc::InvalidArgument, false, "request itself is malformed"},
                {Errc::TtlExpired, false, "entry expired"},
                {Errc::NotSupported, false, "opcode/feature unsupported by this peer"},
                {Errc::InternalError, false, "local fault (disk/fs) -- not wire-transient"},
                {Errc::PermissionDenied, false, "auth rejected -- retry cannot fix credentials"},
                {Errc::CorruptData, false, "durable state corrupt -- needs operator action"},
            };

            for (const auto& c : cases) {
                auto res = err<net::Response>(Error(c.code));
                EXPECT_EQ(retryable(res), c.retryable_expected)
                    << "Errc::"
                    << static_cast<int>(c.code)
                    << " -- "
                    << c.why;
            }
        }
    } // namespace
} // namespace cinder
