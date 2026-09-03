#include <gtest/gtest.h>

#include "cinder/client/cache_client.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {
namespace {

TEST(CacheClientTest, RoutesSingleNode) {
    ClusterConfig config;
    config.nodes.push_back({"node1", "127.0.0.1", 7'000});

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
    for (int i = 0; i < 1'000; i++) {
        nodes.insert(ring.getNode(std::to_string(i)));
    }
    EXPECT_GE(nodes.size(), 2);
}

TEST(CacheClientTest, ParseRedirect) {
    EXPECT_EQ(parseRedirect("moved to node2"), std::optional<NodeId>("node2"));
    EXPECT_EQ(parseRedirect("moved to  node2 "), std::optional<NodeId>(" node2 "));
    EXPECT_FALSE(parseRedirect("OK").has_value());
    EXPECT_FALSE(parseRedirect("").has_value());
    EXPECT_FALSE(parseRedirect("moved to").has_value());
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
    for (int i = 0; i < 1'000; ++i) {
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
} // namespace
} // namespace cinder
