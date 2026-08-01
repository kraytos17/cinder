#include <chrono>
#include <gtest/gtest.h>
#include <thread>

#include "cinder/store/lru_store.hpp"

namespace cinder {
namespace {

TEST(LruStoreTest, PutAndGet) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("key", "value").has_value());
    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST(LruStoreTest, GetMissing) {
    LruStore store(1'024);
    auto result = store.get("missing");
    EXPECT_FALSE(result.has_value());
}

TEST(LruStoreTest, Overwrite) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("key", "value1").has_value());
    EXPECT_TRUE(store.put("key", "value2").has_value());
    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value2");
}

TEST(LruStoreTest, Remove) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("key", "value").has_value());
    EXPECT_TRUE(store.remove("key"));
    EXPECT_FALSE(store.get("key").has_value());
    EXPECT_FALSE(store.remove("missing"));
}

TEST(LruStoreTest, Size) {
    LruStore store(1'024);
    EXPECT_EQ(store.size(), 0);
    EXPECT_TRUE(store.put("a", "1").has_value());
    EXPECT_EQ(store.size(), 1);
    EXPECT_TRUE(store.put("b", "2").has_value());
    EXPECT_EQ(store.size(), 2);
    store.remove("a");
    EXPECT_EQ(store.size(), 1);
}

TEST(LruStoreTest, EvictionOnCapacity) {
    LruStore store(100);
    for (int i = 0; i < 100; i++) {
        // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
        (void)store.put(std::to_string(i), std::string(50, 'x'));
    }
    EXPECT_LE(store.size(), 3);
}

TEST(LruStoreTest, TtlExpiry) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("key", "value", std::chrono::milliseconds(10)).has_value());
    EXPECT_TRUE(store.get("key").has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(store.get("key").has_value());
}

TEST(LruStoreTest, ValueLargerThanCapacity) {
    LruStore store(50);
    auto result = store.put("key", std::string(100, 'x'));
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), Errc::CapacityExceeded);
}

TEST(LruStoreTest, EvictExpired) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("a", "1", std::chrono::milliseconds(10)).has_value());
    EXPECT_TRUE(store.put("b", "2").has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    EXPECT_EQ(store.evictExpired(), 1);
    EXPECT_EQ(store.size(), 1);
    EXPECT_FALSE(store.get("a").has_value());
    EXPECT_TRUE(store.get("b").has_value());
}

TEST(LruStoreTest, LruOrder) {
    LruStore store(3'500);
    EXPECT_TRUE(store.put("a", std::string(1'000, 'x')).has_value());
    EXPECT_TRUE(store.put("b", std::string(1'000, 'x')).has_value());
    EXPECT_TRUE(store.put("c", std::string(1'000, 'x')).has_value());
    store.get("a");
    // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
    (void)store.put("d", std::string(1'000, 'x'));

    EXPECT_TRUE(store.get("a").has_value());
    EXPECT_TRUE(store.get("d").has_value());
    EXPECT_FALSE(store.get("b").has_value());
}
} // namespace
} // namespace cinder
