#include <chrono>
#include <gtest/gtest.h>
#include <thread>

#include "cinder/store/lfu_store.hpp"

using std::chrono::milliseconds;

namespace cinder {
namespace {

TEST(LfuStoreTest, PutAndGet) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("key", "value").has_value());
    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST(LfuStoreTest, GetMissing) {
    LfuStore store(1'024);
    auto result = store.get("missing");
    EXPECT_FALSE(result.has_value());
}

TEST(LfuStoreTest, Overwrite) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("key", "value1").has_value());
    EXPECT_TRUE(store.put("key", "value2").has_value());
    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value2");
}

TEST(LfuStoreTest, Remove) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("key", "value").has_value());
    EXPECT_TRUE(store.remove("key"));
    EXPECT_FALSE(store.get("key").has_value());
    EXPECT_FALSE(store.remove("missing"));
}

TEST(LfuStoreTest, Size) {
    LfuStore store(1'024);
    EXPECT_EQ(store.size(), 0);
    EXPECT_TRUE(store.put("a", "1").has_value());
    EXPECT_EQ(store.size(), 1);
    EXPECT_TRUE(store.put("b", "2").has_value());
    EXPECT_EQ(store.size(), 2);
    store.remove("a");
    EXPECT_EQ(store.size(), 1);
}

TEST(LfuStoreTest, EvictionOnCapacity) {
    LfuStore store(100);
    for (int i = 0; i < 100; i++) {
        // NOLINTNEXTLINE(bugprone-unused-return-value, cert-err33-c)
        (void)store.put(std::to_string(i), std::string(50, 'x'));
    }
    EXPECT_LE(store.size(), 3);
}

TEST(LfuStoreTest, FrequencyOrder) {
    LfuStore store(10'000);
    auto val = std::string(3'000, 'x');
    EXPECT_TRUE(store.put("a", val).has_value());
    EXPECT_TRUE(store.put("b", val).has_value());
    EXPECT_TRUE(store.put("c", val).has_value());

    // Access 'a' 3 times, 'b' 2 times — 'c' stays at freq 1
    store.get("a");
    store.get("a");
    store.get("a");
    store.get("b");
    store.get("b");

    // Adding 'd' should evict 'c' (lowest freq = 1)
    EXPECT_TRUE(store.put("d", val).has_value());
    EXPECT_TRUE(store.get("a").has_value());
    EXPECT_TRUE(store.get("b").has_value());
    EXPECT_TRUE(store.get("d").has_value());
    EXPECT_FALSE(store.get("c").has_value());
}

TEST(LfuStoreTest, TtlExpiry) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("key", "value", milliseconds(10)).has_value());
    EXPECT_TRUE(store.get("key").has_value());
    std::this_thread::sleep_for(milliseconds(20));
    EXPECT_FALSE(store.get("key").has_value());
}

TEST(LfuStoreTest, ValueLargerThanCapacity) {
    LfuStore store(50);
    auto result = store.put("key", std::string(100, 'x'));
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), Errc::CapacityExceeded);
}

TEST(LfuStoreTest, EvictExpired) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("a", "1", milliseconds(10)).has_value());
    EXPECT_TRUE(store.put("b", "2").has_value());

    std::this_thread::sleep_for(milliseconds(20));
    EXPECT_EQ(store.evictExpired(), 1);
    EXPECT_EQ(store.size(), 1);
    EXPECT_FALSE(store.get("a").has_value());
    EXPECT_TRUE(store.get("b").has_value());
}

TEST(LfuStoreTest, EmptyKey) {
    LfuStore store(1'024);
    EXPECT_TRUE(store.put("", "value").has_value());
    auto result = store.get("");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST(LfuStoreTest, MaxSizeValue) {
    LfuStore store(200);
    EXPECT_TRUE(store.put("k", std::string(50, 'x')).has_value());
    EXPECT_EQ(store.size(), 1);
}

TEST(LfuStoreTest, FrequencyResetAfterEviction) {
    // sizeof(LfuNode) ≈ 96 bytes (std::string SSO overhead)
    // Each entry: key(1) + value(10) + sizeof(LfuNode) ≈ 107 bytes
    // Capacity 250 holds 2 entries; 3rd triggers eviction
    LfuStore store(250);
    EXPECT_TRUE(store.put("a", std::string(10, 'x')).has_value());
    EXPECT_TRUE(store.put("b", std::string(10, 'x')).has_value());
    // Access "a" many times to increase its frequency
    store.get("a");
    store.get("a");
    store.get("a");

    // "c" should evict "b" (freq=1), not "a" (freq=4)
    EXPECT_TRUE(store.put("c", std::string(10, 'x')).has_value());
    EXPECT_TRUE(store.get("a").has_value());
    EXPECT_FALSE(store.get("b").has_value());
    EXPECT_TRUE(store.get("c").has_value());
}
} // namespace
} // namespace cinder
