#include <chrono>
#include <gtest/gtest.h>
#include <thread>

#include "cinder/store/lru_store.hpp"

using std::chrono::milliseconds;

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
    EXPECT_TRUE(store.put("key", "value", milliseconds(10)).has_value());
    EXPECT_TRUE(store.get("key").has_value());
    std::this_thread::sleep_for(milliseconds(20));
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
    EXPECT_TRUE(store.put("a", "1", milliseconds(10)).has_value());
    EXPECT_TRUE(store.put("b", "2").has_value());
    std::this_thread::sleep_for(milliseconds(20));

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

TEST(LruStoreTest, ConcurrentGetPutRemoveStress) {
    // Small capacity forces constant eviction churn so the exclusive-lock
    // paths in get()/getVersioned() contend heavily. Run under the tsan
    // preset: this test exists to catch lock-downgrade regressions.
    LruStore store(4'096);
    constexpr int K_THREADS = 4;
    constexpr int K_ITERS = 5'000;

    std::vector<std::jthread> threads;
    for (int t = 0; t < K_THREADS; ++t) {
        threads.emplace_back([&store, t] {
            for (int i = 0; i < K_ITERS; ++i) {
                auto key = "k" + std::to_string((t * K_ITERS + i) % 512);
                switch (i % 4) {
                    case 0:
                        EXPECT_TRUE(store.put(key, "v" + std::to_string(i)).has_value());
                        break;
                    case 1:
                        (void)store.getVersioned(key);
                        break;
                    case 2:
                        (void)store.remove(key);
                        break;
                    default:
                        (void)store.get(key);
                        break;
                }
                EXPECT_LE(store.size(), 512U + K_THREADS);
            }
        });
    }
}

TEST(LruStoreTest, EmptyKey) {
    LruStore store(1'024);
    EXPECT_TRUE(store.put("", "value").has_value());
    auto result = store.get("");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}

TEST(LruStoreTest, MaxSizeValue) {
    LruStore store(200);
    // Key (1 byte) + value (50 bytes) + Node overhead should fit in 200 bytes
    EXPECT_TRUE(store.put("k", std::string(50, 'x')).has_value());
    EXPECT_EQ(store.size(), 1);
}

TEST(LruStoreTest, MultipleOverwrites) {
    LruStore store(1'024);
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(store.put("key", "value" + std::to_string(i)).has_value());
    }

    auto result = store.get("key");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value99");
}
} // namespace
} // namespace cinder
