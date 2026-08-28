#include <gtest/gtest.h>
#include <thread>
#include <vector>

#include "cinder/store/lfu_store.hpp"

namespace cinder {
namespace {

TEST(LfuConcurrentTest, ConcurrentGetPutRemoveStress) {
    // Small capacity forces constant eviction churn so the exclusive-lock
    // paths in get()/getVersioned() contend heavily. Run under the tsan
    // preset: this test exists to catch lock-downgrade regressions.
    LfuStore store(4'096);
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

TEST(LfuConcurrentTest, ConcurrentFrequencyTracking) {
    // Multiple threads incrementing the same key's frequency.
    // Verifies that LFU frequency buckets don't corrupt under contention.
    LfuStore store(1'048'576);
    EXPECT_TRUE(store.put("shared", "value").has_value());

    constexpr int K_THREADS = 4;
    constexpr int K_ITERS = 1'000;

    std::vector<std::jthread> threads;
    for (int t = 0; t < K_THREADS; ++t) {
        threads.emplace_back([&store] {
            for (int i = 0; i < K_ITERS; ++i) {
                (void)store.get("shared");
            }
        });
    }

    // After all threads finish, the key should still be accessible
    auto result = store.get("shared");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(*result, "value");
}
} // namespace
} // namespace cinder
