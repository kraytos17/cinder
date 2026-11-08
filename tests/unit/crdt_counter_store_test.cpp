#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/store/crdt_counter_store.hpp"
#include "gtest/gtest.h"

namespace cinder {
    namespace {

        // 1 MiB test budget (unsigned shift: bugprone-signed-bitwise).
        inline constexpr size_t K_TEST_CAPACITY = 1U << 20U;

        auto mustAdjust(
            CrdtCounterStore& store,
            const std::string& key,
            const NodeId& actor,
            int64_t delta
        ) -> void {
            Result<void> res = delta >= 0
                ? store.increment(key, actor, static_cast<uint64_t>(delta))
                : store.decrement(key, actor, static_cast<uint64_t>(-delta));
            ASSERT_TRUE(res.has_value()) << res.error().message();
        }

        TEST(CrdtCounterStoreTest, IncrementDecrementRead) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            EXPECT_EQ(store.size(), 0U);
            EXPECT_FALSE(store.value("k").has_value());
            EXPECT_FALSE(store.get("k").has_value());

            mustAdjust(store, "k", "n1", 10);
            mustAdjust(store, "k", "n1", -4);
            mustAdjust(store, "k", "n2", -7);
            EXPECT_EQ(store.size(), 1U);
            ASSERT_TRUE(store.value("k").has_value());
            EXPECT_EQ(store.value("k").value(), -1);
        }

        TEST(CrdtCounterStoreTest, EmptyActorAndZeroDelta) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            EXPECT_FALSE(store.increment("k", "", 1).has_value());
            EXPECT_FALSE(store.decrement("k", "", 1).has_value());
            EXPECT_EQ(store.size(), 0U);
            // Zero deltas are no-ops and create nothing.
            ASSERT_TRUE(store.increment("k", "n1", 0).has_value());
            ASSERT_TRUE(store.decrement("k", "n1", 0).has_value());
            EXPECT_EQ(store.size(), 0U);
        }

        TEST(CrdtCounterStoreTest, TwoStoresConvergeViaMerge) {
            // Replicas diverge concurrently, then exchange full state both
            // ways: both end up identical with no update lost.
            CrdtCounterStore a(K_TEST_CAPACITY);
            CrdtCounterStore b(K_TEST_CAPACITY);
            mustAdjust(a, "k", "n1", 10);
            mustAdjust(a, "k", "n2", -3);
            mustAdjust(b, "k", "n2", 5);
            mustAdjust(b, "k", "n3", -1);

            auto sa = a.get("k");
            auto sb = b.get("k");
            ASSERT_TRUE(sa.has_value());
            ASSERT_TRUE(sb.has_value());
            ASSERT_TRUE(a.merge("k", *sb).has_value());
            ASSERT_TRUE(b.merge("k", *sa).has_value());
            EXPECT_EQ(a.value("k"), b.value("k"));
            // P: 10 + 5 = 15; N: 3 + 1 = 4.
            EXPECT_EQ(a.value("k").value(), 11);
        }

        TEST(CrdtCounterStoreTest, CapacityRejectsNewKey) {
            // Only room for a tiny entry: a big actor id does not fit.
            CrdtCounterStore store(32);
            auto res = store.increment("k", std::string(64, 'a'), 1);
            EXPECT_FALSE(res.has_value());
            EXPECT_EQ(res.error().code(), Errc::CapacityExceeded);
            EXPECT_EQ(store.size(), 0U);
        }

        TEST(CrdtCounterStoreTest, CapacityRejectsNewActorKeepsOldValue) {
            // One small counter fits; a second actor would overflow the shard.
            CrdtCounterStore store(64);
            mustAdjust(store, "k", "n1", 5);
            const auto before = store.value("k");
            auto res = store.increment("k", std::string(64, 'b'), 1);
            EXPECT_FALSE(res.has_value());
            EXPECT_EQ(res.error().code(), Errc::CapacityExceeded);
            EXPECT_EQ(store.value("k"), before);
            // The existing actor can still adjust (no growth).
            mustAdjust(store, "k", "n1", 2);
            EXPECT_EQ(store.value("k").value(), 7);
        }

        TEST(CrdtCounterStoreTest, FailedMergeLeavesLocalUnchanged) {
            CrdtCounterStore store(64);
            mustAdjust(store, "k", "n1", 5);
            const auto before = store.get("k");
            ASSERT_TRUE(before.has_value());

            crdt::PNCounter remote;
            ASSERT_TRUE(remote.increment(std::string(64, 'r'), 100).has_value());
            auto res = store.merge("k", remote);
            EXPECT_FALSE(res.has_value());
            EXPECT_EQ(res.error().code(), Errc::CapacityExceeded);
            EXPECT_EQ(store.get("k"), before);
        }

        TEST(CrdtCounterStoreTest, MergeOfMissingKeyAdmitsWhenFits) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            crdt::PNCounter remote;
            ASSERT_TRUE(remote.increment("n9", 42).has_value());
            ASSERT_TRUE(store.merge("k", remote).has_value());
            EXPECT_EQ(store.value("k").value(), 42);
            // Merging empty state is a no-op, even for missing keys.
            crdt::PNCounter empty;
            ASSERT_TRUE(store.merge("absent", empty).has_value());
            EXPECT_FALSE(store.value("absent").has_value());
        }

        TEST(CrdtCounterStoreTest, RemoveIsLocalAndMergeResurrects) {
            // Documents the deferred-delete design: remove() erases locally,
            // but a peer that still holds the key re-adds it on merge.
            // Distributed delete needs tombstones (not in this slice).
            CrdtCounterStore a(K_TEST_CAPACITY);
            CrdtCounterStore b(K_TEST_CAPACITY);
            mustAdjust(a, "k", "n1", 5);
            mustAdjust(b, "k", "n1", 5);

            EXPECT_TRUE(a.remove("k"));
            EXPECT_FALSE(a.value("k").has_value());
            EXPECT_FALSE(a.remove("k"));

            auto sb = b.get("k");
            ASSERT_TRUE(sb.has_value());
            ASSERT_TRUE(a.merge("k", *sb).has_value());
            EXPECT_EQ(a.value("k").value(), 5);
        }

        TEST(CrdtCounterStoreTest, LoweredCapacityKeepsStateRejectsGrowth) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            mustAdjust(store, "k", "n1", 5);
            store.setCapacity(1); // far below current use
            // Existing state is untouched (never evicted)...
            EXPECT_EQ(store.value("k").value(), 5);
            // ...but any growth is rejected.
            EXPECT_FALSE(store.increment("k2", "n1", 1).has_value());
            EXPECT_FALSE(store.increment("k", "n2", 1).has_value());
            // Same-actor adjusts need no growth and still work.
            mustAdjust(store, "k", "n1", 1);
            // Removals free the shard again.
            EXPECT_TRUE(store.remove("k"));
            EXPECT_EQ(store.size(), 0U);
        }

        TEST(CrdtCounterStoreTest, LiveCountersSnapshotsAllKeys) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            mustAdjust(store, "a", "n1", 1);
            mustAdjust(store, "b", "n2", 2);
            auto snap = store.liveCounters();
            EXPECT_EQ(snap.size(), 2U);
            // Snapshot is a copy: mutating the store leaves it intact.
            mustAdjust(store, "a", "n1", 100);
            int64_t total = 0;
            for (const auto& [key, counter] : snap) {
                total += counter.value();
            }
            EXPECT_EQ(total, 3);
        }

        TEST(CrdtCounterStoreTest, ConcurrentIncrementsConverge) {
            CrdtCounterStore store(K_TEST_CAPACITY);
            constexpr int K_THREADS = 4;
            constexpr int K_ITERS = 200;
            {
                // jthread joins on destruction: the close brace is the
                // barrier, no manual join loop to forget.
                std::vector<std::jthread> threads;
                for (int t = 0; t < K_THREADS; ++t) {
                    threads.emplace_back([&, t]() {
                        const NodeId actor = "n" + std::to_string(t);
                        for (int i = 0; i < K_ITERS; ++i) {
                            auto res = store.increment("shared", actor, 1);
                            ASSERT_TRUE(res.has_value()) << res.error().message();
                        }
                    });
                }
            }
            EXPECT_EQ(store.value("shared").value(), K_THREADS * K_ITERS);
        }
    } // namespace
} // namespace cinder
