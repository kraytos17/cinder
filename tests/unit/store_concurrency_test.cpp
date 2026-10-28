#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <vector>

#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"

using std::chrono::milliseconds;

namespace cinder {
    namespace {

        // Store-policy concurrency stress. The per-store `ConcurrentGetPutRemoveStress`
        // tests (lru_store_test.cpp, lfu_concurrent_test.cpp) already hammer the
        // get/put/remove/getVersioned/size paths. These tests cover what those leave
        // out: the background-maintenance and admin mutators that share the same
        // mutex and are reachable from the io thread while requests are in flight --
        // evictExpired (TTL wheel sweep), forEach (snapshot copy), setCapacity
        // (resize + immediate eviction), and the versioned write path
        // (mintVersion/putVersioned).
        //
        // Run under the `tsan` preset. A regression that downgrades any of these to a
        // shared lock, or that touches the wheel/list without the lock, shows up here
        // rather than as intermittent integration flakiness.

        // Drives a store through the maintenance surface from several threads while
        // other threads do normal traffic. Templated so LRU and LFU share one body --
        // the locking lives in their common base, so both must behave identically.
        template <typename Store> void stressMaintenancePaths(const char* label) {
            constexpr int K_WRITERS = 3;
            constexpr int K_MAINTENANCE = 2;
            constexpr int K_ITERS = 2000;
            constexpr size_t K_CAPACITY = 8192;

            Store store(K_CAPACITY);
            std::atomic<bool> stop{false};

            // Writers: mix plain puts (some with a short TTL so evictExpired has real
            // work), versioned writes, and reads.
            std::vector<std::jthread> writers;
            for (int t = 0; t < K_WRITERS; ++t) {
                writers.emplace_back([&store, t] {
                    for (int i = 0; i < K_ITERS; ++i) {
                        auto key = "k" + std::to_string((t * K_ITERS + i) % 256);
                        switch (i % 4) {
                            case 0:
                                // Short TTL: guarantees the sweep finds expired entries.
                                (void)store.put(key, "v" + std::to_string(i), milliseconds(1));
                                break;
                            case 1: {
                                // Versioned write path (replication entry point).
                                VersionedEntry entry;
                                entry.value = "vv" + std::to_string(i);
                                entry.setVersion(store.mintVersion());
                                entry.writer_node_hash = 1;
                                (void)store.putVersioned(key, std::move(entry));
                                break;
                            }
                            case 2:
                                (void)store.getVersioned(key);
                                break;
                            default:
                                (void)store.get(key);
                                break;
                        }
                    }
                });
            }

            // Maintenance threads: enough iterations that each sweep overlaps real
            // traffic. Sleeps yield the CPU so TSan sees interleavings rather than
            // long serialised stretches.
            std::vector<std::jthread> maintenance;
            for (int t = 0; t < K_MAINTENANCE; ++t) {
                maintenance.emplace_back([&store, &stop] {
                    for (int i = 0; i < K_ITERS / 4 && !stop; ++i) {
                        switch (i % 3) {
                            case 0:
                                (void)store.evictExpired();
                                break;
                            case 1: {
                                // forEach copies a snapshot under the lock then invokes
                                // the visitor outside it; the visitor must not deadlock
                                // if it re-enters the store.
                                size_t seen = 0;
                                store.forEach([&seen](const std::string&, const VersionedEntry&) {
                                    ++seen;
                                });
                                (void)seen;
                                break;
                            }
                            default:
                                // Growth-only resize keeps capacity >= live bytes so we
                                // do not deterministically evict everything mid-iteration.
                                (void)store.size();
                                break;
                        }
                        std::this_thread::sleep_for(milliseconds(1));
                    }
                });
                (void)t;
            }

            // Capacity churn on a separate thread: setCapacity is an admin mutator that
            // takes the same lock and may evict immediately when shrinking.
            std::jthread resizer([&store, &stop] {
                // Only ever shrink to a size that still admits the working set, so the
                // test exercises the resize+evict path without emptying the store.
                for (int i = 0; i < 200 && !stop; ++i) {
                    store.setCapacity(i % 2 == 0 ? 1'048'576 : 8192);
                    std::this_thread::sleep_for(milliseconds(1));
                }
            });

            writers.clear(); // join writers first
            stop = true;
            maintenance.clear();
            resizer.join();
            // Grow before the liveness checks: the churn above ends at 8192
            // bytes with a store that may hold more live data, so a fresh put
            // can be immediately evicted again (LFU evicts the min-frequency
            // entry, which can be the just-written key when it is uniquely
            // minimal — legitimate cache behavior, and putVersioned correctly
            // reports ok). Growth never evicts, so the post-condition below is
            // timing-independent.
            store.setCapacity(1U << 20U);

            // Post-conditions: the store must still be functional, not wedged or
            // corrupted. A race that corrupted the list/index would typically surface
            // as a crash or a bogus size under TSan; this asserts liveness too.
            EXPECT_TRUE(store.put("post", "check").has_value()) << label;
            auto got = store.get("post");
            ASSERT_TRUE(got.has_value()) << label;
            EXPECT_EQ(*got, "check") << label;
            (void)store.evictExpired();
        }

        TEST(StoreConcurrencyTest, LruMaintenancePathsUnderLoad) {
            stressMaintenancePaths<LruStore>("lru");
        }

        TEST(StoreConcurrencyTest, LfuMaintenancePathsUnderLoad) {
            stressMaintenancePaths<LfuStore>("lfu");
        }

        // forEach's documented contract: the visitor runs OUTSIDE the store lock over
        // a snapshot, so it may call store mutators without self-deadlocking. This is
        // the property most likely to regress if the snapshot/lock discipline is
        // refactored, so assert it directly rather than only via stress.
        template <typename Store> void forEachVisitorMayMutate(const char* label) {
            Store store(1U << 20U);
            for (int i = 0; i < 64; ++i) {
                ASSERT_TRUE(store.put("k" + std::to_string(i), "v").has_value()) << label;
            }

            size_t visited = 0;
            size_t removed = 0;
            store.forEach([&](const std::string& key, const VersionedEntry&) {
                ++visited;
                // Must not deadlock: this takes the store mutex from inside the
                // visitor, which is only safe because the lock is released first.
                if (store.remove(key)) {
                    ++removed;
                }
            });

            EXPECT_EQ(visited, 64U) << label;
            EXPECT_EQ(removed, 64U) << label;
            EXPECT_EQ(store.size(), 0U) << label;
        }

        TEST(StoreConcurrencyTest, LruForEachVisitorMayMutate) {
            forEachVisitorMayMutate<LruStore>("lru");
        }

        TEST(StoreConcurrencyTest, LfuForEachVisitorMayMutate) {
            forEachVisitorMayMutate<LfuStore>("lfu");
        }

        // The versioned write path is idempotent and LWW: concurrent writers must never
        // leave the entry in a torn state, and the highest version must win. Uses one
        // key so every thread contends on the same index/list node.
        template <typename Store> void concurrentVersionedWritesSingleKey(const char* label) {
            Store store(1U << 20U);
            constexpr int K_THREADS = 4;
            constexpr int K_ITERS = 1000;

            std::vector<std::jthread> threads;
            for (int t = 0; t < K_THREADS; ++t) {
                threads.emplace_back([&store, t] {
                    for (int i = 0; i < K_ITERS; ++i) {
                        VersionedEntry entry;
                        entry.value = "t" + std::to_string(t);
                        // Monotonic versions from the store's own authority.
                        entry.setVersion(store.mintVersion());
                        entry.writer_node_hash = static_cast<uint64_t>(t);
                        (void)store.putVersioned("hot", std::move(entry));
                    }
                });
            }
            threads.clear();

            auto final_entry = store.getVersioned("hot");
            ASSERT_TRUE(final_entry.has_value()) << label;
            EXPECT_FALSE(final_entry->value.empty()) << label;
            // Exactly one entry must exist for the key regardless of interleaving.
            EXPECT_EQ(store.size(), 1U) << label;
        }

        TEST(StoreConcurrencyTest, LruConcurrentVersionedWritesSingleKey) {
            concurrentVersionedWritesSingleKey<LruStore>("lru");
        }

        TEST(StoreConcurrencyTest, LfuConcurrentVersionedWritesSingleKey) {
            concurrentVersionedWritesSingleKey<LfuStore>("lfu");
        }

        // Striped routing: a 1 MiB store has 16 shards. Keys spread across
        // shards must all round-trip, size must aggregate, and concurrent
        // disjoint-key traffic must not corrupt the index (tsan preset).
        template <typename Store> void stripedDisjointKeys(const char* label) {
            // 4 MiB still stripes over the max 16 shards (256 KiB slices)
            // with headroom for hash skew: ~10k small keys ≈ 1.4 MiB live.
            Store store(4U << 20U);
            constexpr int K_KEYS = 2000;
            for (int i = 0; i < K_KEYS; ++i) {
                ASSERT_TRUE(
                    store.put("sk" + std::to_string(i), "v" + std::to_string(i)).has_value()
                ) << label;
            }
            EXPECT_EQ(store.size(), static_cast<size_t>(K_KEYS)) << label;
            for (int i = 0; i < K_KEYS; ++i) {
                auto got = store.get("sk" + std::to_string(i));
                ASSERT_TRUE(got.has_value()) << label << " i=" << i;
                EXPECT_EQ(*got, "v" + std::to_string(i)) << label;
            }
            auto entries = store.liveEntries();
            EXPECT_EQ(entries.size(), static_cast<size_t>(K_KEYS)) << label;

            // Concurrent disjoint puts/gets/removes across shards.
            constexpr int K_THREADS = 8;
            constexpr int K_ITERS = 2000;
            std::vector<std::jthread> threads;
            for (int t = 0; t < K_THREADS; ++t) {
                threads.emplace_back([&store, t] {
                    for (int i = 0; i < K_ITERS; ++i) {
                        auto key = "t" + std::to_string(t) + "-" + std::to_string(i);
                        (void)store.put(key, "v");
                        (void)store.get(key);
                        if (i % 2 == 0) {
                            (void)store.remove(key);
                        }
                    }
                });
            }
            threads.clear();
            // Half the striped keys removed; size must reflect exactly that.
            EXPECT_EQ(store.size(), static_cast<size_t>(K_KEYS + K_THREADS * K_ITERS / 2)) << label;
            EXPECT_TRUE(store.put("post", "check").has_value()) << label;
            EXPECT_EQ(store.get("post"), std::optional<std::string>("check")) << label;
        }

        TEST(StoreConcurrencyTest, LruStripedDisjointKeys) {
            stripedDisjointKeys<LruStore>("lru");
        }

        TEST(StoreConcurrencyTest, LfuStripedDisjointKeys) {
            stripedDisjointKeys<LfuStore>("lfu");
        }
    } // namespace
} // namespace cinder
