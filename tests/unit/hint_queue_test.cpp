// Direct unit tests for HintQueue bounded-FIFO semantics.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <future>
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cinder/cluster/membership.hpp"
#include "cinder/cluster/transport.hpp"
#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"
#include "cinder/node/hint_journal.hpp"
#include "cinder/node/hint_queue.hpp"
#include "cinder/node/replication_manager.hpp"
#include "cinder/store/lru_store.hpp"
#include "test_scratch.hpp"
#include "unit/hint_test_helpers.hpp"

using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::steady_clock;

namespace cinder {
    namespace {

        TEST(HintQueueTest, EmptyQueueHasZeroSizeAndReplaysNothing) {
            HintQueue q;
            EXPECT_EQ(q.size(), 0U);
            size_t visited = q.replay([](const HintQueue::Hint&) {}, steady_clock::now());
            EXPECT_EQ(visited, 0U);
        }

        TEST(HintQueueTest, PushIncreasesSizeAndReplayVisitsInFifoOrder) {
            HintQueue q;
            q.push(cctMakeHint("n2", "a"));
            q.push(cctMakeHint("n2", "b"));
            q.push(cctMakeHint("n2", "c"));
            EXPECT_EQ(q.size(), 3U);

            std::vector<std::string> order;
            size_t visited = q.replay([&](const HintQueue::Hint& h) {
                order.push_back(h.req.key);
            }, steady_clock::now());
            EXPECT_EQ(visited, 3U);
            EXPECT_EQ(order, (std::vector<std::string>{"a", "b", "c"}));
            // replay() snapshots but does not consume: hints stay until remove().
            EXPECT_EQ(q.size(), 3U);
        }

        TEST(HintQueueTest, PushAlwaysSucceedsAndDropsOldestWhenFull) {
            // The bounded mutex queue uses every slot; overflow drops the
            // oldest hint rather than sacrificing a sentinel slot.
            constexpr size_t K_USABLE = HintQueue::K_CAPACITY;

            HintQueue q;
            for (size_t i = 0; i < K_USABLE; ++i) {
                EXPECT_TRUE(q.push(cctMakeHint("n2", "k" + std::to_string(i))));
            }
            EXPECT_EQ(q.size(), K_USABLE);

            // The contract is "push always succeeds"; on a full queue the OLDEST
            // entry is dropped, so size stays at capacity rather than growing.
            EXPECT_TRUE(q.push(cctMakeHint("n2", "newest")));
            EXPECT_EQ(q.size(), K_USABLE);

            std::vector<std::string> keys;
            q.replay([&](const HintQueue::Hint& h) {
                keys.push_back(h.req.key);
            }, steady_clock::now());
            ASSERT_EQ(keys.size(), K_USABLE);
            EXPECT_EQ(keys.front(), "k1") << "k0 was oldest and should have been dropped";
            EXPECT_EQ(keys.back(), "newest");
            EXPECT_EQ(std::count(keys.begin(), keys.end(), "k0"), 0);
        }

        TEST(HintQueueTest, ByteBudgetEvictsOldestFirst) {
            auto sized = cctMakeHint("n2", "k");
            sized.req.value = std::string(1000, 'x');
            const auto per_hint = HintQueue::estimatedPayloadBytes(sized);
            // Room for two hints but not three.
            auto make = [](const std::string& key) {
                auto h = cctMakeHint("n2", key);
                h.req.value = std::string(1000, 'x');
                return h;
            };

            HintQueue q;
            q.setMaxBytes(2 * per_hint + per_hint / 2);
            EXPECT_TRUE(q.push(make("old1")));
            EXPECT_TRUE(q.push(make("old2")));
            EXPECT_EQ(q.dropped(), 0U);
            EXPECT_TRUE(q.push(make("newest")));
            EXPECT_EQ(q.dropped(), 1U);
            EXPECT_LE(q.estimatedBytes(), 2 * per_hint + per_hint / 2);

            std::vector<std::string> keys;
            q.replay([&](const HintQueue::Hint& h) {
                keys.push_back(h.req.key);
            }, steady_clock::now());
            EXPECT_EQ(keys, (std::vector<std::string>{"old2", "newest"}));
        }

        TEST(HintQueueTest, CoalesceNewerReplacesSameKey) {
            HintQueue q;
            auto old_hint = cctMakeHint("n2", "k", 1);
            old_hint.req.value = "v1";
            auto new_hint = cctMakeHint("n2", "k", 2);
            new_hint.req.value = "v2";
            EXPECT_TRUE(q.push(std::move(old_hint)));
            EXPECT_TRUE(q.push(std::move(new_hint)));
            EXPECT_EQ(q.size(), 1U);
            EXPECT_EQ(q.dropped(), 0U);

            std::vector<std::string> values;
            q.replay([&](const HintQueue::Hint& h) {
                values.push_back(h.req.value);
            }, steady_clock::now());
            EXPECT_EQ(values, (std::vector<std::string>{"v2"}));
        }

        TEST(HintQueueTest, OlderWriteIgnored) {
            HintQueue q;
            auto newer = cctMakeHint("n2", "k", 2);
            newer.req.value = "v2";
            auto older = cctMakeHint("n2", "k", 1);
            older.req.value = "v1";
            EXPECT_TRUE(q.push(std::move(newer)));
            EXPECT_FALSE(q.push(std::move(older)));
            EXPECT_EQ(q.size(), 1U);
            EXPECT_EQ(q.dropped(), 0U);

            std::vector<std::string> values;
            q.replay([&](const HintQueue::Hint& h) {
                values.push_back(h.req.value);
            }, steady_clock::now());
            EXPECT_EQ(values, (std::vector<std::string>{"v2"}));
        }

        TEST(HintQueueTest, CoalesceTieBreaksOnWriterHash) {
            HintQueue q;
            auto low = cctMakeHint("n2", "k", 1);
            low.req.writer_node_hash = 7;
            low.req.value = "low";
            auto high = cctMakeHint("n2", "k", 1);
            high.req.writer_node_hash = 9;
            high.req.value = "high";
            EXPECT_TRUE(q.push(std::move(low)));
            EXPECT_TRUE(q.push(std::move(high)));
            EXPECT_EQ(q.size(), 1U);

            std::vector<std::string> values;
            q.replay([&](const HintQueue::Hint& h) {
                values.push_back(h.req.value);
            }, steady_clock::now());
            EXPECT_EQ(values, (std::vector<std::string>{"high"}));
        }

        TEST(HintQueueTest, ReplacementMovesToTail) {
            HintQueue q;
            auto make = [](const std::string& key, uint64_t version) {
                auto h = cctMakeHint("n2", key, version);
                h.req.value = key + "-v" + std::to_string(version);
                return h;
            };

            EXPECT_TRUE(q.push(make("a", 1)));
            EXPECT_TRUE(q.push(make("b", 1)));
            EXPECT_TRUE(q.push(make("a", 2)));

            std::vector<std::string> keys;
            q.replay([&](const HintQueue::Hint& h) {
                keys.push_back(h.req.key);
            }, steady_clock::now());
            EXPECT_EQ(keys, (std::vector<std::string>{"b", "a"}));
            EXPECT_EQ(q.size(), 2U);
        }

        TEST(HintQueueTest, StaleReplayHandleCannotRemoveReplacement) {
            HintQueue q;
            auto first = cctMakeHint("n2", "k", 1);
            first.req.value = "v1";
            EXPECT_TRUE(q.push(std::move(first)));

            auto snapshot = q.snapshot(steady_clock::now());
            ASSERT_EQ(snapshot.size(), 1U);

            auto second = cctMakeHint("n2", "k", 2);
            second.req.value = "v2";
            EXPECT_TRUE(q.push(std::move(second)));

            // The pre-replacement handle is stale: removing by it must not
            // delete the newer hint.
            q.remove(snapshot.front().handle);
            EXPECT_EQ(q.size(), 1U);
            std::vector<std::string> values;
            q.replay([&](const HintQueue::Hint& h) {
                values.push_back(h.req.value);
            }, steady_clock::now());
            EXPECT_EQ(values, (std::vector<std::string>{"v2"}));
        }

        TEST(HintQueueTest, CoalescingAvoidsBudgetEviction) {
            auto sized = cctMakeHint("n2", "k", 1);
            sized.req.value = std::string(1000, 'x');
            const auto per_hint = HintQueue::estimatedPayloadBytes(sized);
            auto make = [](const std::string& key, uint64_t version) {
                auto h = cctMakeHint("n2", key, version);
                h.req.value = std::string(1000, 'x');
                return h;
            };

            HintQueue q;
            q.setMaxBytes(2 * per_hint + per_hint / 2);
            EXPECT_TRUE(q.push(make("k1", 1)));
            EXPECT_TRUE(q.push(make("k2", 1)));
            // Replacement frees the old entry first: no eviction needed.
            EXPECT_TRUE(q.push(make("k1", 2)));
            EXPECT_EQ(q.size(), 2U);
            EXPECT_EQ(q.dropped(), 0U);
        }

        TEST(HintQueueTest, OversizedSingleHintRejected) {
            HintQueue q;
            q.setMaxBytes(100);
            auto big = cctMakeHint("n2", "big");
            big.req.value = std::string(10'000, 'x');
            ASSERT_GT(HintQueue::estimatedPayloadBytes(big), 100U);

            EXPECT_FALSE(q.push(std::move(big)));
            EXPECT_EQ(q.size(), 0U);
            EXPECT_EQ(q.dropped(), 1U);
            EXPECT_EQ(q.estimatedBytes(), 0U);
        }

        TEST(HintQueueTest, DefaultBudgetMatchesDocumentedValue) {
            EXPECT_EQ(HintQueue::K_DEFAULT_MAX_BYTES, 67'108'864U);
            HintQueue q;
            EXPECT_EQ(q.estimatedBytes(), 0U);
            EXPECT_EQ(q.dropped(), 0U);
        }

        TEST(HintQueueTest, ReplayDropsExpiredHintsWithoutVisitingThem) {
            HintQueue q;
            const auto now = steady_clock::now();
            auto expired = cctMakeHint("n2", "expired");
            expired.expires_at = now - milliseconds(1);
            auto live = cctMakeHint("n2", "live");
            live.expires_at = now + milliseconds(1000);

            q.push(std::move(expired));
            q.push(std::move(live));

            std::vector<std::string> seen;
            size_t visited =
                q.replay([&](const HintQueue::Hint& h) { seen.push_back(h.req.key); }, now);
            EXPECT_EQ(visited, 1U);
            EXPECT_EQ(seen, (std::vector<std::string>{"live"}));
            // Expired hints are dropped during the snapshot, not merely skipped.
            EXPECT_EQ(q.size(), 1U);
        }

        TEST(HintQueueTest, SnapshotCopiesLiveHintsWithoutConsumingThem) {
            HintQueue q;
            const auto now = steady_clock::now();
            auto expired = cctMakeHint("n2", "expired");
            expired.expires_at = now - milliseconds(1);
            auto live = cctMakeHint("n3", "live");
            live.expires_at = now + milliseconds(1000);
            q.push(std::move(expired));
            q.push(std::move(live));

            auto snapshot = q.snapshot(now);
            ASSERT_EQ(snapshot.size(), 1U);
            EXPECT_EQ(snapshot.front().hint.target, "n3");
            EXPECT_EQ(snapshot.front().hint.req.key, "live");
            // Snapshot leaves live hints queued for completion-driven removal.
            EXPECT_EQ(q.size(), 1U);
        }

        TEST(HintQueueTest, BoundedSnapshotStopsAtCapAndLeavesRemainder) {
            HintQueue q;
            const auto now = steady_clock::now();
            constexpr size_t K_TOTAL = 300;
            constexpr size_t K_CAP = 256;
            for (size_t i = 0; i < K_TOTAL; ++i) {
                auto h = cctMakeHint("n2", "k" + std::to_string(i));
                h.expires_at = now + milliseconds(1000);
                ASSERT_TRUE(q.push(std::move(h)));
            }

            // FIFO from the head, exactly cap entries; the queue is untouched.
            auto first = q.snapshot(now, K_CAP);
            ASSERT_EQ(first.size(), K_CAP);
            EXPECT_EQ(first.front().hint.req.key, "k0");
            EXPECT_EQ(first.back().hint.req.key, "k255");
            EXPECT_EQ(q.size(), K_TOTAL);

            // Removing the batch exposes the remainder on the next call.
            for (const auto& entry : first) {
                q.remove(entry.handle);
            }
            EXPECT_EQ(q.size(), K_TOTAL - K_CAP);
            auto second = q.snapshot(now, K_CAP);
            ASSERT_EQ(second.size(), K_TOTAL - K_CAP);
            EXPECT_EQ(second.front().hint.req.key, "k256");
        }

        TEST(HintQueueTest, EstimatedPayloadBytesTrackQueueRetention) {
            HintQueue q;
            std::atomic<uint64_t> entries{0};
            std::atomic<uint64_t> bytes{0};
            std::atomic<uint64_t> peak{0};
            q.setMetrics(&entries, &bytes, &peak);

            auto first = cctMakeHint("replica-1", "key-a");
            first.req.value = "value-a";
            auto second = cctMakeHint("replica-2", "key-b");
            second.req.value = "value-bb";
            const uint64_t first_bytes = HintQueue::estimatedPayloadBytes(first);
            const uint64_t second_bytes = HintQueue::estimatedPayloadBytes(second);

            q.push(std::move(first));
            EXPECT_EQ(entries.load(), 1U);
            EXPECT_EQ(bytes.load(), first_bytes);
            EXPECT_EQ(peak.load(), first_bytes);

            q.push(std::move(second));
            EXPECT_EQ(entries.load(), 2U);
            EXPECT_EQ(bytes.load(), first_bytes + second_bytes);
            EXPECT_EQ(peak.load(), first_bytes + second_bytes);

            // Snapshot prunes expired hints and updates the current gauges.
            const auto now = steady_clock::now();
            auto live = cctMakeHint("replica-3", "key-live");
            live.req.value = "live";
            live.expires_at = now + milliseconds(1000);
            const uint64_t live_bytes = HintQueue::estimatedPayloadBytes(live);
            auto expired = cctMakeHint("replica-4", "key-expired");
            expired.req.value = "expired";
            expired.expires_at = now - milliseconds(1);
            const uint64_t expired_bytes = HintQueue::estimatedPayloadBytes(expired);
            q.push(std::move(live));
            q.push(std::move(expired));
            auto snapshot = q.snapshot(now);
            ASSERT_EQ(snapshot.size(), 3U);
            EXPECT_EQ(entries.load(), 3U);
            EXPECT_EQ(bytes.load(), first_bytes + second_bytes + live_bytes);

            q.remove(snapshot.back().handle);
            EXPECT_EQ(entries.load(), 2U);
            EXPECT_EQ(bytes.load(), first_bytes + second_bytes);
            EXPECT_EQ(peak.load(), first_bytes + second_bytes + live_bytes + expired_bytes);

            // Filling past capacity drops the oldest retained payload and
            // adds the replacement, keeping count and bytes aligned.
            // Distinct keys: same-key writes coalesce instead of filling.
            // Fixed-width suffixes keep every filler hint the same size.
            auto make_filler_key = [](size_t i) {
                auto num = std::to_string(i);
                return "filler-" + std::string(4 - num.size(), '0') + num;
            };
            const uint64_t filler_bytes =
                HintQueue::estimatedPayloadBytes(cctMakeHint("replica", make_filler_key(0)));
            const uint64_t bytes_at_capacity =
                first_bytes + second_bytes + (HintQueue::K_CAPACITY - 2) * filler_bytes;
            for (size_t i = 0; i < HintQueue::K_CAPACITY - 2; ++i) {
                q.push(cctMakeHint("replica", make_filler_key(i)));
            }
            ASSERT_EQ(entries.load(), HintQueue::K_CAPACITY);
            EXPECT_EQ(bytes.load(), bytes_at_capacity);
            q.push(cctMakeHint("replica", make_filler_key(HintQueue::K_CAPACITY)));
            EXPECT_EQ(entries.load(), HintQueue::K_CAPACITY);
            EXPECT_EQ(bytes.load(), bytes_at_capacity - first_bytes + filler_bytes);
        }

        TEST(HintQueueTest, StaleHandleCannotRemoveReusedSlot) {
            HintQueue q;
            q.push(cctMakeHint("n2", "old"));
            auto old_snapshot = q.snapshot(steady_clock::now());
            ASSERT_EQ(old_snapshot.size(), 1U);

            // Remove the old generation, then reuse its slot for another key.
            q.remove(old_snapshot.front().handle);
            q.push(cctMakeHint("n2", "replacement"));
            auto replacement = q.snapshot(steady_clock::now());
            ASSERT_EQ(replacement.size(), 1U);
            EXPECT_EQ(replacement.front().hint.req.key, "replacement");
            EXPECT_NE(replacement.front().handle, old_snapshot.front().handle);

            // A late completion from the old batch must not remove the
            // replacement occupying the same physical slot.
            q.remove(old_snapshot.front().handle);
            EXPECT_EQ(q.size(), 1U);
            q.remove(replacement.front().handle);
            EXPECT_EQ(q.size(), 0U);
        }

        TEST(HintQueueTest, DroppingOldestInvalidatesItsHandle) {
            HintQueue q;
            auto first = cctMakeHint("n2", "first");
            auto oldest_handle = HintQueue::Handle{};
            q.push(std::move(first));
            auto initial = q.snapshot(steady_clock::now());
            ASSERT_EQ(initial.size(), 1U);
            oldest_handle = initial.front().handle;

            for (size_t i = 1; i < HintQueue::K_CAPACITY; ++i) {
                q.push(cctMakeHint("n2", "k" + std::to_string(i)));
            }
            ASSERT_EQ(q.size(), HintQueue::K_CAPACITY);
            q.push(cctMakeHint("n2", "newest")); // evicts the oldest slot

            // Capacity is full again and the old handle is stale. Whether the
            // physical slot has already been reused is deliberately hidden by
            // its generation tag; removing with the old handle is a no-op.
            const auto count_before = q.size();
            q.remove(oldest_handle);
            EXPECT_EQ(q.size(), count_before);
            auto live = q.snapshot(steady_clock::now());
            ASSERT_EQ(live.size(), HintQueue::K_CAPACITY);
            EXPECT_EQ(live.front().hint.req.key, "k1");
            EXPECT_EQ(live.back().hint.req.key, "newest");
        }

        TEST(HintQueueTest, ReusesFreedSlotsBeforeGrowingAndKeepsHandlesDistinct) {
            HintQueue q;
            std::vector<HintQueue::Handle> first_generation;
            first_generation.reserve(HintQueue::K_CAPACITY / 2);
            for (size_t i = 0; i < HintQueue::K_CAPACITY / 2; ++i) {
                q.push(cctMakeHint("n2", "old-" + std::to_string(i)));
            }
            auto first = q.snapshot(steady_clock::now());
            for (const auto& entry : first) {
                first_generation.push_back(entry.handle);
            }
            for (const auto handle : first_generation) {
                q.remove(handle);
            }
            ASSERT_EQ(q.size(), 0U);

            for (size_t i = 0; i < HintQueue::K_CAPACITY; ++i) {
                q.push(cctMakeHint("n3", "new-" + std::to_string(i)));
            }
            ASSERT_EQ(q.size(), HintQueue::K_CAPACITY);

            // Reused slots have incremented generations, so stale handles
            // from the previous occupants cannot erase new hints.
            for (const auto handle : first_generation) {
                q.remove(handle);
            }
            EXPECT_EQ(q.size(), HintQueue::K_CAPACITY);

            auto current = q.snapshot(steady_clock::now());
            ASSERT_EQ(current.size(), HintQueue::K_CAPACITY);
            for (size_t i = 0; i < current.size(); ++i) {
                EXPECT_EQ(current[i].hint.req.key, "new-" + std::to_string(i));
            }
        }

        TEST(HintQueueTest, RemoveMatchesOnTargetKeyAndVersionTogether) {
            HintQueue q;
            q.push(cctMakeHint("n2", "key", /*version=*/7));
            // Same (target, key) coalesces: only the newest survives.
            q.push(cctMakeHint("n2", "key", /*version=*/8));
            // Same key on a different target stays distinct.
            q.push(cctMakeHint("n3", "key", /*version=*/7));
            ASSERT_EQ(q.size(), 2U);

            net::Request probe;
            probe.key = "key";
            probe.version = 8;
            q.remove("n2", probe);

            EXPECT_EQ(q.size(), 1U);
            std::vector<std::pair<std::string, uint64_t>> remaining;
            q.replay([&](const HintQueue::Hint& h) {
                remaining.emplace_back(h.target, h.req.version);
            }, steady_clock::now());
            ASSERT_EQ(remaining.size(), 1U);
            EXPECT_EQ(remaining.front().first, "n3");
            EXPECT_EQ(remaining.front().second, 7U);
        }

        TEST(HintQueueTest, RemoveOnAbsentHintIsANoop) {
            HintQueue q;
            q.push(cctMakeHint("n2", "present"));
            net::Request probe;
            probe.key = "absent";
            probe.version = 1;
            q.remove("n2", probe);
            EXPECT_EQ(q.size(), 1U);
        }

        TEST(HintQueueTest, ReplayVisitorMayRemoveWithoutSelfDeadlock) {
            // replay() documents that the visitor runs OUTSIDE its lock, so a
            // visitor that synchronously completes a send whose completion calls
            // remove() must not self-deadlock. If the lock were held across the
            // callback this test would hang rather than fail.
            HintQueue q;
            q.push(cctMakeHint("n2", "k1"));
            q.push(cctMakeHint("n2", "k2"));

            q.replay([&](const HintQueue::Hint& h) {
                q.remove(h.target, h.req);
            }, steady_clock::now());
            EXPECT_EQ(q.size(), 0U);
        }

        TEST(HintQueueTest, ReplayOfExactlyExpiredHintIsDropped) {
            // Boundary: expires_at == now counts as expired (`<=` in replay).
            HintQueue q;
            const auto now = steady_clock::now();
            auto h = cctMakeHint("n2", "boundary");
            h.expires_at = now;
            q.push(std::move(h));

            size_t visited = q.replay([](const HintQueue::Hint&) {}, now);
            EXPECT_EQ(visited, 0U);
            EXPECT_EQ(q.size(), 0U);
        }

        TEST(HintQueueTest, ConcurrentPushReplayRemoveIsSound) {
            // MPSC stress: N producer threads push while the main thread
            // replays + removes (the production threading shape). Total live
            // hints stay under capacity so nothing may be dropped: every pushed
            // hint must be visited exactly once, and the queue must drain to
            // empty. Any lost update, torn read, or double-count fails this.
            // (Single-threaded tests above cannot exercise the concurrency;
            // TSan needs this test to have anything to check.)
            HintQueue q;
            constexpr int K_PRODUCERS = 4;
            constexpr int K_PER_PRODUCER = 200; // 800 total < 1024: no drops expected
            const auto far = steady_clock::now() + seconds(3600);

            std::atomic<int> done{0};
            std::atomic<bool> go{false};
            std::vector<std::thread> producers;
            for (int p = 0; p < K_PRODUCERS; ++p) {
                producers.emplace_back([&, p] {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    for (int i = 0; i < K_PER_PRODUCER; ++i) {
                        auto h = cctMakeHint("n" + std::to_string(p), "k" + std::to_string(i));
                        h.expires_at = far;
                        EXPECT_TRUE(q.push(std::move(h)));
                    }
                    done.fetch_add(1, std::memory_order_release);
                });
            }
            go.store(true, std::memory_order_release);

            // Drain pass: visit everything live, remove it immediately (mirrors
            // the production replay-then-remove flow on one thread).
            std::vector<std::string> seen;
            seen.reserve(K_PRODUCERS * K_PER_PRODUCER);
            auto drain_pass = [&] {
                return q.replay([&](const HintQueue::Hint& h) {
                    seen.push_back(h.target + "|" + h.req.key);
                    q.remove(h.target, h.req);
                }, steady_clock::now());
            };
            while (done.load(std::memory_order_acquire) < K_PRODUCERS) {
                drain_pass();
            }
            for (auto& t : producers) {
                t.join();
            }
            // Post-join there are no more pushes, so repeated passes must
            // terminate: each pass strictly shrinks the live set.
            for (int i = 0; i < 1000 && q.size() > 0; ++i) {
                drain_pass();
            }

            EXPECT_EQ(q.size(), 0U) << "drain did not terminate: hints leaked";
            ASSERT_EQ(seen.size(), static_cast<size_t>(K_PRODUCERS * K_PER_PRODUCER))
                << "lost or duplicated hints under concurrency";
            // Compare as sets: replay order is FIFO per producer but
            // interleaving across producers is schedule-dependent, and
            // lexicographic sort does not match numeric key order.
            std::sort(seen.begin(), seen.end());
            std::vector<std::string> want;
            want.reserve(seen.size());
            for (int p = 0; p < K_PRODUCERS; ++p) {
                for (int i = 0; i < K_PER_PRODUCER; ++i) {
                    want.push_back("n" + std::to_string(p) + "|k" + std::to_string(i));
                }
            }
            std::sort(want.begin(), want.end());
            EXPECT_EQ(seen, want) << "hint set mismatch under concurrency";
        }

        TEST(HintQueueTest, ConcurrentPushDropRemoveExpireStaysBounded) {
            // Pressure test: far more pushes than capacity, concurrent random
            // removes, short-TTL expiry mixed in. Asserts the structural
            // invariants that must hold regardless of interleaving: size never
            // exceeds capacity, the run terminates, and the queue drains to
            // empty afterwards. (Which hints survive drop-oldest/expiry is
            // schedule-dependent by design, so no exact-content assertions.)
            HintQueue q;
            constexpr int K_PRODUCERS = 4;
            constexpr int K_PER_PRODUCER = 2000; // 8000 total >> 1024: drops expected
            const auto start = steady_clock::now();
            const auto soon = start + milliseconds(50);
            const auto far = start + seconds(3600);

            std::atomic<int> done{0};
            std::atomic<bool> go{false};
            std::atomic<size_t> max_seen{0};
            std::vector<std::thread> producers;
            for (int p = 0; p < K_PRODUCERS; ++p) {
                producers.emplace_back([&, p] {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    std::random_device rd;
                    std::mt19937 rng(rd());
                    for (int i = 0; i < K_PER_PRODUCER; ++i) {
                        auto h = cctMakeHint("n" + std::to_string(p), "k" + std::to_string(i));
                        h.expires_at = (rng() & 1U) != 0U ? soon : far;
                        q.push(std::move(h));
                    }
                    done.fetch_add(1, std::memory_order_release);
                });
            }
            // Remover thread: random (target, key) removals, mostly no-ops,
            // racing replay and push exactly like send-completion callbacks do.
            std::thread remover([&] {
                while (!go.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                std::random_device rd;
                std::mt19937 rng(rd());
                for (int i = 0; i < 4000; ++i) {
                    net::Request probe;
                    probe.key = "k" + std::to_string(rng() % (K_PER_PRODUCER * 2));
                    probe.version = 1;
                    q.remove("n" + std::to_string(rng() % K_PRODUCERS), probe);
                }
            });
            go.store(true, std::memory_order_release);

            // Replay continuously while producers run (production shape: the
            // 1s timer fires regardless of push traffic).
            while (done.load(std::memory_order_acquire) < K_PRODUCERS) {
                const size_t s = q.size();
                size_t prev = max_seen.load(std::memory_order_relaxed);
                while (prev < s && !max_seen.compare_exchange_weak(prev, s)) {
                }
                q.replay([](const HintQueue::Hint&) {}, steady_clock::now());
            }
            for (auto& t : producers) {
                t.join();
            }
            remover.join();
            EXPECT_LE(max_seen.load(), HintQueue::K_CAPACITY)
                << "size() exceeded capacity under concurrency";

            // Single-threaded drain: deterministic termination. Every pass
            // removes or expires what it visits; with no concurrent pushes,
            // the live set strictly shrinks.
            for (int i = 0; i < 100'000 && q.size() > 0; ++i) {
                q.replay([&](const HintQueue::Hint& h) {
                    q.remove(h.target, h.req);
                }, steady_clock::now());
            }
            EXPECT_EQ(q.size(), 0U) << "drain did not terminate: hints leaked";
        }

        // ---------------------------------------------------------------------

        TEST(HintQueueTest, CoalesceTombstoneWinsExactStampTies) {
            // Equal (version, writer), contradictory delete state: the
            // tombstone must win regardless of arrival order, or a replayed
            // live hint resurrects a delete that already won LWW.
            HintQueue q;
            auto live = cctMakeHint("n2", "k", 5);
            live.req.writer_node_hash = 7;
            live.req.value = "live";
            auto dead = cctMakeHint("n2", "k", 5);
            dead.req.writer_node_hash = 7;
            dead.req.tombstone = true;

            EXPECT_TRUE(q.push(std::move(live)));
            EXPECT_TRUE(q.push(std::move(dead)));
            EXPECT_EQ(q.size(), 1U);
            std::vector<bool> tombstones;
            q.replay([&](const HintQueue::Hint& h) {
                tombstones.push_back(h.req.tombstone);
            }, steady_clock::now());
            ASSERT_EQ(tombstones.size(), 1U);
            EXPECT_TRUE(tombstones[0]) << "tombstone must survive live-at-tie";

            HintQueue q2;
            auto dead2 = cctMakeHint("n2", "k", 5);
            dead2.req.writer_node_hash = 7;
            dead2.req.tombstone = true;
            auto live2 = cctMakeHint("n2", "k", 5);
            live2.req.writer_node_hash = 7;
            live2.req.value = "live";
            EXPECT_TRUE(q2.push(std::move(dead2)));
            // Tied live hint is ignored (push reports false), leaving the
            // tombstone queued.
            EXPECT_FALSE(q2.push(std::move(live2)));
            EXPECT_EQ(q2.size(), 1U);
            std::vector<bool> tombstones2;
            q2.replay([&](const HintQueue::Hint& h) {
                tombstones2.push_back(h.req.tombstone);
            }, steady_clock::now());
            ASSERT_EQ(tombstones2.size(), 1U);
            EXPECT_TRUE(tombstones2[0]) << "live must not displace tombstone at tie";
        }

    } // namespace
} // namespace cinder
