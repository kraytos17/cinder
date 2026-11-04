// Direct unit tests for cluster components that previously had only indirect
// coverage (§4.2). Deliberately narrow, and deliberately NOT a re-test of what
// existing files already pin:
//
//   * HintQueue  — previously ZERO direct tests, despite non-obvious bounded-FIFO
//                  semantics: drop-oldest on overflow, expiry during snapshot,
//                  remove-by-triple-match, and the documented
//                  "visitor runs outside the lock" contract.
//   * MembershipTable — only the self-refutation edge case not already covered
//                  by membership_test.cpp (higher-incarnation self rumor).
//
// GossipManager / FailureDetector / ShardManager have dedicated sim coverage in
// gossip_partition_sim_test.cpp and rebalance_sim_test.cpp; MembershipTable's
// basic incarnation rules are covered by membership_test.cpp. None duplicated.
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

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::minutes;
using std::chrono::seconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;

namespace cinder {
    namespace {

        // ---------------------------------------------------------------------
        // HintQueue
        // ---------------------------------------------------------------------

        auto cctMakeHint(const std::string& target, const std::string& key, uint64_t version = 1)
            -> HintQueue::Hint {
            HintQueue::Hint h;
            h.target = target;
            h.req.key = key;
            h.req.version = version;
            h.expires_at = steady_clock::time_point::max();
            return h;
        }

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
        // MembershipTable — the one refutation case membership_test.cpp misses
        // ---------------------------------------------------------------------

        // -----------------------------------------------------------------
        // ReplicationManager hint budget
        // -----------------------------------------------------------------

        class FailingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(err<void>(Error(Errc::NotReady, "peer down")));
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "peer down")));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        // -----------------------------------------------------------------
        // HintJournal durability
        // -----------------------------------------------------------------

        struct HintJournalDir {
            // Unique per process and per case (see test_scratch.hpp).
            test::ScratchDir scratch;
            std::string dir;
            std::string journal;

            HintJournalDir() :
                dir(scratch.path().string()),
                journal((scratch.path() / "hints.journal").string()) {}
        };

        auto hjMakeHint(const std::string& target, const std::string& key, uint64_t version)
            -> HintQueue::Hint {
            auto h = cctMakeHint(target, key, version);
            h.req.value = key + "-v" + std::to_string(version);
            return h;
        }

        TEST(HintJournalTest, RoundtripPreservesLiveAndDropsTombstoned) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "keep", 1), far_future);
                journal.appendAdd(hjMakeHint("n2", "gone", 1), far_future);
                journal.appendDel("n2", "gone", 1);
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 1U);
                EXPECT_EQ(recovered.size(), 1U);
                auto snap = recovered.snapshot(steady_clock::now());
                ASSERT_EQ(snap.size(), 1U);
                EXPECT_EQ(snap.front().hint.req.key, "keep");
            }
        }

        TEST(HintJournalTest, RoundtripPreservesTombstoneAndExpiry) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                auto tomb = hjMakeHint("n2", "gone", 7);
                tomb.req.tombstone = true;
                tomb.req.expires_at = system_clock::now() + milliseconds(300'000);
                tomb.req.writer_node_hash = 11;
                journal.appendAdd(tomb, far_future);
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 1U);
                auto snap = recovered.snapshot(steady_clock::now());
                ASSERT_EQ(snap.size(), 1U);
                const auto& req = snap.front().hint.req;
                EXPECT_EQ(req.key, "gone");
                EXPECT_EQ(req.version, 7U);
                EXPECT_EQ(req.writer_node_hash, 11U);
                EXPECT_TRUE(req.tombstone);
                EXPECT_TRUE(req.expires_at.has_value());
            }
        }

        TEST(HintJournalTest, ExpiredAddsAreSkippedOnRecovery) {
            HintJournalDir paths;
            {
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "stale", 1), 1U); // long past
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 0U);
                EXPECT_EQ(recovered.size(), 0U);
            }
        }

        TEST(HintJournalTest, TornTailTruncatesWithoutLosingHistory) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 60'000U;
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                journal.appendAdd(hjMakeHint("n2", "k1", 1), far_future);
                journal.appendAdd(hjMakeHint("n2", "k2", 1), far_future);
                journal.sync();
            }
            {
                // Simulate a crash mid-append: garbage after valid records.
                int fd = ::open(paths.journal.c_str(), O_WRONLY | O_APPEND);
                ASSERT_GE(fd, 0);
                const char garbage[] = {"\x05\x00\x00partial"};
                (void)::write(fd, garbage, sizeof(garbage));
                ::close(fd);
            }
            {
                HintQueue recovered;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, recovered), 2U);
                EXPECT_EQ(journal.recordCount(), 2U);
                EXPECT_EQ(recovered.size(), 2U);
            }
        }

        TEST(HintJournalTest, CompactionRewritesFromLiveSnapshot) {
            HintJournalDir paths;
            const uint64_t far_future =
                static_cast<uint64_t>(
                    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                )
                + 600'000U;
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                // 10 live hints buried in churn: each key rewritten often so
                // most records are superseded or tombstoned. Mirrors
                // enqueueHint: queue and journal advance together.
                for (int round = 0; round < 500; ++round) {
                    for (int k = 0; k < 10; ++k) {
                        auto h = hjMakeHint("n2", "ck" + std::to_string(k), 1);
                        h.req.value = "r" + std::to_string(round);
                        h.expires_at = steady_clock::now() + minutes(10);
                        journal.appendAdd(h, far_future);
                        staging.push(std::move(h));
                        if (round % 50 == 0) {
                            journal.appendDel("n2", "ck" + std::to_string(k), 1);
                            net::Request probe;
                            probe.key = "ck" + std::to_string(k);
                            probe.version = 1;
                            staging.remove("n2", probe);
                        }
                    }
                }
                // Round 499 re-adds every key after its last tombstone
                // (round 450), so 10 hints are live at the end.
                EXPECT_EQ(staging.size(), 10U);
                EXPECT_TRUE(journal.needsCompaction());
                journal.compact(staging);
                EXPECT_FALSE(journal.needsCompaction());
                journal.sync();
            }
            {
                HintQueue recovered;
                HintJournal journal;
                auto n = journal.open(paths.journal, recovered);
                EXPECT_EQ(n, 10U);
                EXPECT_EQ(recovered.size(), 10U);
            }
        }

        TEST(ReplicationHintBoundTest, ByteBudgetBoundsQueueAndCountsDrops) {
            RealClock clock;
            LruStore store(1U << 20U);
            FailingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);
            // ~4KB budget; each hint carries a 1KB value.
            repl.setMaxHintBytes(4096);

            const std::string big(1024, 'v');
            for (int i = 0; i < 20; ++i) {
                bool done = false;
                repl.writeAsync(
                    "k" + std::to_string(i),
                    big,
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Async,
                    [&](Result<void> r) {
                    EXPECT_TRUE(r.has_value());
                    done = true;
                }
                );
                EXPECT_TRUE(done);
            }

            // Async writes succeed locally even as hints drop.
            EXPECT_LT(repl.hintCount(), 20U);
            EXPECT_GT(repl.hintsDropped(), 0U);
            EXPECT_EQ(
                metrics.replicationMetrics().hints_enqueued.load(),
                static_cast<uint64_t>(20)
            );
            EXPECT_EQ(metrics.replicationMetrics().hints_dropped.load(), repl.hintsDropped());
        }

        // Transport that records Replicate/Delete fan-out requests and acks ok
        // inline, so delete tests can assert the tombstone bit and LWW
        // identity carried on the wire.
        class CapturingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& req,
                SendCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                last_ = req;
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                on_done(err<net::Response>(Error(Errc::NotReady, "unused")));
            }

            void onMessage(MessageHandler /*handler*/) override {}

            auto last() const -> net::Request {
                std::scoped_lock lock(mutex_);
                return last_;
            }

        private:

            mutable std::mutex mutex_;
            net::Request last_;
        };

        TEST(ReplicationDeleteTest, DeleteAsyncFansOutTombstone) {
            RealClock clock;
            LruStore store(1U << 20U);
            CapturingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Async, [&](Result<void> r) {
                EXPECT_TRUE(r.has_value());
                done = true;
            });
            EXPECT_TRUE(done);

            // Local delete is a versioned tombstone, hidden from live reads.
            EXPECT_FALSE(store.get("k").has_value());
            auto tomb = store.getVersioned("k");
            ASSERT_TRUE(tomb.has_value());
            EXPECT_TRUE(tomb->isTombstone());
            EXPECT_GT(tomb->version(), 3U);

            // Fan-out carries the marker with the same LWW identity.
            auto sent = transport.last();
            EXPECT_EQ(sent.opcode, net::Opcode::Replicate);
            EXPECT_EQ(sent.key, "k");
            EXPECT_TRUE(sent.tombstone);
            EXPECT_EQ(sent.version, tomb->version());
            EXPECT_EQ(sent.writer_node_hash, tomb->writer_node_hash);
            EXPECT_TRUE(sent.expires_at.has_value()); // converged GC deadline
            EXPECT_EQ(metrics.replicationMetrics().async_deletes.load(), 1U);
        }

        TEST(ReplicationDeleteTest, DeleteAsyncQuorumFailClosedHints) {
            RealClock clock;
            LruStore store(1U << 20U);
            FailingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);
            repl.setMaxHintBytes(1U << 20U);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());
            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Quorum, [&](Result<void> r) {
                EXPECT_FALSE(r.has_value());
                EXPECT_EQ(r.error().code(), Errc::NotReady);
                done = true;
            });
            EXPECT_TRUE(done);

            // Local delete still applied; the failed replica got a hint
            // carrying the tombstone so it heals on return.
            EXPECT_FALSE(store.get("k").has_value());
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
            EXPECT_EQ(repl.hintCount(), 1U);
            EXPECT_EQ(metrics.replicationMetrics().quorum_deletes_failed.load(), 1U);
        }

        TEST(ReplicationDeleteTest, DeleteAsyncQuorumOkCountsAcks) {
            RealClock clock;
            LruStore store(1U << 20U);
            CapturingTransport transport;
            MetricsCollector metrics;
            ReplicationManager repl(store, "n1", clock, transport);
            repl.setMetrics(&metrics);

            VersionedEntry live2;
            live2.value = "v";
            live2.setVersion(3);
            live2.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live2)).has_value());
            bool done = false;
            repl.deleteAsync("k", {"n2"}, ConsistencyMode::Quorum, [&](Result<void> r) {
                EXPECT_TRUE(r.has_value());
                done = true;
            });
            EXPECT_TRUE(done);
            EXPECT_EQ(metrics.replicationMetrics().quorum_deletes_ok.load(), 1U);
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

        // Transport that answers every GetVersioned with a tombstone vote,
        // exercising the quorum-read delete path without a live peer.
        class TombstoneVotingTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                on_done(ok());
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                net::Response vote;
                vote.status = Errc::OK;
                vote.value = std::nullopt;
                vote.version = 5;
                vote.writer_node_hash = 7;
                vote.tombstone = true;
                on_done(ok(std::move(vote)));
            }

            void onMessage(MessageHandler /*handler*/) override {}
        };

        TEST(ReplicationDeleteTest, QuorumReadTombstoneVoteDeletesLocally) {
            RealClock clock;
            LruStore store(1U << 20U);
            TombstoneVotingTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(3);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            repl.readAsync("k", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                done.set_value(std::move(r));
            });

            auto result = done_future.get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->isTombstone());
            EXPECT_EQ(result->version(), 5U);
            // Self-heal applied the tombstone locally (read repair).
            EXPECT_FALSE(store.get("k").has_value());
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

        TEST(ReplicationDeleteTest, QuorumReadTombstoneTieBreaksDeterministically) {
            // Equal (version, writer), contradictory delete state: reachable
            // across standalone writers sharing writer 0. The read must pick
            // the tombstone and repair the live replica — never deadlock or
            // flap between the two.
            RealClock clock;
            LruStore store(1U << 20U);
            TombstoneVotingTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);

            VersionedEntry live;
            live.value = "v";
            live.setVersion(5);
            live.writer_node_hash = 7;
            ASSERT_TRUE(store.putVersioned("k", std::move(live)).has_value());

            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            repl.readAsync("k", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                done.set_value(std::move(r));
            });

            auto result = done_future.get();
            ASSERT_TRUE(result.has_value());
            EXPECT_TRUE(result->isTombstone()) << "tombstone must win exact-stamp ties";
            EXPECT_EQ(result->version(), 5U);
            // Loser repaired: local live copy converged to the tombstone.
            EXPECT_TRUE(store.getVersioned("k")->isTombstone());
        }

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

        TEST(ClusterComponentsTest, SelfRumorWithHigherIncarnationIsStillRefuted) {
            // membership_test.cpp covers a plain self-rumor refutation. The
            // harder case is a well-formed rumor about ourselves whose claimed
            // incarnation EXCEEDS ours: accepting it would make the node drop
            // out of its own ring, so it must still be refuted -- and the
            // refutation must beat the claimed incarnation, not merely tie it.
            MembershipTable table("self");
            table.seed({{"node2", "127.0.0.1", 7001}});

            auto before = table.get("self");
            ASSERT_TRUE(before.has_value());

            NodeInfo rumor;
            rumor.id = "self";
            rumor.host = "127.0.0.1";
            rumor.port = 7000;
            rumor.state = NodeState::Dead;
            rumor.incarnation = before->incarnation + 100;
            table.applyRumor("node2", rumor);

            auto after = table.get("self");
            ASSERT_TRUE(after.has_value());
            EXPECT_EQ(after->state, NodeState::Alive) << "self must never accept its own death";
            EXPECT_GT(after->incarnation, rumor.incarnation)
                << "refuting incarnation must exceed the claimed one, or the rumor "
                   "re-applies on the next gossip round";
        }

        // -----------------------------------------------------------------
        // Async quorum trace propagation (Phase 4)
        // -----------------------------------------------------------------

        // Transport that captures send callbacks instead of completing inline,
        // forcing fanOutQuorum's async tail (system-executor thread with
        // unrelated TLS) to deliver every completion. The test releases the
        // captured callbacks after the initiating call returns.
        class DeferredTransport : public Transport {
        public:

            void sendAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                SendCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                sends_.push_back(std::move(on_done));
            }

            void sendRequestAsync(
                const NodeId& /*to*/,
                const net::Request& /*req*/,
                RequestCallback on_done
            ) override {
                std::scoped_lock lock(mutex_);
                request_sends_.push_back(std::move(on_done));
            }

            void onMessage(MessageHandler /*handler*/) override {}

            void releaseSends(Result<void> result) {
                std::vector<SendCallback> pending;
                {
                    std::scoped_lock lock(mutex_);
                    pending = std::move(sends_);
                    sends_.clear();
                }
                for (auto& cb : pending) {
                    // Fresh Results per waiter: move_only callbacks each fire once.
                    cb(result.has_value() ? ok() : err<void>(result.error()));
                }
            }

            void releaseRequests(Result<net::Response> result) {
                std::vector<RequestCallback> pending;
                {
                    std::scoped_lock lock(mutex_);
                    pending = std::move(request_sends_);
                    request_sends_.clear();
                }
                for (auto& cb : pending) {
                    if (result.has_value()) {
                        cb(ok(std::move(result.value())));
                    } else {
                        cb(err<net::Response>(result.error()));
                    }
                }
            }

        private:

            std::mutex mutex_;
            std::vector<SendCallback> sends_;
            std::vector<RequestCallback> request_sends_;
        };

        struct TraceGuard {
            explicit TraceGuard(std::shared_ptr<Subscriber> sub) :
                prev_(globalSubscriber()) {
                setGlobalSubscriber(std::move(sub));
            }

            ~TraceGuard() {
                setGlobalSubscriber(prev_);
            }

            std::shared_ptr<Subscriber> prev_;
        };

        auto findSpan(
            const std::vector<RecordingSubscriber::RecordedSpan>& spans,
            std::string_view operation
        ) -> const RecordingSubscriber::RecordedSpan* {
            for (const auto& s : spans) {
                if (s.operation == operation) {
                    return &s;
                }
            }
            return nullptr;
        }

        auto findAttr(const RecordingSubscriber::RecordedSpan& span, std::string_view key)
            -> const RecordingSubscriber::RecordedField* {
            for (const auto& a : span.attributes) {
                if (a.key == key) {
                    return &a;
                }
            }
            return nullptr;
        }

        TEST(ReplicationQuorumTraceTest, AsyncTailKeepsRequestTrace) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD1;
            constexpr uint64_t K_PARENT = 0x0BAD2;
            std::promise<Result<void>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.writeAsync(
                    "k",
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Quorum,
                    [&done](Result<void> r) { done.set_value(std::move(r)); },
                    K_TRACE,
                    K_PARENT
                );
                // writeAsync returned with the replica outstanding: the inline
                // drain gave up, so releasing now must go through the tail.
                // The future is awaited inside the guard: the tail runs on a
                // system thread after this scope would otherwise end, and the
                // spans it creates need the subscriber still installed.
                transport.releaseSends(ok());
                ASSERT_TRUE(done_future.get().has_value());
            }

            auto spans = sub->spans();
            const auto* fanout = findSpan(spans, "replication.write.fanout");
            const auto* replica = findSpan(spans, "replication.write.replica");
            const auto* quorum = findSpan(spans, "replication.write.quorum.ok");
            ASSERT_NE(fanout, nullptr);
            ASSERT_NE(replica, nullptr);
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(fanout->trace_id, K_TRACE);
            EXPECT_EQ(replica->trace_id, K_TRACE);
            EXPECT_EQ(replica->parent_span_id, fanout->span_id);
            EXPECT_EQ(replica->status, SpanStatus::Ok);
            EXPECT_TRUE(replica->closed);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Ok);
            // Decision evidence: acks/needed on the quorum span, replica id
            // on the per-replica span, shape on the fanout.
            const auto* q_acks = findAttr(*quorum, "acks");
            ASSERT_NE(q_acks, nullptr);
            EXPECT_EQ(q_acks->uint_val, 2U);
            const auto* q_needed = findAttr(*quorum, "needed");
            ASSERT_NE(q_needed, nullptr);
            EXPECT_EQ(q_needed->uint_val, 2U);
            const auto* r_replica = findAttr(*replica, "replica");
            ASSERT_NE(r_replica, nullptr);
            EXPECT_EQ(r_replica->str_val, "n2");
            const auto* f_replicas = findAttr(*fanout, "replicas");
            ASSERT_NE(f_replicas, nullptr);
            EXPECT_EQ(f_replicas->uint_val, 1U);
        }

        TEST(ReplicationQuorumTraceTest, FailedReplicaHintEnqueueKeepsTrace) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD3;
            constexpr uint64_t K_PARENT = 0x0BAD4;
            std::promise<Result<void>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.writeAsync(
                    "k",
                    "v",
                    std::nullopt,
                    {"n2"},
                    ConsistencyMode::Quorum,
                    [&done](Result<void> r) { done.set_value(std::move(r)); },
                    K_TRACE,
                    K_PARENT
                );
                transport.releaseSends(err<void>(Error(Errc::NotReady, "peer down")));
                // Awaited under the guard for the same reason as above: the
                // tail observes the request trace only while installed.
                EXPECT_FALSE(done_future.get().has_value());
            }
            EXPECT_EQ(repl.hintCount(), 1U);

            // The hint-enqueue event fired on the tail thread must still carry
            // the request trace — not the system pool's ambient TLS.
            bool hint_logged = false;
            for (const auto& log : sub->logEvents()) {
                if (log.message == "hint enqueued" && log.trace_id == K_TRACE) {
                    hint_logged = true;
                }
            }
            EXPECT_TRUE(hint_logged) << "hint enqueue lost the request trace";

            auto spans = sub->spans();
            const auto* replica = findSpan(spans, "replication.write.replica");
            const auto* quorum = findSpan(spans, "replication.write.quorum.fail");
            ASSERT_NE(replica, nullptr);
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(replica->trace_id, K_TRACE);
            EXPECT_EQ(replica->status, SpanStatus::Error);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Error);
            // The failed decision still records how far the quorum got.
            const auto* q_acks = findAttr(*quorum, "acks");
            ASSERT_NE(q_acks, nullptr);
            EXPECT_EQ(q_acks->uint_val, 1U);
            const auto* r_replica = findAttr(*replica, "replica");
            ASSERT_NE(r_replica, nullptr);
            EXPECT_EQ(r_replica->str_val, "n2");
        }

        TEST(ReplicationQuorumTraceTest, ReadMissDecisionIsServedNotFailed) {
            RealClock clock;
            LruStore store(1U << 20U);
            DeferredTransport transport;
            ReplicationManager repl(store, "n1", clock, transport);
            auto sub = std::make_shared<RecordingSubscriber>();

            constexpr uint64_t K_TRACE = 0x0BAD5;
            constexpr uint64_t K_PARENT = 0x0BAD6;
            std::promise<Result<VersionedEntry>> done;
            auto done_future = done.get_future();
            {
                TraceGuard guard(sub);
                repl.readAsync("missing", {"n2"}, 2, [&done](Result<VersionedEntry> r) {
                    done.set_value(std::move(r));
                }, K_TRACE, K_PARENT);
                net::Response miss{.status = Errc::NotFound, .value = std::nullopt};
                transport.releaseRequests(ok(std::move(miss)));
                // Served miss: quorum answered, key simply absent everywhere.
                // Awaited under the guard so the tail's decision span lands.
                EXPECT_FALSE(done_future.get().has_value());
            }

            auto spans = sub->spans();
            const auto* quorum = findSpan(spans, "replication.read.quorum");
            ASSERT_NE(quorum, nullptr);
            EXPECT_EQ(quorum->trace_id, K_TRACE);
            EXPECT_EQ(quorum->status, SpanStatus::Ok);
        }

        // -----------------------------------------------------------------
        // Hint journal spans (Phase 6)
        // -----------------------------------------------------------------

        TEST(HintJournalSpanTest, OpenReportsOk) {
            HintJournalDir paths;
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                TraceGuard guard(sub);
                HintQueue staging;
                HintJournal journal;
                EXPECT_EQ(journal.open(paths.journal, staging), 0U);
                journal.appendAdd(hjMakeHint("n2", "k", 1), 9'999'999'999'999ULL);
                journal.sync();
                HintQueue recovered;
                HintJournal reader;
                EXPECT_EQ(reader.open(paths.journal, recovered), 1U);
            }
            auto spans = sub->spans();
            bool opened = false;
            for (const auto& s : spans) {
                if (s.operation == "hint.journal.open") {
                    opened = true;
                    EXPECT_EQ(s.status, SpanStatus::Ok);
                    EXPECT_TRUE(s.closed);
                }
            }
            // Two opens (staging + recovery) both report Ok.
            EXPECT_TRUE(opened);
            // The recovery open reports how many hints it replayed.
            bool hints_seen = false;
            for (const auto& s : spans) {
                if (s.operation == "hint.journal.open") {
                    for (const auto& a : s.attributes) {
                        if (a.key == "hints" && a.uint_val == 1) {
                            hints_seen = true;
                        }
                    }
                }
            }
            EXPECT_TRUE(hints_seen);
        }

        TEST(HintJournalSpanTest, CompactReportsOk) {
            HintJournalDir paths;
            // 4100 live adds trip the 4096-record compaction threshold.
            {
                HintQueue staging;
                HintJournal journal;
                journal.open(paths.journal, staging);
                const uint64_t far_future =
                    static_cast<uint64_t>(
                        duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
                    )
                    + 600'000ULL;
                for (int i = 0; i < 4100; ++i) {
                    journal.appendAdd(hjMakeHint("n2", "ck" + std::to_string(i), 1), far_future);
                }
                ASSERT_TRUE(journal.needsCompaction());
            }
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                TraceGuard guard(sub);
                HintQueue staging;
                HintJournal journal;
                // Recovery replays the 4100 adds (the bounded queue keeps
                // K_CAPACITY of them); the journal itself is still over the
                // 4096-record compaction threshold.
                EXPECT_GT(journal.open(paths.journal, staging), 0U);
                EXPECT_TRUE(journal.needsCompaction());
                journal.compact(staging);
                EXPECT_FALSE(journal.needsCompaction());
            }
            auto spans = sub->spans();
            const auto* compacted = findSpan(spans, "hint.journal.compact");
            ASSERT_NE(compacted, nullptr);
            EXPECT_EQ(compacted->status, SpanStatus::Ok);
            EXPECT_TRUE(compacted->closed);
        }
    } // namespace
} // namespace cinder
