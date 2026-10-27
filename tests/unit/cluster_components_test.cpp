// Direct unit tests for cluster components that previously had only indirect
// coverage (§4.2). Deliberately narrow, and deliberately NOT a re-test of what
// existing files already pin:
//
//   * HintQueue  — previously ZERO direct tests, despite non-obvious ring-buffer
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
#include <gtest/gtest.h>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cinder/cluster/membership.hpp"
#include "cinder/node/hint_queue.hpp"

using std::chrono::milliseconds;
using std::chrono::steady_clock;

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
            // The lock-free ring uses ticket sequence numbers instead of a
            // head==tail sentinel, so the full K_CAPACITY slots are usable
            // (no sacrificed slot, unlike the old mutex version).
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

        TEST(HintQueueTest, RemoveMatchesOnTargetKeyAndVersionTogether) {
            HintQueue q;
            q.push(cctMakeHint("n2", "key", /*version=*/7));
            q.push(cctMakeHint("n2", "key", /*version=*/8));
            q.push(cctMakeHint("n3", "key", /*version=*/7));
            ASSERT_EQ(q.size(), 3U);

            // Same key+version on a different target, and same target+key at a
            // different version, are DISTINCT hints and must both survive.
            net::Request probe;
            probe.key = "key";
            probe.version = 7;
            q.remove("n2", probe);

            EXPECT_EQ(q.size(), 2U);
            std::vector<std::pair<std::string, uint64_t>> remaining;
            q.replay([&](const HintQueue::Hint& h) {
                remaining.emplace_back(h.target, h.req.version);
            }, steady_clock::now());
            for (const auto& [target, version] : remaining) {
                EXPECT_FALSE(target == "n2" && version == 7) << "removed hint still present";
            }
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
            // (Single-threaded tests above cannot exercise the atomics; TSan
            // needs this test to have anything to check.)
            HintQueue q;
            constexpr int kProducers = 4;
            constexpr int kPerProducer = 200; // 800 total < 1024: no drops expected
            const auto far = steady_clock::now() + std::chrono::seconds(3600);

            std::atomic<int> done{0};
            std::atomic<bool> go{false};
            std::vector<std::thread> producers;
            for (int p = 0; p < kProducers; ++p) {
                producers.emplace_back([&, p] {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    for (int i = 0; i < kPerProducer; ++i) {
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
            seen.reserve(kProducers * kPerProducer);
            auto drain_pass = [&] {
                return q.replay([&](const HintQueue::Hint& h) {
                    seen.push_back(h.target + "|" + h.req.key);
                    q.remove(h.target, h.req);
                }, steady_clock::now());
            };
            while (done.load(std::memory_order_acquire) < kProducers) {
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
            ASSERT_EQ(seen.size(), static_cast<size_t>(kProducers * kPerProducer))
                << "lost or duplicated hints under concurrency";
            // Compare as sets: replay order is FIFO per producer but
            // interleaving across producers is schedule-dependent, and
            // lexicographic sort does not match numeric key order.
            std::sort(seen.begin(), seen.end());
            std::vector<std::string> want;
            want.reserve(seen.size());
            for (int p = 0; p < kProducers; ++p) {
                for (int i = 0; i < kPerProducer; ++i) {
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
            constexpr int kProducers = 4;
            constexpr int kPerProducer = 2000; // 8000 total >> 1024: drops expected
            const auto start = steady_clock::now();
            const auto soon = start + milliseconds(50);
            const auto far = start + std::chrono::seconds(3600);

            std::atomic<int> done{0};
            std::atomic<bool> go{false};
            std::atomic<size_t> max_seen{0};
            std::vector<std::thread> producers;
            for (int p = 0; p < kProducers; ++p) {
                producers.emplace_back([&, p] {
                    while (!go.load(std::memory_order_acquire)) {
                        std::this_thread::yield();
                    }
                    std::random_device rd;
                    std::mt19937 rng(rd());
                    for (int i = 0; i < kPerProducer; ++i) {
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
                    probe.key = "k" + std::to_string(rng() % (kPerProducer * 2));
                    probe.version = 1;
                    q.remove("n" + std::to_string(rng() % kProducers), probe);
                }
            });
            go.store(true, std::memory_order_release);

            // Replay continuously while producers run (production shape: the
            // 1s timer fires regardless of push traffic).
            while (done.load(std::memory_order_acquire) < kProducers) {
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
    } // namespace
} // namespace cinder
