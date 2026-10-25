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
#include <chrono>
#include <gtest/gtest.h>
#include <string>
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
            // Usable capacity is K_MAX_HINTS, not K_CAPACITY: the ring uses the
            // next==tail_ sentinel to distinguish full from empty, so one slot is
            // always sacrificed. Asserted explicitly so the constant's name
            // cannot mislead a future reader into expecting 1024 live hints.
            constexpr size_t K_USABLE = HintQueue::K_MAX_HINTS;

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
