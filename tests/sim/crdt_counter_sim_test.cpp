#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/net/protocol.hpp"
#include "cinder/node/anti_entropy/core.hpp"
#include "cinder/store/crdt_counter.hpp"
#include "cinder/store/crdt_counter_store.hpp"
#include "cinder/store/lru_store.hpp"
#include "sim_clock.hpp"
#include "sim_transport.hpp"

using namespace std::chrono_literals;

namespace cinder {
    namespace {

        auto stateMessage(const crdt::PNCounter& counter, const std::string& key = "shared-counter")
            -> net::Request {
            net::Request req;
            req.opcode = net::Opcode::Ping;
            req.key = key;
            req.value = crdt::encodeCounter(counter);
            return req;
        }

        auto applyState(CrdtCounterStore& store, const net::Request& req) -> Errc {
            auto decoded = crdt::decodePNCounter(req.value);
            if (!decoded.has_value()) {
                return Errc::CorruptData;
            }
            auto merged = store.merge(req.key, *decoded);
            return merged.has_value() ? Errc::OK : merged.error().code();
        }

        TEST(CrdtCounterSimTest, PartitionHealingReorderedDuplicateStatesConverge) {
            SimClock clock;
            SimBus bus{clock, 0xC12D};
            CrdtCounterStore a{1U << 20U};
            CrdtCounterStore b{1U << 20U};
            std::vector<uint64_t> b_observed_a_components;
            std::vector<Errc> b_apply_results;

            bus.registerHandler("node-b", [&](const NodeId&, const net::Request& req) {
                auto remote = crdt::decodePNCounter(req.value);
                if (!remote.has_value()) {
                    b_apply_results.push_back(Errc::CorruptData);
                    return;
                }
                b_observed_a_components.push_back(remote->positive().component("node-a"));
                b_apply_results.push_back(applyState(b, req));
            });
            bus.registerHandler("node-a", [&](const NodeId&, const net::Request& req) {
                b_apply_results.push_back(applyState(a, req));
            });

            // Network partition: both replicas continue to accept independent
            // local updates; attempts to send state to the down peer fail.
            bus.setNodeDown("node-b");
            ASSERT_TRUE(a.increment("shared-counter", "node-a", 3).has_value());
            ASSERT_TRUE(b.decrement("shared-counter", "node-b", 4).has_value());
            EXPECT_FALSE(bus.route("node-a", "node-b", stateMessage(*a.get("shared-counter"))));
            EXPECT_EQ(bus.pending(), 0U);
            EXPECT_EQ(a.value("shared-counter").value(), 3);
            EXPECT_EQ(b.value("shared-counter").value(), -4);

            // Heal the partition. Queue an old state, then a newer state twice
            // (duplicate delivery). Distinct due times ensure SimBus reverses
            // their order: newest, duplicate newest, then stale state.
            bus.setNodeUp("node-b");
            bus.setReorder(true);
            bus.setDelay(10ms);
            ASSERT_TRUE(bus.route("node-a", "node-b", stateMessage(*a.get("shared-counter"))));
            ASSERT_TRUE(a.increment("shared-counter", "node-a", 4).has_value());
            bus.setDelay(20ms);
            const auto newest_a = stateMessage(*a.get("shared-counter"));
            ASSERT_TRUE(bus.route("node-a", "node-b", newest_a));
            ASSERT_TRUE(bus.route("node-a", "node-b", newest_a));

            clock.advance(20ms);
            bus.deliver();
            ASSERT_EQ(b_observed_a_components.size(), 3U);
            EXPECT_EQ(b_observed_a_components[0], 7U);
            EXPECT_EQ(b_observed_a_components[1], 7U);
            EXPECT_EQ(b_observed_a_components[2], 3U);
            for (Errc result : b_apply_results) {
                EXPECT_EQ(result, Errc::OK);
            }
            EXPECT_EQ(b.value("shared-counter").value(), 3); // 7 - 4

            // Return B's merged state. Replaying it twice is idempotent and
            // both replicas converge to the sum of independent contributions.
            bus.setReorder(false);
            bus.setDelay(5ms);
            const auto merged_b = stateMessage(*b.get("shared-counter"));
            ASSERT_TRUE(bus.route("node-b", "node-a", merged_b));
            ASSERT_TRUE(bus.route("node-b", "node-a", merged_b));
            clock.advance(5ms);
            bus.deliver();
            EXPECT_EQ(a.value("shared-counter").value(), 3);
            EXPECT_EQ(b.value("shared-counter").value(), 3);
            EXPECT_EQ(a.get("shared-counter"), b.get("shared-counter"));
        }

        TEST(CrdtCounterSimTest, CapacityRejectedStateCanBeRetriedAfterExpansion) {
            SimClock clock;
            SimBus bus{clock, 0xCA9};
            CrdtCounterStore sender{1U << 20U};
            CrdtCounterStore receiver{32};
            std::vector<Errc> apply_results;
            bus.registerHandler("receiver", [&](const NodeId&, const net::Request& req) {
                apply_results.push_back(applyState(receiver, req));
            });

            ASSERT_TRUE(sender.increment("k", "long-actor-id-for-counter", 9).has_value());
            const auto state = stateMessage(*sender.get("k"), "k");
            ASSERT_TRUE(bus.route("sender", "receiver", state));
            bus.deliver();
            ASSERT_EQ(apply_results.size(), 1U);
            EXPECT_EQ(apply_results.back(), Errc::CapacityExceeded);
            EXPECT_FALSE(receiver.value("k").has_value());

            // The merge failed atomically; retrying the exact same serialized
            // state after capacity is expanded must succeed and converge.
            receiver.setCapacity(256);
            ASSERT_TRUE(bus.route("sender", "receiver", state));
            bus.deliver();
            ASSERT_EQ(apply_results.size(), 2U);
            EXPECT_EQ(apply_results.back(), Errc::OK);
            EXPECT_EQ(receiver.get("k"), sender.get("k"));
            EXPECT_EQ(receiver.value("k").value(), 9);
        }

        TEST(CrdtCounterSimTest, AntiEntropyRepairsDroppedCounterMerge) {
            SimClock clock;
            SimBus bus{clock, 0xAE17};
            SimTransport transport_a{bus, "node-a"};
            SimTransport transport_b{bus, "node-b"};
            LruStore cache_a{1024, &clock};
            LruStore cache_b{1024, &clock};
            CrdtCounterStore counters_a{1U << 20U};
            CrdtCounterStore counters_b{1U << 20U};
            ConsistentHashRing ring_a;
            ConsistentHashRing ring_b;
            const std::vector<NodeId> members{"node-a", "node-b"};
            ring_a.setNodes(members);
            ring_b.setNodes(members);
            AntiEntropyManager ae_a(
                cache_a,
                ring_a,
                "node-a",
                clock,
                transport_a,
                8,
                nullptr,
                1U << 20U,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_a
            );
            AntiEntropyManager ae_b(
                cache_b,
                ring_b,
                "node-b",
                clock,
                transport_b,
                8,
                nullptr,
                1U << 20U,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_b
            );

            bus.registerRequestHandler("node-a", [&](const NodeId& from, const net::Request& req) {
                std::optional<net::Response> response;
                ae_a.onCounterAntiEntropy(from, req, [&](net::Response r) {
                    response = std::move(r);
                });
                EXPECT_TRUE(response.has_value());
                return response.value_or(net::Response{.status = Errc::InternalError});
            });
            bus.registerRequestHandler("node-b", [&](const NodeId& from, const net::Request& req) {
                std::optional<net::Response> response;
                ae_b.onCounterAntiEntropy(from, req, [&](net::Response r) {
                    response = std::move(r);
                });
                EXPECT_TRUE(response.has_value());
                return response.value_or(net::Response{.status = Errc::InternalError});
            });

            ASSERT_TRUE(counters_a.increment("counter-only-key", "node-a", 9).has_value());
            ASSERT_TRUE(counters_b.decrement("counter-only-key", "node-b", 4).has_value());

            // Simulate a lost async CounterMerge while B is unavailable. The
            // stores diverge, then the normal periodic counter-AE round repairs
            // both sides without relying on any LWW cache entries.
            bus.setNodeDown("node-b");
            net::Request lost_merge;
            lost_merge.opcode = net::Opcode::CounterMerge;
            lost_merge.key = "counter-only-key";
            lost_merge.value = crdt::encodeCounter(*counters_a.get("counter-only-key"));
            EXPECT_FALSE(bus.route("node-a", "node-b", lost_merge));
            EXPECT_EQ(counters_a.value("counter-only-key").value(), 9);
            EXPECT_EQ(counters_b.value("counter-only-key").value(), -4);

            bus.setNodeUp("node-b");
            ae_a.runCounterRound(2);
            EXPECT_EQ(counters_a.value("counter-only-key").value(), 5);
            EXPECT_EQ(counters_b.value("counter-only-key").value(), 5);
            EXPECT_EQ(counters_a.get("counter-only-key"), counters_b.get("counter-only-key"));

            // Duplicate and delayed counter-AE messages remain harmless: the
            // bundle exchange is full-state and each receiver joins by max.
            bus.setDelay(25ms);
            ASSERT_TRUE(bus.routeRequest(
                "node-a",
                "node-b",
                net::Request{
                    .opcode = net::Opcode::CounterAntiEntropy,
                    .value = crdt::encodeCounter(*counters_a.get("counter-only-key")),
                },
                [](Result<net::Response>) {}
            ));
            ASSERT_TRUE(bus.routeRequest(
                "node-a",
                "node-b",
                net::Request{
                    .opcode = net::Opcode::CounterAntiEntropy,
                    .value = crdt::encodeCounter(*counters_a.get("counter-only-key")),
                },
                [](Result<net::Response>) {}
            ));
            clock.advance(25ms);
            bus.deliver();
            EXPECT_EQ(counters_a.get("counter-only-key"), counters_b.get("counter-only-key"));
        }

        TEST(CrdtCounterSimTest, AntiEntropyCursorRepairsAcrossBoundedBatches) {
            SimClock clock;
            SimBus bus{clock, 0xBA7C};
            SimTransport transport_a{bus, "node-a"};
            SimTransport transport_b{bus, "node-b"};
            LruStore cache_a{1024, &clock};
            LruStore cache_b{1024, &clock};
            CrdtCounterStore counters_a{1U << 20U};
            CrdtCounterStore counters_b{1U << 20U};
            ConsistentHashRing ring_a;
            ConsistentHashRing ring_b;
            const std::vector<NodeId> members{"node-a", "node-b"};
            ring_a.setNodes(members);
            ring_b.setNodes(members);
            constexpr size_t K_BATCH_BYTES = 128;
            AntiEntropyManager ae_a(
                cache_a,
                ring_a,
                "node-a",
                clock,
                transport_a,
                8,
                nullptr,
                K_BATCH_BYTES,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_a
            );
            AntiEntropyManager ae_b(
                cache_b,
                ring_b,
                "node-b",
                clock,
                transport_b,
                8,
                nullptr,
                K_BATCH_BYTES,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters_b
            );
            bus.registerRequestHandler("node-a", [&](const NodeId& from, const net::Request& req) {
                std::optional<net::Response> response;
                ae_a.onCounterAntiEntropy(from, req, [&](net::Response r) {
                    response = std::move(r);
                });
                EXPECT_TRUE(response.has_value());
                return response.value_or(net::Response{.status = Errc::InternalError});
            });
            bus.registerRequestHandler("node-b", [&](const NodeId& from, const net::Request& req) {
                std::optional<net::Response> response;
                ae_b.onCounterAntiEntropy(from, req, [&](net::Response r) {
                    response = std::move(r);
                });
                EXPECT_TRUE(response.has_value());
                return response.value_or(net::Response{.status = Errc::InternalError});
            });

            ASSERT_TRUE(counters_a.increment("a", "node-a", 3).has_value());
            ASSERT_TRUE(counters_a.increment("b", "node-a", 7).has_value());
            ASSERT_TRUE(counters_b.decrement("a", "node-b", 2).has_value());
            ASSERT_TRUE(counters_b.decrement("b", "node-b", 4).has_value());

            // 128 bytes admits one merged counter record per message.
            // Repeated
            // rounds must advance the independent cursor until both keys have
            // converged; no key is starved behind the first sorted entry.
            for (int round = 0; round < 16; ++round) {
                ae_a.runCounterRound(2);
                ae_b.runCounterRound(2);
                SCOPED_TRACE(round);
                EXPECT_EQ(counters_a.get("a"), counters_b.get("a"));
                EXPECT_EQ(counters_a.get("b"), counters_b.get("b"));
            }
            EXPECT_EQ(counters_a.value("a"), 1);
            EXPECT_EQ(counters_b.value("a"), 1);
            EXPECT_EQ(counters_a.value("b"), 3);
            EXPECT_EQ(counters_b.value("b"), 3);
            EXPECT_EQ(counters_a.get("a"), counters_b.get("a"));
            EXPECT_EQ(counters_a.get("b"), counters_b.get("b"));
        }

        TEST(CrdtCounterSimTest, AntiEntropyRejectsCounterLargerThanBatchBudget) {
            SimClock clock;
            SimBus bus{clock, 0xB16};
            SimTransport transport{bus, "node-a"};
            LruStore cache{1024, &clock};
            CrdtCounterStore counters{1U << 20U};
            ConsistentHashRing ring;
            const std::vector<NodeId> members{"node-a", "node-b"};
            ring.setNodes(members);
            AntiEntropyManager ae(
                cache,
                ring,
                "node-a",
                clock,
                transport,
                8,
                nullptr,
                32,
                AntiEntropyManager::K_DEFAULT_FULL_REHASH_INTERVAL,
                &counters
            );
            ASSERT_TRUE(
                counters.increment("large", "actor-name-too-long-for-budget", 1).has_value()
            );
            ae.runCounterRound(2);
            // The unencodable batch is deferred; it is not sent as an empty
            // success that could advance/strand the cursor.
            EXPECT_EQ(bus.pending(), 0U);
            EXPECT_EQ(counters.value("large").value(), 1);
        }

    } // namespace
} // namespace cinder
