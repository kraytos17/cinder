#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/net/protocol.hpp"
#include "cinder/store/crdt_counter.hpp"
#include "cinder/store/crdt_counter_store.hpp"
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

    } // namespace
} // namespace cinder
