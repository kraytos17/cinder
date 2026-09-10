#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "cinder/store/ttl_wheel.hpp"

namespace cinder {
namespace {

// Simple test node that inherits from WheelNode for intrusive wheel linkage.
struct TestNode : WheelNode {
    std::string key;
};

// Helper: tick once and collect expired keys via callback.
auto
tickCollect(TtlWheel<TestNode>& wheel) -> std::vector<std::string> {
    std::vector<std::string> expired;
    wheel.tick([&](TestNode& node) { expired.push_back(node.key); });
    return expired;
}

TEST(TtlWheelTest, InsertAndTick) {
    TtlWheel<TestNode> wheel;
    TestNode n1{.key = "key1"};
    TestNode n2{.key = "key2"};

    wheel.insert(&n1, 1);
    wheel.insert(&n2, 2);

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key1");

    expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key2");
}

TEST(TtlWheelTest, NoExpiryBeforeSlot) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    wheel.insert(&node, 5);

    for (int i = 0; i < 4; i++) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty());
    }
}

TEST(TtlWheelTest, MultipleExpiries) {
    TtlWheel<TestNode> wheel;
    TestNode a{.key = "a"};
    TestNode b{.key = "b"};

    wheel.insert(&a, 1);
    wheel.insert(&b, 1);

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 2);
}

TEST(TtlWheelTest, RemoveBeforeExpiry) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    wheel.insert(&node, 1);
    wheel.remove(&node);

    auto expired = tickCollect(wheel);
    EXPECT_TRUE(expired.empty());
}

TEST(TtlWheelTest, InsertOverwritesRemovesOldSlot) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    wheel.insert(&node, 1);
    wheel.insert(&node, 10); // expiry moved — must leave slot 1

    for (int i = 0; i < 9; i++) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "stale entry fired from old slot at tick " << i;
    }

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key");
}

TEST(TtlWheelTest, WrapAround) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    wheel.insert(&node, TtlWheel<TestNode>::K_SLOT_COUNT);

    for (size_t i = 0; i < TtlWheel<TestNode>::K_SLOT_COUNT; i++) {
        auto expired = tickCollect(wheel);
        if (!expired.empty()) {
            ASSERT_EQ(expired.size(), 1);
            EXPECT_EQ(expired[0], "key");
            return;
        }
    }
    GTEST_FAIL() << "key was never expired after a full wheel rotation";
}

TEST(TtlWheelTest, LongTtlFiresWithoutReinsert) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "long_key"};
    wheel.insert(&node, TtlWheel<TestNode>::K_SLOT_COUNT + 100); // 356 ticks

    // Tick 355 times — should not fire.
    for (size_t i = 0; i < 355; ++i) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "fired early at tick " << i;
    }

    // Tick once more — should fire.
    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "long_key");
}

TEST(TtlWheelTest, LongTtlRemovePreventsFire) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "long_key"};
    wheel.insert(&node, TtlWheel<TestNode>::K_SLOT_COUNT + 200);
    wheel.remove(&node);

    // Tick past the would-be expiry — key must never fire.
    for (size_t i = 0; i < TtlWheel<TestNode>::K_SLOT_COUNT + 201; ++i) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "removed key fired at tick " << i;
    }
}

TEST(TtlWheelTest, MixedShortAndLongTtl) {
    TtlWheel<TestNode> wheel;
    TestNode short_node{.key = "short"};
    TestNode long_node{.key = "long"};
    wheel.insert(&short_node, 1);
    wheel.insert(&long_node, TtlWheel<TestNode>::K_SLOT_COUNT + 50); // 306 ticks

    // Tick 1 — short fires, long does not.
    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "short");

    // Tick 304 more — nothing should fire.
    for (size_t i = 0; i < 304; ++i) {
        expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "unexpected fire at tick " << i + 1;
    }

    // Tick once more (total 306) — long fires.
    expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "long");
}

TEST(TtlWheelTest, LongTtlOverwriteRoutesToWheel) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    wheel.insert(&node, TtlWheel<TestNode>::K_SLOT_COUNT + 100); // heap path

    // Overwrite with a short TTL — should move to the wheel.
    wheel.insert(&node, 3);

    // Tick 2 — should not fire.
    for (int i = 0; i < 2; ++i) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty());
    }

    // Tick once more — fires from the wheel, not the heap.
    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key");
}

TEST(TtlWheelTest, TickCountMonotonicallyIncreases) {
    TtlWheel<TestNode> wheel;
    TestNode node{.key = "key"};
    EXPECT_EQ(wheel.tickCount(), 0);
    tickCollect(wheel);
    EXPECT_EQ(wheel.tickCount(), 1);
    tickCollect(wheel);
    EXPECT_EQ(wheel.tickCount(), 2);
}
} // namespace
} // namespace cinder
