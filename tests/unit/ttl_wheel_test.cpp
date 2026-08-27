#include <gtest/gtest.h>
#include <vector>

#include "cinder/store/ttl_wheel.hpp"

namespace cinder {
namespace {

// Helper: tick once and collect expired keys via callback.
auto
tickCollect(TtlWheel& wheel) -> std::vector<std::string> {
    std::vector<std::string> expired;
    wheel.tick([&](const std::string& key) { expired.push_back(key); });
    return expired;
}

TEST(TtlWheelTest, InsertAndTick) {
    TtlWheel wheel;
    wheel.insert("key1", 1);
    wheel.insert("key2", 2);

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key1");

    expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key2");
}

TEST(TtlWheelTest, NoExpiryBeforeSlot) {
    TtlWheel wheel;
    wheel.insert("key", 5);

    for (int i = 0; i < 4; i++) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty());
    }
}

TEST(TtlWheelTest, MultipleExpiries) {
    TtlWheel wheel;
    wheel.insert("a", 1);
    wheel.insert("b", 1);

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 2);
}

TEST(TtlWheelTest, RemoveBeforeExpiry) {
    TtlWheel wheel;
    wheel.insert("key", 1);
    wheel.remove("key");

    auto expired = tickCollect(wheel);
    EXPECT_TRUE(expired.empty());
}

TEST(TtlWheelTest, InsertOverwritesRemovesOldSlot) {
    TtlWheel wheel;
    wheel.insert("key", 1);
    wheel.insert("key", 10); // expiry moved — must leave slot 1
    for (int i = 0; i < 9; i++) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "stale entry fired from old slot at tick " << i;
    }

    auto expired = tickCollect(wheel);
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key");
}

TEST(TtlWheelTest, WrapAround) {
    TtlWheel wheel;
    wheel.insert("key", TtlWheel::K_SLOT_COUNT);
    for (size_t i = 0; i < TtlWheel::K_SLOT_COUNT; i++) {
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
    TtlWheel wheel;
    wheel.insert("long_key", TtlWheel::K_SLOT_COUNT + 100); // 356 ticks

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
    TtlWheel wheel;
    wheel.insert("long_key", TtlWheel::K_SLOT_COUNT + 200);
    wheel.remove("long_key");

    // Tick past the would-be expiry — key must never fire.
    for (size_t i = 0; i < TtlWheel::K_SLOT_COUNT + 201; ++i) {
        auto expired = tickCollect(wheel);
        EXPECT_TRUE(expired.empty()) << "removed key fired at tick " << i;
    }
}

TEST(TtlWheelTest, MixedShortAndLongTtl) {
    TtlWheel wheel;
    wheel.insert("short", 1);
    wheel.insert("long", TtlWheel::K_SLOT_COUNT + 50); // 306 ticks

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
    TtlWheel wheel;
    wheel.insert("key", TtlWheel::K_SLOT_COUNT + 100); // heap path

    // Overwrite with a short TTL — should move to the wheel.
    wheel.insert("key", 3);

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
    TtlWheel wheel;
    EXPECT_EQ(wheel.tickCount(), 0);
    tickCollect(wheel);
    EXPECT_EQ(wheel.tickCount(), 1);
    tickCollect(wheel);
    EXPECT_EQ(wheel.tickCount(), 2);
}
} // namespace
} // namespace cinder
