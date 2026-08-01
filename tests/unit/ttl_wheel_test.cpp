#include <gtest/gtest.h>

#include "cinder/store/ttl_wheel.hpp"

namespace cinder {
namespace {

TEST(TtlWheelTest, InsertAndTick) {
    TtlWheel wheel;
    wheel.insert("key1", 1);
    wheel.insert("key2", 2);

    auto expired = wheel.tick();
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key1");

    expired = wheel.tick();
    ASSERT_EQ(expired.size(), 1);
    EXPECT_EQ(expired[0], "key2");
}

TEST(TtlWheelTest, NoExpiryBeforeSlot) {
    TtlWheel wheel;
    wheel.insert("key", 5);

    for (int i = 0; i < 4; i++) {
        auto expired = wheel.tick();
        EXPECT_TRUE(expired.empty());
    }
}

TEST(TtlWheelTest, MultipleExpiries) {
    TtlWheel wheel;
    wheel.insert("a", 1);
    wheel.insert("b", 1);

    auto expired = wheel.tick();
    ASSERT_EQ(expired.size(), 2);
}

TEST(TtlWheelTest, RemoveBeforeExpiry) {
    TtlWheel wheel;
    wheel.insert("key", 1);
    wheel.remove("key");

    auto expired = wheel.tick();
    EXPECT_TRUE(expired.empty());
}

TEST(TtlWheelTest, WrapAround) {
    TtlWheel wheel;
    wheel.insert("key", TtlWheel::K_SLOT_COUNT);
    for (size_t i = 0; i < TtlWheel::K_SLOT_COUNT; i++) {
        auto expired = wheel.tick();
        if (!expired.empty()) {
            ASSERT_EQ(expired.size(), 1);
            EXPECT_EQ(expired[0], "key");
            return;
        }
    }
    GTEST_FAIL() << "key was never expired after a full wheel rotation";
}
} // namespace
} // namespace cinder
