#include <gtest/gtest.h>

#include "cinder/common/status.hpp"

namespace cinder {
namespace {

TEST(ResultTest, OkValue) {
    auto r = ok(42);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r.value(), 42);
}

TEST(ResultTest, ErrorValue) {
    auto r = err<int>(Error(Errc::NotFound, "missing"));
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), Errc::NotFound);
    EXPECT_EQ(r.error().message(), "missing");
}

TEST(ResultTest, ValueOr) {
    auto o = ok(42);
    EXPECT_EQ(o.value_or(0), 42);

    auto e = err<int>(Error(Errc::NotFound));
    EXPECT_EQ(e.value_or(0), 0);
}

TEST(ResultTest, VoidOk) {
    auto r = ok();
    EXPECT_TRUE(r.has_value());
}

TEST(ResultTest, VoidError) {
    auto r = err(Error(Errc::NotFound));
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), Errc::NotFound);
}

TEST(ResultTest, MoveOnlyType) {
    auto r = ok(std::make_unique<int>(42));
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(*r.value(), 42);
}

TEST(ResultTest, FreeFunctionOk) {
    auto r = cinder::ok(42);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r.value(), 42);
}

TEST(ResultTest, FreeFunctionErr) {
    auto r = cinder::err<int>(Error(Errc::NotFound));
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), Errc::NotFound);
}
} // namespace
} // namespace cinder
