#include "cinder/common/status.hpp"

#include <gtest/gtest.h>

namespace cinder {
namespace {

TEST(ResultTest, OkValue) {
    auto r = Result<int>::ok(42);
    EXPECT_TRUE(r.has_value());
    EXPECT_EQ(r.value(), 42);
}

TEST(ResultTest, ErrorValue) {
    auto r = Result<int>::err(Error(Errc::NotFound, "missing"));
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), Errc::NotFound);
    EXPECT_EQ(r.error().message(), "missing");
}

TEST(ResultTest, ValueOr) {
    auto ok = Result<int>::ok(42);
    EXPECT_EQ(ok.value_or(0), 42);

    auto err = Result<int>::err(Error(Errc::NotFound));
    EXPECT_EQ(err.value_or(0), 0);
}

TEST(ResultTest, VoidOk) {
    auto r = Result<void>::ok();
    EXPECT_TRUE(r.has_value());
}

TEST(ResultTest, VoidError) {
    auto r = Result<void>::err(Error(Errc::NotFound));
    EXPECT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code(), Errc::NotFound);
}

TEST(ResultTest, MoveOnlyType) {
    auto r = Result<std::unique_ptr<int>>::ok(std::make_unique<int>(42));
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
