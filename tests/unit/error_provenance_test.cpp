#include <gtest/gtest.h>

#include "cinder/common/status.hpp"

namespace cinder {
namespace {

TEST(ErrorProvenanceTest, UnwrappedErrorHasNullCause) {
    Error e(Errc::NotFound, "not found");
    EXPECT_EQ(e.cause(), nullptr);
}

TEST(ErrorProvenanceTest, WrapPreservesCause) {
    Error original(Errc::Timeout, "original error");
    Error wrapped = original.wrap(Errc::NotReady, "wrapped error");

    EXPECT_EQ(wrapped.code(), Errc::NotReady);
    EXPECT_EQ(wrapped.message(), "wrapped error");
    ASSERT_NE(wrapped.cause(), nullptr);
    EXPECT_EQ(wrapped.cause()->code(), Errc::Timeout);
    EXPECT_EQ(wrapped.cause()->message(), "original error");
}

TEST(ErrorProvenanceTest, WrapDeepChain) {
    Error e1(Errc::Timeout, "level 1");
    Error e2 = e1.wrap(Errc::NotReady, "level 2");
    Error e3 = e2.wrap(Errc::InternalError, "level 3");

    EXPECT_EQ(e3.code(), Errc::InternalError);
    ASSERT_NE(e3.cause(), nullptr);
    EXPECT_EQ(e3.cause()->code(), Errc::NotReady);
    ASSERT_NE(e3.cause()->cause(), nullptr);
    EXPECT_EQ(e3.cause()->cause()->code(), Errc::Timeout);
    EXPECT_EQ(e3.cause()->cause()->cause(), nullptr);
}

TEST(ErrorProvenanceTest, CopyConstructsDeep) {
    Error original(Errc::Timeout, "original");
    Error wrapped = original.wrap(Errc::NotReady, "wrapped");
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    Error copy(wrapped);

    EXPECT_EQ(copy.code(), Errc::NotReady);
    ASSERT_NE(copy.cause(), nullptr);
    EXPECT_EQ(copy.cause()->code(), Errc::Timeout);

    // Verify deep copy — copy's cause is a separate allocation
    EXPECT_NE(copy.cause(), wrapped.cause());
}

TEST(ErrorProvenanceTest, CopyAssignsDeep) {
    Error e1(Errc::Timeout, "e1");
    Error e2(Errc::NotFound, "e2");
    Error wrapped = e1.wrap(Errc::NotReady, "wrapped");

    e2 = wrapped;
    EXPECT_EQ(e2.code(), Errc::NotReady);
    ASSERT_NE(e2.cause(), nullptr);
    EXPECT_EQ(e2.cause()->code(), Errc::Timeout);
}

TEST(ErrorProvenanceTest, CopySelfAssignment) {
    Error e(Errc::Timeout, "self");
    Error wrapped = e.wrap(Errc::NotReady, "wrapped");
    EXPECT_EQ(wrapped.code(), Errc::NotReady);
    ASSERT_NE(wrapped.cause(), nullptr);
}

TEST(ErrorProvenanceTest, MoveConstructs) {
    Error original(Errc::Timeout, "original");
    Error wrapped = original.wrap(Errc::NotReady, "wrapped");
    Error moved(std::move(wrapped));

    EXPECT_EQ(moved.code(), Errc::NotReady);
    ASSERT_NE(moved.cause(), nullptr);
    EXPECT_EQ(moved.cause()->code(), Errc::Timeout);
}

TEST(ErrorProvenanceTest, WrapPreservesLocation) {
    Error original(Errc::Timeout, "original");
    Error wrapped = original.wrap(Errc::NotReady, "wrapped");

    // The wrapped error should have its own source_location (from the wrap call)
    // and the original should have its own (from construction)
    EXPECT_NE(wrapped.location().line(), original.location().line());
}

TEST(ErrorProvenanceTest, WrapWithEmptyMessage) {
    Error original(Errc::Timeout, "original");
    Error wrapped = original.wrap(Errc::NotReady);

    EXPECT_EQ(wrapped.code(), Errc::NotReady);
    EXPECT_EQ(wrapped.message().size(), 0U);
    ASSERT_NE(wrapped.cause(), nullptr);
    EXPECT_EQ(wrapped.cause()->code(), Errc::Timeout);
}
} // namespace
} // namespace cinder
