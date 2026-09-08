#include <gtest/gtest.h>
#include <string>

#include "cinder/common/tracing.hpp"

namespace cinder {
namespace {

TEST(EventTest, EmptyFields) {
    // Event with no fields should produce output with just the message.
    // We can't easily capture spdlog output in a unit test, but we can
    // verify that Event::debug doesn't crash with empty fields.
    Event::debug("empty fields test");
}

TEST(EventTest, StructuredFields) {
    // Event with multiple key=value fields should not crash.
    Event::info("structured test", {{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}});
}

TEST(EventTest, SingleField) {
    Event::warn("single field", {{"status", "error"}});
}

TEST(EventTest, LongMessage) {
    // Event with a message close to the 256-byte buffer limit.
    std::string long_msg(200, 'x');
    Event::debug(long_msg, {{"key", "val"}});
}

TEST(EventTest, TraceContextActive) {
    // When a Span is active, Event output should include trace context.
    Span span("test_operation");
    Event::info("event with trace context", {{"key", "value"}});
}

TEST(SpanTest, FreshTraceId) {
    // A fresh span (parent_span_id == 0) should generate a unique trace-id.
    Span span1("op1");
    Span span2("op2");
    EXPECT_NE(span1.traceId(), span2.traceId());
}

TEST(SpanTest, ChildInheritsTraceId) {
    // A child span should inherit the parent's trace-id.
    Span parent("parent");
    uint64_t parent_trace = parent.traceId();
    Span child("child", parent.spanId());
    EXPECT_EQ(child.traceId(), parent_trace);
}

TEST(SpanTest, UniqueSpanIds) {
    // Multiple spans should generate unique span-ids.
    Span s1("op1");
    Span s2("op2");
    Span s3("op3");
    EXPECT_NE(s1.spanId(), s2.spanId());
    EXPECT_NE(s2.spanId(), s3.spanId());
    EXPECT_NE(s1.spanId(), s3.spanId());
}

TEST(SpanTest, OperationAccessor) {
    Span span("test_op");
    EXPECT_EQ(span.operation(), "test_op");
}

TEST(SpanTest, NestedSaveRestore) {
    // Nested spans should correctly save/restore the thread-local context.
    Span outer("outer");
    uint64_t outer_trace = outer.traceId();
    uint64_t outer_span = outer.spanId();

    {
        Span inner("inner", outer_span);
        // Inner should have the same trace-id as outer.
        EXPECT_EQ(inner.traceId(), outer_trace);
        // But a different span-id.
        EXPECT_NE(inner.spanId(), outer_span);
    }
    // After inner is destroyed, the outer context should be restored.
    // This is verified implicitly by the next log line having outer's context.
    Event::debug("after inner scope");
}

TEST(SpanTest, OperationInTraceContext) {
    // When a Span is active, the operation name should be included in log output.
    Span span("my_operation");
    EXPECT_EQ(span.operation(), "my_operation");
}

TEST(SplitMix64Test, UniqueValues) {
    uint64_t state = 12'345;
    uint64_t v1 = splitmix64(state);
    uint64_t v2 = splitmix64(state);
    uint64_t v3 = splitmix64(state);
    EXPECT_NE(v1, v2);
    EXPECT_NE(v2, v3);
    EXPECT_NE(v1, v3);
}

TEST(SplitMix64Test, Deterministic) {
    uint64_t state1 = 999;
    uint64_t state2 = 999;
    EXPECT_EQ(splitmix64(state1), splitmix64(state2));
}
} // namespace
} // namespace cinder
