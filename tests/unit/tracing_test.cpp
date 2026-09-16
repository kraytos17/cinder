#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "cinder/common/tracing.hpp"

namespace cinder {
namespace {

TEST(EventTest, EmptyFields) {
    // Event with no fields should produce output with just the message.
    // We can't easily capture spdlog output in a unit test, but we can
    // verify that Event::debug doesn't crash with empty fields.
    CINDER_DEBUG("empty fields test");
}

TEST(EventTest, StructuredFields) {
    // Event with multiple key=value fields should not crash.
    CINDER_INFO("structured test", {"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"});
}

TEST(EventTest, SingleField) {
    CINDER_WARN("single field", {"status", "error"});
}

TEST(FieldValueTest, LiteralBindsAsStr) {
    // String literals must bind as Str, not Bool (pointer-to-bool is a
    // standard conversion that would otherwise win over string_view).
    constexpr FieldValue f{"status", "error"};
    EXPECT_EQ(f.type, FieldValue::Type::Str);
    EXPECT_EQ(f.strView(), "error");
}

TEST(FieldValueTest, IntBindsAsInt) {
    constexpr FieldValue f{"n", -42};
    EXPECT_EQ(f.type, FieldValue::Type::Int);
}

TEST(FieldValueTest, UIntBindsAsUInt) {
    constexpr size_t n = 42;
    constexpr FieldValue f{"n", n};
    EXPECT_EQ(f.type, FieldValue::Type::UInt);
}

TEST(FieldValueTest, BoolBindsAsBool) {
    constexpr FieldValue f{"b", true};
    EXPECT_EQ(f.type, FieldValue::Type::Bool);
    EXPECT_TRUE(f.bool_val);
}

TEST(FieldValueTest, StringViewBindsAsStr) {
    const std::string s = "hello";
    const FieldValue f{"k", std::string_view(s)};
    EXPECT_EQ(f.type, FieldValue::Type::Str);
    EXPECT_EQ(f.strView(), "hello");
}

TEST(EventTest, LongMessage) {
    // Event with a message close to the 256-byte buffer limit.
    std::string long_msg(200, 'x');
    CINDER_DEBUG(long_msg, {"key", "val"});
}

TEST(EventTest, TraceContextActive) {
    // When a Span is active, Event output should include trace context.
    Span span("test_operation");
    CINDER_INFO("event with trace context", {"key", "value"});
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
    CINDER_DEBUG("after inner scope");
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

// Test fake: records every subscriber notification in order. Single-threaded
// use only (mirrors the tests below).
struct RecordingSubscriber : public Subscriber {
    explicit RecordingSubscriber(Interest interest = Interest::Always)
        : interest_(interest) {}

    auto registerCallsite(const Metadata& meta) -> Interest override {
        order.push_back("register:" + std::string(meta.target));
        return interest_;
    }

    auto newSpan(const Metadata& meta, uint64_t parent_trace_id, uint64_t parent_span_id) noexcept
        -> uint64_t override {
        order.push_back("newSpan:" + std::string(meta.target));
        last_parent_trace = parent_trace_id;
        last_parent_span = parent_span_id;
        return next_id++;
    }

    void enter(uint64_t span_id) noexcept override {
        order.push_back("enter:" + std::to_string(span_id));
    }

    void exit(uint64_t span_id) noexcept override {
        order.push_back("exit:" + std::to_string(span_id));
    }

    void tryClose(uint64_t span_id) noexcept override {
        order.push_back("tryClose:" + std::to_string(span_id));
    }

    void event(const EventRecord& record) noexcept override {
        events.push_back(std::string(record.message));
        std::string rendered;
        for (const auto& f : record.fields) {
            rendered += std::string(f.key) + "=";
            switch (f.type) {
                case FieldValue::Type::Str:
                    rendered += std::string(f.strView());
                    break;
                case FieldValue::Type::Int:
                    rendered += std::to_string(f.int_val);
                    break;
                case FieldValue::Type::UInt:
                    rendered += std::to_string(f.uint_val);
                    break;
                case FieldValue::Type::Bool:
                    rendered += f.bool_val ? "1" : "0";
                    break;
            }
            rendered += ";";
        }
        rendered_fields.push_back(rendered);
        last_level = record.metadata.level;
    }

    Interest interest_;
    std::vector<std::string> order;
    std::vector<std::string> events;
    std::vector<std::string> rendered_fields;
    LogLevel last_level = LogLevel::Trace;
    uint64_t last_parent_trace = 0;
    uint64_t last_parent_span = 0;
    uint64_t next_id = 1'000;

    auto registerCalls() const -> size_t {
        size_t n = 0;
        for (const auto& e : order) {
            n += (e.rfind("register:", 0) == 0) ? 1 : 0;
        }
        return n;
    }
};

// RAII guard: installs a subscriber for the test body, restores the
// previous one (usually null) on destruction.
struct SubscriberGuard {
    explicit SubscriberGuard(std::shared_ptr<Subscriber> sub)
        : prev_(globalSubscriber()) {
        setGlobalSubscriber(std::move(sub));
    }

    ~SubscriberGuard() { setGlobalSubscriber(prev_); }

    std::shared_ptr<Subscriber> prev_;
};

TEST(SubscriberTest, GlobalIsNullByDefault) {
    EXPECT_EQ(globalSubscriber(), nullptr);
}

TEST(SubscriberTest, SpanNotifiesLifecycleInOrder) {
    auto sub = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(sub);
        {
            Span span("op");
            EXPECT_EQ(span.spanId(), 1'000); // adopts the subscriber-assigned id
            EXPECT_EQ(currentTraceContext().span_id, 1'000);
        }
    }
    EXPECT_EQ(sub->order,
        (std::vector<std::string>{
            "register:op", "newSpan:op", "enter:1000", "exit:1000", "tryClose:1000"}));
}

TEST(SubscriberTest, NeverSkipsNotifications) {
    auto sub = std::make_shared<RecordingSubscriber>(Interest::Never);
    uint64_t trace_id = 0;
    {
        SubscriberGuard guard(sub);
        Span span("op");
        trace_id = span.traceId();
        EXPECT_NE(trace_id, 0); // context still works without notifications
    }
    EXPECT_EQ(sub->order, (std::vector<std::string>{"register:op"}));
}

TEST(SubscriberTest, EventRoutesToSubscriberWithTypedFields) {
    auto sub = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(sub);
        CINDER_INFO("hello", {"n", 42}, {"ok", true}, {"s", "x"});
    }
    ASSERT_EQ(sub->events.size(), 1);
    EXPECT_EQ(sub->events[0], "hello");
    ASSERT_EQ(sub->rendered_fields.size(), 1);
    EXPECT_EQ(sub->rendered_fields[0], "n=42;ok=1;s=x;");
    EXPECT_EQ(sub->last_level, LogLevel::Info);
}

TEST(SubscriberTest, ChildSpanPropagatesParentIds) {
    auto sub = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(sub);
        Span parent("p");
        Span child("c", parent.spanId());
        EXPECT_EQ(sub->last_parent_trace, parent.traceId());
        EXPECT_EQ(sub->last_parent_span, parent.spanId());
    }
}

TEST(SubscriberTest, SpdlogSubscriberAcceptsEverything) {
    SpdlogSubscriber sub;
    Metadata meta{.target = "op", .level = LogLevel::Info, .file = "f", .line = 1};
    EXPECT_EQ(sub.registerCallsite(meta), Interest::Always);
    uint64_t id = sub.newSpan(meta, 7, 0);
    EXPECT_NE(id, 0);
    // Lifecycle notifications are accepted no-ops; must not crash.
    sub.enter(id);
    sub.exit(id);
    sub.tryClose(id);
    // Events render without crashing (output goes to spdlog).
    EventRecord record{.metadata = meta, .message = "test", .fields = {{"n", 1}}, .context = {}};
    sub.event(record);
}

TEST(LayeredTest, InterestCombinesPermissively) {
    auto always = std::make_shared<RecordingSubscriber>(Interest::Always);
    auto sometimes = std::make_shared<RecordingSubscriber>(Interest::Sometimes);
    auto never = std::make_shared<RecordingSubscriber>(Interest::Never);
    Metadata meta{.target = "op", .level = LogLevel::Info, .file = "f", .line = 1};

    LayeredSubscriber all_never;
    all_never.addLayer(never);
    EXPECT_EQ(all_never.registerCallsite(meta), Interest::Never);

    LayeredSubscriber mixed;
    mixed.addLayer(never);
    mixed.addLayer(sometimes);
    EXPECT_EQ(mixed.registerCallsite(meta), Interest::Sometimes);

    LayeredSubscriber permissive;
    permissive.addLayer(never);
    permissive.addLayer(sometimes);
    permissive.addLayer(always);
    EXPECT_EQ(permissive.registerCallsite(meta), Interest::Always);

    LayeredSubscriber empty;
    EXPECT_EQ(empty.registerCallsite(meta), Interest::Never);
}

TEST(LayeredTest, DispatchesToAllLayersWithCanonicalId) {
    auto first = std::make_shared<RecordingSubscriber>();
    auto second = std::make_shared<RecordingSubscriber>();
    auto layered = std::make_shared<LayeredSubscriber>();
    layered->addLayer(first);
    layered->addLayer(second);
    {
        SubscriberGuard guard(layered);
        Span span("op");
        EXPECT_EQ(span.spanId(), 1); // layered counter starts at 1
        CINDER_INFO("hello", {"n", 7});
    }
    // Both layers observe the same canonical id, in registration order.
    // Note: the Event::info call adds its own "register:<fn>" entry between
    // enter and exit (emit consults interest before formatting).
    for (const auto& sub : {first, second}) {
        ASSERT_EQ(sub->order.size(), 6);
        EXPECT_EQ(sub->order[0], "register:op");
        EXPECT_EQ(sub->order[1], "newSpan:op");
        EXPECT_EQ(sub->order[2], "enter:1");
        EXPECT_EQ(sub->order[3].substr(0, 9), "register:");
        EXPECT_EQ(sub->order[4], "exit:1");
        EXPECT_EQ(sub->order[5], "tryClose:1");
        ASSERT_EQ(sub->events.size(), 1);
        EXPECT_EQ(sub->events[0], "hello");
        ASSERT_EQ(sub->rendered_fields.size(), 1);
        EXPECT_EQ(sub->rendered_fields[0], "n=7;");
    }
}

TEST(LevelFilterTest, DropsBelowMinLevel) {
    auto inner = std::make_shared<RecordingSubscriber>();
    auto filter = std::make_shared<LevelFilter>(LogLevel::Warn, inner);
    Metadata debug_meta{.target = "op", .level = LogLevel::Debug, .file = "f", .line = 1};
    Metadata warn_meta{.target = "op", .level = LogLevel::Warn, .file = "f", .line = 1};
    EXPECT_EQ(filter->registerCallsite(debug_meta), Interest::Never);
    EXPECT_EQ(filter->registerCallsite(warn_meta), Interest::Always);
    {
        SubscriberGuard guard(filter);
        CINDER_DEBUG("dropped");
        CINDER_INFO("dropped");
        CINDER_WARN("kept");
        CINDER_ERROR("kept");
    }
    ASSERT_EQ(inner->events.size(), 2);
    EXPECT_EQ(inner->events[0], "kept");
    EXPECT_EQ(inner->events[1], "kept");
}

TEST(LevelFilterTest, RespectsInnerNever) {
    auto inner = std::make_shared<RecordingSubscriber>(Interest::Never);
    LevelFilter filter(LogLevel::Trace, inner);
    Metadata meta{.target = "op", .level = LogLevel::Error, .file = "f", .line = 1};
    EXPECT_EQ(filter.registerCallsite(meta), Interest::Never);
}

TEST(EmitTest, SkipsEventWhenInterestNever) {
    // A unanimous Never short-circuits before formatting: no layer observes
    // the event at all.
    auto inner = std::make_shared<RecordingSubscriber>();
    auto layered = std::make_shared<LayeredSubscriber>();
    layered->addLayer(std::make_shared<LevelFilter>(LogLevel::Error, inner));
    {
        SubscriberGuard guard(layered);
        CINDER_INFO("invisible", {"n", 1});
        EXPECT_TRUE(inner->events.empty());
        CINDER_ERROR("visible");
        ASSERT_EQ(inner->events.size(), 1);
        EXPECT_EQ(inner->events[0], "visible");
    }
}

TEST(MacroTest, FieldsSkippedWhenDisabled) {
    auto sub = std::make_shared<RecordingSubscriber>(Interest::Never);
    int evaluations = 0;
    auto expensive = [&] {
        ++evaluations;
        return 42;
    };
    {
        SubscriberGuard guard(sub);
        CINDER_INFO("gated", {"n", expensive()});
        CINDER_INFO("gated", {"n", expensive()});
    }
    // Disabled callsites never evaluate field expressions.
    EXPECT_EQ(evaluations, 0);
    EXPECT_TRUE(sub->events.empty());
}

TEST(MacroTest, FieldsEvaluatedWhenEnabled) {
    auto sub = std::make_shared<RecordingSubscriber>();
    int evaluations = 0;
    auto expensive = [&] {
        ++evaluations;
        return 42;
    };
    {
        SubscriberGuard guard(sub);
        CINDER_INFO("gated", {"n", expensive()});
    }
    EXPECT_EQ(evaluations, 1);
    ASSERT_EQ(sub->events.size(), 1);
    EXPECT_EQ(sub->rendered_fields[0], "n=42;");
}

TEST(MacroTest, CallsiteInterestCached) {
    auto sub = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(sub);
        for (int i = 0; i < 3; ++i) {
            CINDER_DEBUG("loop", {"i", i});
        }
    }
    // One registration for three emissions at the same callsite.
    EXPECT_EQ(sub->registerCalls(), 1);
    EXPECT_EQ(sub->events.size(), 3);
}

TEST(MacroTest, SubscriberSwapInvalidatesCache) {
    auto first = std::make_shared<RecordingSubscriber>(Interest::Never);
    auto second = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(first);
        for (int i = 0; i < 2; ++i) {
            CINDER_INFO("swap");
        }
    }
    EXPECT_EQ(first->registerCalls(), 1);
    EXPECT_TRUE(first->events.empty());
    {
        SubscriberGuard guard(second);
        for (int i = 0; i < 2; ++i) {
            CINDER_INFO("swap");
        }
    }
    // Same callsite, new subscriber: interest re-registered exactly once.
    EXPECT_EQ(second->registerCalls(), 1);
    EXPECT_EQ(second->events.size(), 2);
}

TEST(MacroTest, SometimesReconsultsEveryTime) {
    auto sub = std::make_shared<RecordingSubscriber>(Interest::Sometimes);
    {
        SubscriberGuard guard(sub);
        for (int i = 0; i < 2; ++i) {
            CINDER_INFO("s");
        }
    }
    EXPECT_EQ(sub->registerCalls(), 2);
    EXPECT_EQ(sub->events.size(), 2);
}

TEST(MacroTest, AllLevelsRoute) {
    auto sub = std::make_shared<RecordingSubscriber>();
    {
        SubscriberGuard guard(sub);
        CINDER_TRACE("t");
        CINDER_DEBUG("d");
        CINDER_INFO("i");
        CINDER_WARN("w");
        CINDER_ERROR("e");
    }
    ASSERT_EQ(sub->events.size(), 5);
    EXPECT_EQ(sub->events[0], "t");
    EXPECT_EQ(sub->events[4], "e");
}
} // namespace
} // namespace cinder
