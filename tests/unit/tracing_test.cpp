#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "cinder/common/tracing.hpp"

using std::chrono::steady_clock;

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
            CINDER_INFO(
                "structured test",
                {"key1", "value1"},
                {"key2", "value2"},
                {"key3", "value3"}
            );
        }

        TEST(EventTest, SingleField) {
            CINDER_WARN("single field", {"status", "error"});
        }

        TEST(FieldValueTest, LiteralBindsAsStr) {
            // String literals must bind as Str, not Bool (pointer-to-bool is a
            // standard conversion that would otherwise win over string_view).
            constexpr FieldValue F{"status", "error"};
            EXPECT_EQ(F.type, FieldValue::Type::Str);
            EXPECT_EQ(F.strView(), "error");
        }

        TEST(FieldValueTest, IntBindsAsInt) {
            constexpr FieldValue F{"n", -42};
            EXPECT_EQ(F.type, FieldValue::Type::Int);
        }

        TEST(FieldValueTest, UIntBindsAsUInt) {
            constexpr size_t N = 42;
            constexpr FieldValue F{"n", N};
            EXPECT_EQ(F.type, FieldValue::Type::UInt);
        }

        TEST(FieldValueTest, BoolBindsAsBool) {
            constexpr FieldValue F{"b", true};
            EXPECT_EQ(F.type, FieldValue::Type::Bool);
            EXPECT_TRUE(F.bool_val);
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

        TEST(SpanTest, RemoteParentAttachesExplicitly) {
            // Simulate a worker thread whose TLS belongs to an unrelated trace,
            // then attach a remote (wire-decoded) parent explicitly.
            Span unrelated("unrelated");
            SpanContext remote{0xAAAABBBBCCCCDDDDULL, 0x1111222233334444ULL};

            Span child("child", remote);
            EXPECT_EQ(child.traceId(), remote.trace_id);
            EXPECT_NE(child.spanId(), remote.span_id);
            EXPECT_EQ(child.parentSpanId(), remote.span_id);
            EXPECT_EQ(child.context(), (SpanContext{child.traceId(), child.spanId()}));
            // TLS now reflects the child, not the unrelated ambient span.
            EXPECT_EQ(currentTraceContext().trace_id, remote.trace_id);
            EXPECT_EQ(currentTraceContext().span_id, child.spanId());
        }

        TEST(SpanTest, InvalidParentStartsFreshTrace) {
            SpanContext empty;
            EXPECT_FALSE(empty.valid());
            Span span("root", empty);
            EXPECT_NE(span.traceId(), 0U);
            EXPECT_EQ(span.parentSpanId(), 0U);
        }

        TEST(TraceScopeTest, AttachAndRestore) {
            Span outer("outer");
            auto outer_ctx = outer.context();

            SpanContext other{outer_ctx.trace_id + 1, outer_ctx.span_id + 1};
            {
                TraceScope scope(other);
                EXPECT_EQ(currentTraceContext().trace_id, other.trace_id);
                EXPECT_EQ(currentTraceContext().span_id, other.span_id);
                {
                    TraceScope nested(outer_ctx);
                    EXPECT_EQ(currentTraceContext().trace_id, outer_ctx.trace_id);
                }
                EXPECT_EQ(currentTraceContext().span_id, other.span_id);
            }
            EXPECT_EQ(currentTraceContext().trace_id, outer_ctx.trace_id);
            EXPECT_EQ(currentTraceContext().span_id, outer_ctx.span_id);
        }

        TEST(TraceScopeTest, RestoresOnException) {
            Span outer("outer");
            auto before = captureTraceContext();
            bool caught = false;
            try {
                TraceScope scope({before.trace_id + 1, before.span_id + 1});
                throw std::runtime_error("boom");
            } catch (const std::runtime_error&) {
                caught = true;
            }
            EXPECT_TRUE(caught);
            EXPECT_EQ(captureTraceContext(), before);
        }

        TEST(TraceScopeTest, CrossThreadPropagation) {
            Span root("root");
            auto root_ctx = root.context();

            std::thread worker([root_ctx] {
                // New thread starts with empty TLS; attach the captured parent.
                EXPECT_EQ(captureTraceContext(), (SpanContext{}));
                TraceScope scope(root_ctx);
                Span child("child", root_ctx);
                EXPECT_EQ(child.traceId(), root_ctx.trace_id);
                EXPECT_EQ(child.parentSpanId(), root_ctx.span_id);
                EXPECT_EQ(captureTraceContext().trace_id, root_ctx.trace_id);
            });
            worker.join();
            // Originating thread untouched.
            EXPECT_EQ(captureTraceContext(), root_ctx);
        }

        TEST(TraceScopeTest, CaptureRoundTrip) {
            Span span("op");
            EXPECT_EQ(captureTraceContext(), span.context());
            EXPECT_TRUE(span.context().valid());
            EXPECT_FALSE(SpanContext{}.valid());
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

        TEST(ParseSpanContextTest, ValidPair) {
            auto ctx = parseSpanContext("12345", "67890");
            EXPECT_TRUE(ctx.valid());
            EXPECT_EQ(ctx.trace_id, 12'345);
            EXPECT_EQ(ctx.span_id, 67'890);
        }

        TEST(ParseSpanContextTest, MaxUint64) {
            auto ctx = parseSpanContext("18446744073709551615", "18446744073709551615");
            EXPECT_TRUE(ctx.valid());
            EXPECT_EQ(ctx.trace_id, 18'446'744'073'709'551'615ULL);
        }

        TEST(ParseSpanContextTest, RejectsEmpty) {
            EXPECT_FALSE(parseSpanContext("", "1").valid());
            EXPECT_FALSE(parseSpanContext("1", "").valid());
            EXPECT_FALSE(parseSpanContext("", "").valid());
        }

        TEST(ParseSpanContextTest, RejectsZero) {
            EXPECT_FALSE(parseSpanContext("0", "1").valid());
            EXPECT_FALSE(parseSpanContext("1", "0").valid());
        }

        TEST(ParseSpanContextTest, RejectsNonNumeric) {
            EXPECT_FALSE(parseSpanContext("abc", "1").valid());
            EXPECT_FALSE(parseSpanContext("1", "1.5").valid());
            EXPECT_FALSE(parseSpanContext(" 12", "1").valid());
            EXPECT_FALSE(parseSpanContext("12 ", "1").valid());
            EXPECT_FALSE(parseSpanContext("+12", "1").valid());
            EXPECT_FALSE(parseSpanContext("-1", "1").valid());
            EXPECT_FALSE(parseSpanContext("0x10", "1").valid());
        }

        TEST(ParseSpanContextTest, RejectsOverflowAndOversize) {
            EXPECT_FALSE(parseSpanContext("18446744073709551616", "1").valid());
            EXPECT_FALSE(parseSpanContext("123456789012345678901", "1").valid());
        }

        // Test fake: records every subscriber notification in order. Single-threaded
        // use only (mirrors the tests below).
        struct OrderLogSubscriber : public Subscriber {
            explicit OrderLogSubscriber(Interest interest = Interest::Always) :
                interest_(interest) {}

            auto registerCallsite(const Metadata& meta) -> Interest override {
                order.push_back("register:" + std::string(meta.target));
                return interest_;
            }

            auto newSpan(
                const Metadata& meta,
                uint64_t parent_trace_id,
                uint64_t parent_span_id
            ) noexcept -> uint64_t override {
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
                events.emplace_back(record.message);
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
                    rendered += ';';
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
            uint64_t next_id = 1000;

            auto registerCalls() const -> size_t {
                size_t n = 0;
                for (const auto& e : order) {
                    n += e.starts_with("register:") ? 1 : 0;
                }
                return n;
            }
        };

        // RAII guard: installs a subscriber for the test body, restores the
        // previous one (usually null) on destruction.
        struct SubscriberGuard {
            explicit SubscriberGuard(std::shared_ptr<Subscriber> sub) :
                prev_(globalSubscriber()) {
                setGlobalSubscriber(std::move(sub));
            }

            ~SubscriberGuard() {
                setGlobalSubscriber(prev_);
            }

            SubscriberGuard(const SubscriberGuard&) = delete;
            auto operator=(const SubscriberGuard&) -> SubscriberGuard& = delete;
            SubscriberGuard(SubscriberGuard&&) = delete;
            auto operator=(SubscriberGuard&&) -> SubscriberGuard& = delete;

            std::shared_ptr<Subscriber> prev_;
        };

        TEST(SubscriberTest, GlobalIsNullByDefault) {
            EXPECT_EQ(globalSubscriber(), nullptr);
        }

        TEST(SubscriberTest, SpanNotifiesLifecycleInOrder) {
            auto sub = std::make_shared<OrderLogSubscriber>();
            {
                SubscriberGuard guard(sub);
                {
                    Span span("op");
                    EXPECT_EQ(span.spanId(), 1000); // adopts the subscriber-assigned id
                    EXPECT_EQ(currentTraceContext().span_id, 1000);
                }
            }
            EXPECT_EQ(
                sub->order,
                (std::vector<std::string>{
                    "register:op",
                    "newSpan:op",
                    "enter:1000",
                    "exit:1000",
                    "tryClose:1000"
                })
            );
        }

        TEST(SubscriberTest, NeverSkipsNotifications) {
            auto sub = std::make_shared<OrderLogSubscriber>(Interest::Never);
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
            auto sub = std::make_shared<OrderLogSubscriber>();
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
            auto sub = std::make_shared<OrderLogSubscriber>();
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
            Metadata meta{.target = "op", .file = "f", .line = 1, .level = LogLevel::Info};
            EXPECT_EQ(sub.registerCallsite(meta), Interest::Always);
            uint64_t id = sub.newSpan(meta, 7, 0);
            EXPECT_NE(id, 0);
            // Lifecycle notifications are accepted no-ops; must not crash.
            sub.enter(id);
            sub.exit(id);
            sub.tryClose(id);
            // Events render without crashing (output goes to spdlog).
            EventRecord
                record{.metadata = meta, .message = "test", .fields = {{"n", 1}}, .context = {}};
            sub.event(record);
        }

        TEST(LayeredTest, InterestCombinesPermissively) {
            auto always = std::make_shared<OrderLogSubscriber>(Interest::Always);
            auto sometimes = std::make_shared<OrderLogSubscriber>(Interest::Sometimes);
            auto never = std::make_shared<OrderLogSubscriber>(Interest::Never);
            Metadata meta{.target = "op", .file = "f", .line = 1, .level = LogLevel::Info};

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
            auto first = std::make_shared<OrderLogSubscriber>();
            auto second = std::make_shared<OrderLogSubscriber>();
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
            auto inner = std::make_shared<OrderLogSubscriber>();
            auto filter = std::make_shared<LevelFilter>(LogLevel::Warn, inner);
            Metadata debug_meta{.target = "op", .file = "f", .line = 1, .level = LogLevel::Debug};
            Metadata warn_meta{.target = "op", .file = "f", .line = 1, .level = LogLevel::Warn};
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
            auto inner = std::make_shared<OrderLogSubscriber>(Interest::Never);
            LevelFilter filter(LogLevel::Trace, inner);
            Metadata meta{.target = "op", .file = "f", .line = 1, .level = LogLevel::Error};
            EXPECT_EQ(filter.registerCallsite(meta), Interest::Never);
        }

        TEST(EmitTest, SkipsEventWhenInterestNever) {
            // A unanimous Never short-circuits before formatting: no layer observes
            // the event at all.
            auto inner = std::make_shared<OrderLogSubscriber>();
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
            auto sub = std::make_shared<OrderLogSubscriber>(Interest::Never);
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
            auto sub = std::make_shared<OrderLogSubscriber>();
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
            auto sub = std::make_shared<OrderLogSubscriber>();
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
            auto first = std::make_shared<OrderLogSubscriber>(Interest::Never);
            auto second = std::make_shared<OrderLogSubscriber>();
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
            auto sub = std::make_shared<OrderLogSubscriber>(Interest::Sometimes);
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
            auto sub = std::make_shared<OrderLogSubscriber>();
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

        TEST(SpanStatusTest, DefaultIsUnset) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                EXPECT_EQ(span.status(), SpanStatus::Unset);
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            EXPECT_EQ(found->status, SpanStatus::Unset);
            EXPECT_TRUE(found->closed);
        }

        TEST(SpanStatusTest, SetPropagatesToRecorder) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                span.setStatus(SpanStatus::Ok);
                EXPECT_EQ(span.status(), SpanStatus::Ok);
                span.setStatus(SpanStatus::Error);
                id = span.spanId();
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            EXPECT_EQ(found->status, SpanStatus::Error); // last call wins
        }

        TEST(SpanStatusTest, ErrorSurvivesException) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                try {
                    Span span("op");
                    id = span.spanId();
                    span.setStatus(SpanStatus::Error);
                    throw std::runtime_error("boom");
                } catch (const std::runtime_error& e) {
                    EXPECT_STREQ(e.what(), "boom");
                }
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            EXPECT_TRUE(found->closed);
            EXPECT_EQ(found->status, SpanStatus::Error);
        }

        TEST(SpanStatusTest, TimeoutStatus) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("rpc");
                span.setStatus(SpanStatus::Timeout);
                id = span.spanId();
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            EXPECT_EQ(found->status, SpanStatus::Timeout);
        }

        TEST(SpanStatusTest, DroppedWithoutSubscriber) {
            // No subscriber installed: status/events must be harmless no-ops.
            Span span("op");
            span.setStatus(SpanStatus::Ok);
            span.addEvent("milestone");
            EXPECT_EQ(span.status(), SpanStatus::Ok);
        }

        TEST(SpanEventTest, MarkersRecordedWithTimestamps) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                span.addEvent("phase-a");
                span.addEvent("phase-b");
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->events.size(), 2);
            EXPECT_EQ(found->events[0].name, "phase-a");
            EXPECT_EQ(found->events[1].name, "phase-b");
            EXPECT_LE(found->started_at, found->events[0].at);
            EXPECT_LE(found->events[0].at, found->events[1].at);
            EXPECT_LE(found->events[1].at, found->ended_at);
        }

        TEST(SpanAttributeTest, TypedValuesRecorded) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                span.setAttribute({"opcode", "GET"});
                span.setAttribute({"acks", 3});
                span.setAttribute({"needed", static_cast<size_t>(2)});
                span.setAttribute({"hit", true});
                span.setAttribute({"attempt", -1});
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->attributes.size(), 5);
            auto byKey = [&](std::string_view key) -> const RecordingSubscriber::RecordedField* {
                for (const auto& a : found->attributes) {
                    if (a.key == key) {
                        return &a;
                    }
                }
                return nullptr;
            };
            const auto* op = byKey("opcode");
            ASSERT_NE(op, nullptr);
            EXPECT_EQ(op->type, FieldValue::Type::Str);
            EXPECT_EQ(op->str_val, "GET");
            const auto* acks = byKey("acks");
            ASSERT_NE(acks, nullptr);
            EXPECT_EQ(acks->type, FieldValue::Type::Int);
            EXPECT_EQ(acks->int_val, 3);
            const auto* needed = byKey("needed");
            ASSERT_NE(needed, nullptr);
            EXPECT_EQ(needed->type, FieldValue::Type::UInt);
            EXPECT_EQ(needed->uint_val, 2U);
            const auto* hit = byKey("hit");
            ASSERT_NE(hit, nullptr);
            EXPECT_EQ(hit->type, FieldValue::Type::Bool);
            EXPECT_TRUE(hit->bool_val);
        }

        TEST(SpanAttributeTest, LastWriteWinsPerKey) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                span.setAttribute({"phase", "resolve"});
                span.setAttribute({"phase", "write"});
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->attributes.size(), 1);
            EXPECT_EQ(found->attributes[0].key, "phase");
            EXPECT_EQ(found->attributes[0].str_val, "write");
        }

        TEST(SpanAttributeTest, OwnsBorrowedStrings) {
            // Attribute strings borrow caller temporaries; the recorder must
            // copy synchronously so records stay valid afterwards.
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                std::string key = "peer";
                std::string value = "node-7";
                span.setAttribute({key, std::string_view(value)});
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->attributes.size(), 1);
            EXPECT_EQ(found->attributes[0].key, "peer");
            EXPECT_EQ(found->attributes[0].str_val, "node-7");
        }

        TEST(SpanAttributeTest, DroppedWithoutSubscriber) {
            Span span("op");
            span.setAttribute({"acks", 1});
            span.addEvent("phase", {{"bytes", 64}});
            SUCCEED();
        }

        TEST(SpanEventTest, FieldsRecorded) {
            auto sub = std::make_shared<RecordingSubscriber>();
            uint64_t id = 0;
            {
                SubscriberGuard guard(sub);
                Span span("op");
                id = span.spanId();
                span.addEvent("send", {{"bytes", 64}, {"attempt", 2}});
                span.addEvent("bare");
            }
            auto found = sub->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->events.size(), 2);
            EXPECT_EQ(found->events[0].name, "send");
            ASSERT_EQ(found->events[0].fields.size(), 2);
            EXPECT_EQ(found->events[0].fields[0].key, "bytes");
            EXPECT_EQ(found->events[0].fields[0].int_val, 64);
            EXPECT_EQ(found->events[0].fields[1].key, "attempt");
            EXPECT_TRUE(found->events[1].fields.empty());
        }

        TEST(LayeredTest, StatusAttributesAndEventsFanOut) {
            // Status, attributes and events must reach every layer — the
            // previous LayeredSubscriber dropped all three. A probe layer
            // records the ids it is called with; all three calls must carry
            // the span's canonical id.
            struct ProbeLayer : public Subscriber {
                auto registerCallsite(const Metadata& /*meta*/) -> Interest override {
                    return Interest::Always;
                }

                auto newSpan(
                    const Metadata& /*meta*/,
                    uint64_t /*parent_trace_id*/,
                    uint64_t /*parent_span_id*/
                ) noexcept -> uint64_t override {
                    return 1;
                }

                void enter(uint64_t /*span_id*/) noexcept override {}

                void exit(uint64_t /*span_id*/) noexcept override {}

                void tryClose(uint64_t /*span_id*/) noexcept override {}

                void setSpanStatus(uint64_t id, SpanStatus status) noexcept override {
                    status_calls.emplace_back(id, status);
                }

                void spanAttribute(uint64_t id, const FieldValue& attr) noexcept override {
                    attr_calls.emplace_back(id, std::string(attr.key));
                }

                void spanEvent(
                    uint64_t id,
                    std::string_view name,
                    steady_clock::time_point /*at*/,
                    std::initializer_list<FieldValue> /*fields*/
                ) noexcept override {
                    event_calls.emplace_back(id, std::string(name));
                }

                void event(const EventRecord& /*record*/) noexcept override {}

                std::vector<std::pair<uint64_t, SpanStatus>> status_calls;
                std::vector<std::pair<uint64_t, std::string>> attr_calls;
                std::vector<std::pair<uint64_t, std::string>> event_calls;
            };

            auto probe = std::make_shared<ProbeLayer>();
            auto layered = std::make_shared<LayeredSubscriber>();
            layered->addLayer(probe);
            uint64_t canonical = 0;
            {
                SubscriberGuard guard(layered);
                Span span("op");
                canonical = span.spanId();
                span.setAttribute({"acks", 2});
                span.addEvent("phase", {{"bytes", 8}});
                span.setStatus(SpanStatus::Ok);
            }
            ASSERT_EQ(probe->status_calls.size(), 1);
            EXPECT_EQ(probe->status_calls[0].first, canonical);
            EXPECT_EQ(probe->status_calls[0].second, SpanStatus::Ok);
            ASSERT_EQ(probe->attr_calls.size(), 1);
            EXPECT_EQ(probe->attr_calls[0].first, canonical);
            EXPECT_EQ(probe->attr_calls[0].second, "acks");
            ASSERT_EQ(probe->event_calls.size(), 1);
            EXPECT_EQ(probe->event_calls[0].first, canonical);
            EXPECT_EQ(probe->event_calls[0].second, "phase");
        }

        TEST(LevelFilterTest, AttributesPassThrough) {
            auto inner = std::make_shared<RecordingSubscriber>();
            auto filter = std::make_shared<LevelFilter>(LogLevel::Trace, inner);
            uint64_t id = 0;
            {
                SubscriberGuard guard(filter);
                Span span("op");
                id = span.spanId();
                span.setAttribute({"acks", 2});
                span.setStatus(SpanStatus::Ok);
            }
            auto found = inner->find(id);
            ASSERT_TRUE(found.has_value());
            ASSERT_EQ(found->attributes.size(), 1);
            EXPECT_EQ(found->attributes[0].key, "acks");
            EXPECT_EQ(found->status, SpanStatus::Ok);
        }

        TEST(RecorderTest, ParentLinkage) {
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                SubscriberGuard guard(sub);
                Span root("root");
                Span child("child", root.context());
                EXPECT_EQ(child.traceId(), root.traceId());
                EXPECT_EQ(child.parentSpanId(), root.spanId());
            }
            auto spans = sub->spans();
            ASSERT_EQ(spans.size(), 2);
            EXPECT_EQ(spans[0].operation, "root");
            EXPECT_EQ(spans[0].parent_span_id, 0);
            EXPECT_NE(spans[0].trace_id, 0);
            EXPECT_EQ(spans[1].operation, "child");
            EXPECT_EQ(spans[1].trace_id, spans[0].trace_id);
            EXPECT_EQ(spans[1].parent_span_id, spans[0].span_id);
            EXPECT_TRUE(spans[0].closed);
            EXPECT_TRUE(spans[1].closed);
            EXPECT_LE(spans[0].started_at, spans[0].ended_at);
        }

        TEST(RecorderTest, LogEventsCorrelated) {
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                SubscriberGuard guard(sub);
                Span span("op");
                CINDER_INFO("hello");
            }
            auto logs = sub->logEvents();
            ASSERT_EQ(logs.size(), 1);
            EXPECT_EQ(logs[0].message, "hello");
            auto spans = sub->spans();
            ASSERT_EQ(spans.size(), 1);
            EXPECT_EQ(logs[0].trace_id, spans[0].trace_id);
            EXPECT_EQ(logs[0].span_id, spans[0].span_id);
        }

        TEST(RecorderTest, Clear) {
            auto sub = std::make_shared<RecordingSubscriber>();
            {
                SubscriberGuard guard(sub);
                Span span("op");
                CINDER_INFO("x");
            }
            EXPECT_FALSE(sub->spans().empty());
            sub->clear();
            EXPECT_TRUE(sub->spans().empty());
            EXPECT_TRUE(sub->logEvents().empty());
        }

        TEST(RecorderTest, ConcurrentSpans) {
            auto sub = std::make_shared<RecordingSubscriber>();
            constexpr int K_THREADS = 8;
            constexpr int K_SPANS = 25;
            {
                SubscriberGuard guard(sub);
                std::vector<std::thread> workers;
                for (int t = 0; t < K_THREADS; ++t) {
                    workers.emplace_back([sub] {
                        for (int i = 0; i < K_SPANS; ++i) {
                            Span span("work");
                            span.addEvent("tick");
                            span.setStatus(SpanStatus::Ok);
                        }
                    });
                }
                for (auto& w : workers) {
                    w.join();
                }
            }
            auto spans = sub->spans();
            ASSERT_EQ(spans.size(), static_cast<size_t>(K_THREADS * K_SPANS));
            for (const auto& s : spans) {
                EXPECT_TRUE(s.closed);
                EXPECT_EQ(s.status, SpanStatus::Ok);
                ASSERT_EQ(s.events.size(), 1);
            }
        }

        // Layer whose newSpan emits an event through the same layered
        // dispatcher. With lock-held dispatch this deadlocks; routed
        // lock-free it completes and the event is observed.
        struct EmittingLayer : public Subscriber {
            auto registerCallsite(const Metadata& /*meta*/) -> Interest override {
                return Interest::Always;
            }

            auto newSpan(
                const Metadata& /*meta*/,
                uint64_t /*parent_trace_id*/,
                uint64_t /*parent_span_id*/
            ) noexcept -> uint64_t override {
                Metadata meta{.target = "emit", .file = "t", .line = 1, .level = LogLevel::Info};
                Event::dispatchEvent(meta, "from-newSpan", {});
                return 1;
            }

            void enter(uint64_t /*span_id*/) noexcept override {}

            void exit(uint64_t /*span_id*/) noexcept override {}

            void tryClose(uint64_t /*span_id*/) noexcept override {}

            void event(const EventRecord& record) noexcept override {
                messages.push_back(std::string(record.message));
            }

            std::vector<std::string> messages;
        };

        TEST(LayeredTest, ReentrantEventFromNewSpan) {
            auto layer = std::make_shared<EmittingLayer>();
            auto layered = std::make_shared<LayeredSubscriber>();
            layered->addLayer(layer);
            {
                SubscriberGuard guard(layered);
                Span span("op");
            }
            ASSERT_EQ(layer->messages.size(), 1);
            EXPECT_EQ(layer->messages[0], "from-newSpan");
        }
    } // namespace
} // namespace cinder
