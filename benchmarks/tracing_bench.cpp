#include <benchmark/benchmark.h>
#include <memory>

#include "cinder/common/tracing.hpp"

// Tracing substrate overhead (plan Phase 9): the production default pays for
// span create/destroy with no subscriber installed. Status, attributes and
// events must add ~nothing on top in that configuration (dropped silently),
// while the opted-in RecordingSubscriber path quantifies the full cost.

namespace cinder {
    namespace {

        void bmSpanNoSubscriber(benchmark::State& state) {
            // NOLINTNEXTLINE
            for (auto _ : state) {
                Span span("bench.op");
                benchmark::DoNotOptimize(span.spanId());
            }
        }

        BENCHMARK(bmSpanNoSubscriber);

        void bmSpanNoSubscriberChild(benchmark::State& state) {
            // Child spans skip the freshTraceId() atomic (they inherit the
            // parent's trace id). Parent lives outside the loop; each child
            // still pays TLS save/restore + splitmix span id.
            Span parent("bench.parent");
            // NOLINTNEXTLINE
            for (auto _ : state) {
                Span child("bench.op", parent.spanId());
                benchmark::DoNotOptimize(child.spanId());
            }
        }

        BENCHMARK(bmSpanNoSubscriberChild);

        void bmSpanNoSubscriberWithOutcome(benchmark::State& state) {
            // NOLINTNEXTLINE
            for (auto _ : state) {
                Span span("bench.op");
                span.setAttribute({"phase", "write"});
                span.setAttribute({"bytes_out", 128U});
                span.setAttribute({"attempt", 1});
                span.addEvent("flushed", {{"bytes", 128}});
                span.setStatus(SpanStatus::Ok);
                benchmark::DoNotOptimize(span.spanId());
            }
        }

        BENCHMARK(bmSpanNoSubscriberWithOutcome);

        void bmSpanRecording(benchmark::State& state) {
            auto sub = std::make_shared<RecordingSubscriber>();
            auto prev = globalSubscriber();
            setGlobalSubscriber(sub);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                Span span("bench.op");
                benchmark::DoNotOptimize(span.spanId());
            }
            setGlobalSubscriber(prev);
            state.SetItemsProcessed(state.iterations());
            state.counters["spans"] = static_cast<double>(sub->spans().size());
        }

        BENCHMARK(bmSpanRecording);

        void bmSpanRecordingWithOutcome(benchmark::State& state) {
            auto sub = std::make_shared<RecordingSubscriber>();
            auto prev = globalSubscriber();
            setGlobalSubscriber(sub);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                Span span("bench.op");
                span.setAttribute({"phase", "write"});
                span.setAttribute({"bytes_out", 128U});
                span.setAttribute({"attempt", 1});
                span.addEvent("flushed", {{"bytes", 128}});
                span.setStatus(SpanStatus::Ok);
                benchmark::DoNotOptimize(span.spanId());
            }
            setGlobalSubscriber(prev);
            state.SetItemsProcessed(state.iterations());
            state.counters["spans"] = static_cast<double>(sub->spans().size());
        }

        BENCHMARK(bmSpanRecordingWithOutcome);

    } // namespace
} // namespace cinder
