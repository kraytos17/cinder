#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <source_location>

#include "cinder/common/tracing.hpp"

using std::chrono::steady_clock;

namespace cinder {

    static thread_local TraceContext t_trace_ctx{};

    // Process-wide subscriber (null = historical behavior: context-only spans,
    // events straight to spdlog). Atomic shared_ptr so install/observe is
    // race-free even while spans are being created on other threads.
    static std::atomic<std::shared_ptr<Subscriber>> g_subscriber{nullptr};

    // Bumped on every set/reset so cached callsite interests invalidate.
    // Starts at 1; generation 0 in a Callsite means "never registered".
    static std::atomic<uint64_t> g_interest_generation{1};

    auto currentTraceContext() -> TraceContext {
        return t_trace_ctx;
    }

    auto captureTraceContext() -> SpanContext {
        return {t_trace_ctx.trace_id, t_trace_ctx.span_id};
    }

    auto parseSpanContext(std::string_view trace_id_str, std::string_view span_id_str)
        -> SpanContext {
        // ASCII decimal only, no whitespace/sign handling: strict so a
        // truncated or binary value can never alias another trace.
        constexpr size_t K_MAX_DIGITS = 20; // uint64 max is 20 digits
        auto parse = [](std::string_view s) -> uint64_t {
            if (s.empty() || s.size() > K_MAX_DIGITS) {
                return 0;
            }

            uint64_t value = 0;
            const char* first = s.data();
            const char* last = s.data() + s.size();
            auto [ptr, ec] = std::from_chars(first, last, value);
            if (ec != std::errc{} || ptr != last) {
                return 0;
            }
            return value;
        };

        uint64_t trace_id = parse(trace_id_str);
        uint64_t span_id = parse(span_id_str);
        if (trace_id == 0 || span_id == 0) {
            return {};
        }
        return {trace_id, span_id};
    }

    TraceScope::TraceScope(SpanContext ctx) noexcept :
        prev_(t_trace_ctx) {
        t_trace_ctx.trace_id = ctx.trace_id;
        t_trace_ctx.span_id = ctx.span_id;
        t_trace_ctx.operation = {};
    }

    TraceScope::~TraceScope() {
        t_trace_ctx = prev_;
    }

    void setGlobalSubscriber(std::shared_ptr<Subscriber> sub) {
        g_subscriber.store(std::move(sub), std::memory_order_release);
        // Invalidate every cached callsite interest — the new subscriber gets
        // a fresh registerCallsite on next use.
        g_interest_generation.fetch_add(1, std::memory_order_relaxed);
    }

    void resetGlobalSubscriber() {
        g_subscriber.store(nullptr, std::memory_order_release);
        g_interest_generation.fetch_add(1, std::memory_order_relaxed);
    }

    auto globalSubscriber() -> std::shared_ptr<Subscriber> {
        return g_subscriber.load(std::memory_order_acquire);
    }

    // Fast path for the per-event subscriber lookup. g_subscriber is a
    // spinlock-backed atomic<shared_ptr> on libstdc++/libc++; loading it on
    // every event costs two lock round-trips. Cache it thread-locally and
    // revalidate against the interest generation (bumped on every
    // install/reset, so staleness is bounded by ~one event — the same benign
    // swap race the direct load already accepted).
    auto cachedSubscriber() -> std::shared_ptr<Subscriber> {
        thread_local std::shared_ptr<Subscriber> t_sub;
        thread_local uint64_t t_gen = 0; // generations start at 1; 0 forces first load
        uint64_t gen = g_interest_generation.load(std::memory_order_relaxed);
        if (gen != t_gen) {
            t_sub = g_subscriber.load(std::memory_order_acquire);
            t_gen = gen;
        }
        return t_sub;
    }

    auto isCallsiteEnabled(Callsite& callsite) -> bool {
        auto sub = cachedSubscriber();
        if (sub == nullptr) {
            return true; // direct path; spdlog level filtering applies instead
        }

        uint64_t gen = g_interest_generation.load(std::memory_order_acquire);
        if (callsite.generation.load(std::memory_order_acquire) != gen) {
            Interest interest = sub->registerCallsite(callsite.metadata);
            callsite.interest.store(interest, std::memory_order_relaxed);
            callsite.generation.store(gen, std::memory_order_release);
            return interest != Interest::Never;
        }

        Interest interest = callsite.interest.load(std::memory_order_acquire);
        if (interest == Interest::Sometimes) {
            // Re-consult every time; a Sometimes subscriber may change its mind.
            return sub->registerCallsite(callsite.metadata) != Interest::Never;
        }
        return interest != Interest::Never;
    }

    namespace {
        // Shared span prologue: mint a span id, install thread-local context
        // and notify the subscriber, which may assign the authoritative span
        // id. Returns the subscriber when notified so the caller can stash it
        // for exit/close; null otherwise.
        auto beginSpan(
            std::string_view operation,
            uint64_t trace_id,
            uint64_t parent_span_id,
            const std::source_location& loc,
            uint64_t& span_id_out
        ) -> std::shared_ptr<Subscriber> {
            // Generate a unique span-id via splitmix64.
            static thread_local uint64_t t_rng_state = 0;
            if (t_rng_state == 0) {
                t_rng_state = static_cast<uint64_t>(steady_clock::now().time_since_epoch().count());
            }

            span_id_out = splitmix64(t_rng_state);
            // Set thread-local context for all Event calls in this scope.
            t_trace_ctx.trace_id = trace_id;
            t_trace_ctx.span_id = span_id_out;
            t_trace_ctx.operation = operation;
            // Notify the installed subscriber, if any. It assigns the authoritative
            // span id, which the context adopts so events correlate with it.
            if (auto sub = g_subscriber.load(std::memory_order_acquire); sub != nullptr) {
                Metadata meta{
                    .target = operation,
                    .file = loc.file_name(),
                    .line = loc.line(),
                    .level = LogLevel::Trace,
                };
                if (sub->registerCallsite(meta) != Interest::Never) {
                    uint64_t sub_id = sub->newSpan(meta, trace_id, parent_span_id);
                    sub->enter(sub_id);
                    span_id_out = sub_id;
                    t_trace_ctx.span_id = span_id_out;
                    return sub;
                }
            }
            return nullptr;
        }

        auto freshTraceId() -> uint64_t {
            static std::atomic<uint64_t> s_next_trace{1};
            return s_next_trace.fetch_add(1, std::memory_order_relaxed);
        }

    } // namespace

    Span::Span(std::string_view operation, uint64_t parent_span_id, std::source_location loc) :
        prev_trace_id_(t_trace_ctx.trace_id),
        prev_span_id_(t_trace_ctx.span_id),
        span_id_(0),
        parent_span_id_(parent_span_id),
        operation_(operation) {
        if (parent_span_id == 0) {
            // Fresh trace: generate a unique trace-id from an atomic counter.
            trace_id_ = freshTraceId();
        } else {
            // Child span: inherit trace-id from parent. Only valid when the
            // parent is live on this thread; see the SpanContext overload for
            // remote/cross-thread parents.
            trace_id_ = t_trace_ctx.trace_id;
        }

        if (auto sub = beginSpan(operation, trace_id_, parent_span_id, loc, span_id_);
            sub != nullptr) {
            subscriber_ = std::move(sub);
            sub_notified_ = true;
            span_id_ = sub_span_id_ = t_trace_ctx.span_id;
        }
    }

    Span::Span(std::string_view operation, SpanContext parent, std::source_location loc) :
        prev_trace_id_(t_trace_ctx.trace_id),
        prev_span_id_(t_trace_ctx.span_id),
        trace_id_(parent.valid() ? parent.trace_id : freshTraceId()),
        span_id_(0),
        parent_span_id_(parent.span_id),
        operation_(operation) {
        if (auto sub = beginSpan(operation, trace_id_, parent_span_id_, loc, span_id_);
            sub != nullptr) {
            subscriber_ = std::move(sub);
            sub_notified_ = true;
            span_id_ = sub_span_id_ = t_trace_ctx.span_id;
        }
    }

    Span::~Span() {
        if (sub_notified_) {
            subscriber_->exit(sub_span_id_);
        }

        // Restore previous context.
        t_trace_ctx.trace_id = prev_trace_id_;
        t_trace_ctx.span_id = prev_span_id_;
        t_trace_ctx.operation = {};
        if (sub_notified_) {
            // Guards are not cloneable, so this exit is final.
            subscriber_->tryClose(sub_span_id_);
        }
    }

    void Span::setStatus(SpanStatus status) noexcept {
        status_ = status;
        if (sub_notified_) {
            subscriber_->setSpanStatus(sub_span_id_, status);
        }
    }

    void Span::addEvent(std::string_view name, std::initializer_list<FieldValue> fields) noexcept {
        if (sub_notified_) {
            subscriber_->spanEvent(sub_span_id_, name, steady_clock::now(), fields);
        }
    }

    void Span::setAttribute(const FieldValue& attr) noexcept {
        if (sub_notified_) {
            subscriber_->spanAttribute(sub_span_id_, attr);
        }
    }

} // namespace cinder
