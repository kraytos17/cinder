#include "cinder/common/tracing.hpp"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <source_location>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

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

        // Runs fallible recording work; faults are swallowed at the tracing
        // boundary (subscriber callbacks are noexcept, so an allocation
        // failure must not escape into instrumented code).
        template <typename Fn> void recordQuietly(Fn&& fn) noexcept {
            try {
                fn();
            } catch (...) { // NOLINT(bugprone-empty-catch)
                // Deliberate: the span keeps its id, recording misses one record.
            }
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

    static auto toSpdlogLevel(LogLevel level) -> spdlog::level {
        switch (level) {
            case LogLevel::Trace:
                return spdlog::level::trace;
            case LogLevel::Debug:
                return spdlog::level::debug;
            case LogLevel::Info:
                return spdlog::level::info;
            case LogLevel::Warn:
                return spdlog::level::warn;
            case LogLevel::Error:
                return spdlog::level::err;
        }
        return spdlog::level::info;
    }

    void initLogger(std::string_view name, LogLevel level, LogSink sink) {
        spdlog::sink_ptr spd_sink;
        if (sink == LogSink::Stderr) {
            spd_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
        } else {
            spd_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        }

        spd_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");
        spd_sink->set_level(toSpdlogLevel(level));

        auto logger = std::make_shared<spdlog::logger>(std::string(name), spd_sink);
        logger->set_level(toSpdlogLevel(level));
        spdlog::set_global_logger(logger);
    }

    void setLogLevel(LogLevel level) {
        spdlog::global_logger()->set_level(toSpdlogLevel(level));
    }

    void shutdownLogger() {
        spdlog::shutdown();
    }

    // Format [t=trace.span] [operation] message key=value into buf (up to cap
    // bytes, always leaving room for a NUL). Returns the bytes written.
    // Shared by the direct path (Event::emit) and SpdlogSubscriber::event so
    // both render byte-identical output. Never allocates.
    static auto formatRecordInto(
        char* buf,
        size_t cap,
        std::string_view msg,
        std::initializer_list<FieldValue> fields,
        uint64_t trace_id,
        uint64_t span_id,
        std::string_view operation
    ) -> size_t {
        // Max chars for a 64-bit integer plus key/value separators.
        constexpr size_t K_INT_CHARS = 20;
        size_t len = 0;
        if (trace_id != 0 || span_id != 0) {
            auto res = std::format_to_n(
                buf + len,
                static_cast<long>(cap - len),
                "[t={:08x}.{:08x}]",
                trace_id,
                span_id
            );
            len += static_cast<size_t>(res.size);
        }
        if (!operation.empty()) {
            auto res =
                std::format_to_n(buf + len, static_cast<long>(cap - len), " [{}]", operation);
            len += static_cast<size_t>(res.size);
        }
        if (len > 0) {
            buf[len++] = ' ';
        }

        size_t msg_len = std::min(msg.size(), cap - len - 1);
        std::memcpy(buf + len, msg.data(), msg_len);
        len += msg_len;
        // Append structured fields: key=value. Integers and bools format
        // directly into the buffer via std::to_chars — no heap allocation.
        for (const auto& field : fields) {
            if (len + field.key.size() + K_INT_CHARS + 3 >= cap) {
                break; // Don't overflow buffer
            }

            buf[len++] = ' ';
            std::memcpy(buf + len, field.key.data(), field.key.size());
            len += field.key.size();
            buf[len++] = '=';
            switch (field.type) {
                case FieldValue::Type::Str: {
                    auto v = field.strView();
                    if (len + v.size() + 1 >= cap) {
                        break;
                    }

                    std::memcpy(buf + len, v.data(), v.size());
                    len += v.size();
                    break;
                }
                case FieldValue::Type::Int: {
                    auto res = std::to_chars(buf + len, buf + cap, field.int_val);
                    if (res.ec != std::errc{}) {
                        break;
                    }

                    len += static_cast<size_t>(res.ptr - (buf + len));
                    break;
                }
                case FieldValue::Type::UInt: {
                    auto res = std::to_chars(buf + len, buf + cap, field.uint_val);
                    if (res.ec != std::errc{}) {
                        break;
                    }

                    len += static_cast<size_t>(res.ptr - (buf + len));
                    break;
                }
                case FieldValue::Type::Bool:
                    buf[len++] = field.bool_val ? '1' : '0';
                    break;
            }
        }

        buf[len] = '\0';
        return len;
    }

    static void logToSpdlog(LogLevel level, std::string_view output) {
        switch (level) {
            case LogLevel::Trace:
                spdlog::trace("{}", output);
                break;
            case LogLevel::Debug:
                spdlog::debug("{}", output);
                break;
            case LogLevel::Info:
                spdlog::info("{}", output);
                break;
            case LogLevel::Warn:
                spdlog::warn("{}", output);
                break;
            case LogLevel::Error:
                spdlog::error("{}", output);
                break;
        }
    }

    auto SpdlogSubscriber::registerCallsite(const Metadata& /*meta*/) -> Interest {
        return Interest::Always;
    }

    auto SpdlogSubscriber::newSpan(
        const Metadata& /*meta*/,
        uint64_t /*parent_trace_id*/,
        uint64_t /*parent_span_id*/
    ) noexcept -> uint64_t {
        return next_span_id_.fetch_add(1, std::memory_order_relaxed);
    }

    void SpdlogSubscriber::enter(uint64_t /*span_id*/) noexcept {
        // No per-span state to track: timing and aggregation belong in layers.
    }

    void SpdlogSubscriber::exit(uint64_t /*span_id*/) noexcept {}

    void SpdlogSubscriber::tryClose(uint64_t /*span_id*/) noexcept {}

    void SpdlogSubscriber::event(const EventRecord& record) noexcept {
        // Same rendering as the direct path — must not call Event::emit here
        // (that would recurse back into this method).
        constexpr size_t K_BUF_SIZE = 256;
        std::array<char, K_BUF_SIZE> buf{};
        size_t len = formatRecordInto(
            buf.data(),
            K_BUF_SIZE,
            record.message,
            record.fields,
            record.context.trace_id,
            record.context.span_id,
            record.context.operation
        );
        logToSpdlog(record.metadata.level, std::string_view(buf.data(), len));
    }

    void LayeredSubscriber::addLayer(std::shared_ptr<Subscriber> layer) {
        std::lock_guard lock(mutex_);
        layers_.push_back(std::move(layer));
    }

    auto LayeredSubscriber::registerCallsite(const Metadata& meta) -> Interest {
        // Snapshot first: a layer consulting configuration may reenter (e.g.
        // emit events), which must not block on a held composition lock.
        std::vector<std::shared_ptr<Subscriber>> layers;
        {
            std::lock_guard lock(mutex_);
            layers = layers_;
        }
        // Permissive combination: skip only when every layer says Never, so
        // each layer still filters for itself in event(). Every layer observes
        // the call (layers may cache per-callsite state), so no early return.
        bool any_always = false;
        bool any_sometimes = false;
        for (const auto& layer : layers) {
            switch (layer->registerCallsite(meta)) {
                case Interest::Always:
                    any_always = true;
                    break;
                case Interest::Sometimes:
                    any_sometimes = true;
                    break;
                case Interest::Never:
                    break;
            }
        }
        if (any_always) {
            return Interest::Always;
        }
        return any_sometimes ? Interest::Sometimes : Interest::Never;
    }

    auto LayeredSubscriber::layersSnapshot() noexcept -> std::vector<std::shared_ptr<Subscriber>> {
        try {
            std::lock_guard lock(mutex_);
            return layers_;
        } catch (...) {
            return {};
        }
    }

    auto LayeredSubscriber::newSpan(
        const Metadata& meta,
        uint64_t parent_trace_id,
        uint64_t parent_span_id
    ) noexcept -> uint64_t {
        uint64_t id = next_span_id_.fetch_add(1, std::memory_order_relaxed);
        for (const auto& layer : layersSnapshot()) {
            layer->newSpan(meta, parent_trace_id, parent_span_id);
        }
        return id;
    }

    void LayeredSubscriber::enter(uint64_t span_id) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->enter(span_id);
        }
    }

    void LayeredSubscriber::exit(uint64_t span_id) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->exit(span_id);
        }
    }

    void LayeredSubscriber::tryClose(uint64_t span_id) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->tryClose(span_id);
        }
    }

    void LayeredSubscriber::setSpanStatus(uint64_t span_id, SpanStatus status) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->setSpanStatus(span_id, status);
        }
    }

    void LayeredSubscriber::spanAttribute(uint64_t span_id, const FieldValue& attr) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->spanAttribute(span_id, attr);
        }
    }

    void LayeredSubscriber::spanEvent(
        uint64_t span_id,
        std::string_view name,
        steady_clock::time_point at,
        std::initializer_list<FieldValue> fields
    ) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->spanEvent(span_id, name, at, fields);
        }
    }

    void LayeredSubscriber::event(const EventRecord& record) noexcept {
        for (const auto& layer : layersSnapshot()) {
            layer->event(record);
        }
    }

    LevelFilter::LevelFilter(LogLevel min_level, std::shared_ptr<Subscriber> inner) :
        min_level_(min_level),
        inner_(std::move(inner)) {}

    auto LevelFilter::registerCallsite(const Metadata& meta) -> Interest {
        if (meta.level < min_level_) {
            return Interest::Never;
        }
        return inner_->registerCallsite(meta);
    }

    auto LevelFilter::newSpan(
        const Metadata& meta,
        uint64_t parent_trace_id,
        uint64_t parent_span_id
    ) noexcept -> uint64_t {
        return inner_->newSpan(meta, parent_trace_id, parent_span_id);
    }

    void LevelFilter::enter(uint64_t span_id) noexcept {
        inner_->enter(span_id);
    }

    void LevelFilter::exit(uint64_t span_id) noexcept {
        inner_->exit(span_id);
    }

    void LevelFilter::tryClose(uint64_t span_id) noexcept {
        inner_->tryClose(span_id);
    }

    void LevelFilter::setSpanStatus(uint64_t span_id, SpanStatus status) noexcept {
        inner_->setSpanStatus(span_id, status);
    }

    void LevelFilter::spanAttribute(uint64_t span_id, const FieldValue& attr) noexcept {
        inner_->spanAttribute(span_id, attr);
    }

    void LevelFilter::spanEvent(
        uint64_t span_id,
        std::string_view name,
        steady_clock::time_point at,
        std::initializer_list<FieldValue> fields
    ) noexcept {
        inner_->spanEvent(span_id, name, at, fields);
    }

    void LevelFilter::event(const EventRecord& record) noexcept {
        // A sibling layer may have kept the callsite enabled, so re-check here.
        if (record.metadata.level < min_level_) {
            return;
        }
        inner_->event(record);
    }

    auto RecordingSubscriber::registerCallsite(const Metadata& /*meta*/) -> Interest {
        return Interest::Always;
    }

    auto RecordingSubscriber::newSpan(
        const Metadata& meta,
        uint64_t parent_trace_id,
        uint64_t parent_span_id
    ) noexcept -> uint64_t {
        uint64_t id = next_span_id_.fetch_add(1, std::memory_order_relaxed);
        recordQuietly([&] {
            std::scoped_lock lock(mutex_);
            RecordedSpan span;
            span.span_id = id;
            span.trace_id = parent_trace_id;
            span.parent_span_id = parent_span_id;
            span.operation = std::string(meta.target);
            span.started_at = steady_clock::now();
            spans_.push_back(std::move(span));
        });
        return id;
    }

    void RecordingSubscriber::enter(uint64_t /*span_id*/) noexcept {
        // Timing derives from newSpan/tryClose; no per-enter state to track.
    }

    void RecordingSubscriber::exit(uint64_t /*span_id*/) noexcept {}

    void RecordingSubscriber::tryClose(uint64_t span_id) noexcept {
        recordQuietly([&] {
            std::scoped_lock lock(mutex_);
            for (auto& span : spans_) {
                if (span.span_id == span_id && !span.closed) {
                    span.ended_at = steady_clock::now();
                    span.closed = true;
                    return;
                }
            }
        });
    }

    namespace {
        // Owned copy of a borrowed FieldValue. String data (key and value)
        // is copied; scalars by value. Callers wrap in recordQuietly.
        auto copyField(const FieldValue& f) -> RecordingSubscriber::RecordedField {
            RecordingSubscriber::RecordedField out;
            out.key = std::string(f.key);
            out.type = f.type;
            switch (f.type) {
                case FieldValue::Type::Str:
                    out.str_val = std::string(f.strView());
                    break;
                case FieldValue::Type::Int:
                    out.int_val = f.int_val;
                    break;
                case FieldValue::Type::UInt:
                    out.uint_val = f.uint_val;
                    break;
                case FieldValue::Type::Bool:
                    out.bool_val = f.bool_val;
                    break;
            }
            return out;
        }
    } // namespace

    void RecordingSubscriber::spanAttribute(uint64_t span_id, const FieldValue& attr) noexcept {
        recordQuietly([&] {
            std::scoped_lock lock(mutex_);
            for (auto& span : spans_) {
                if (span.span_id == span_id) {
                    auto copied = copyField(attr);
                    for (auto& existing : span.attributes) {
                        if (existing.key == copied.key) {
                            existing = std::move(copied);
                            return;
                        }
                    }

                    span.attributes.push_back(std::move(copied));
                    return;
                }
            }
        });
    }

    void RecordingSubscriber::setSpanStatus(uint64_t span_id, SpanStatus status) noexcept {
        recordQuietly([&] {
            std::scoped_lock lock(mutex_);
            for (auto& span : spans_) {
                if (span.span_id == span_id) {
                    span.status = status;
                    return;
                }
            }
        });
    }

    void RecordingSubscriber::spanEvent(
        uint64_t span_id,
        std::string_view name,
        steady_clock::time_point at,
        std::initializer_list<FieldValue> fields
    ) noexcept {
        recordQuietly([&] {
            RecordedEvent event;
            event.at = at;
            event.name = std::string(name);
            for (const auto& f : fields) {
                event.fields.push_back(copyField(f));
            }

            std::scoped_lock lock(mutex_);
            for (auto& span : spans_) {
                if (span.span_id == span_id) {
                    span.events.push_back(std::move(event));
                    return;
                }
            }
        });
    }

    void RecordingSubscriber::event(const EventRecord& record) noexcept {
        recordQuietly([&] {
            std::scoped_lock lock(mutex_);
            RecordedLog log;
            log.message = std::string(record.message);
            log.trace_id = record.context.trace_id;
            log.span_id = record.context.span_id;
            logs_.push_back(std::move(log));
        });
    }

    auto RecordingSubscriber::spans() const -> std::vector<RecordedSpan> {
        std::scoped_lock lock(mutex_);
        return spans_;
    }

    auto RecordingSubscriber::logEvents() const -> std::vector<RecordedLog> {
        std::scoped_lock lock(mutex_);
        return logs_;
    }

    auto RecordingSubscriber::find(uint64_t span_id) const -> std::optional<RecordedSpan> {
        std::scoped_lock lock(mutex_);
        for (const auto& span : spans_) {
            if (span.span_id == span_id) {
                return span;
            }
        }
        return std::nullopt;
    }

    void RecordingSubscriber::clear() {
        std::scoped_lock lock(mutex_);
        spans_.clear();
        logs_.clear();
    }

    void Event::dispatchEvent(
        const Metadata& meta,
        std::string_view msg,
        std::initializer_list<FieldValue> fields
    ) {
        // Macro path: the callsite was already interest-checked (cached), so
        // route directly. A subscriber swap racing this load is benign — the
        // event simply goes to whichever backend is current.
        if (auto sub = cachedSubscriber(); sub != nullptr) {
            EventRecord record{
                .metadata = meta,
                .message = msg,
                .fields = fields,
                .context = t_trace_ctx,
            };

            sub->event(record);
            return;
        }

        constexpr size_t K_BUF_SIZE = 256;
        std::array<char, K_BUF_SIZE> buf{};
        // The direct path relies on spdlog level filtering — but consult it
        // BEFORE formatting, so events the sink would drop cost a single
        // atomic load instead of a full format into a discarded buffer.
        if (!spdlog::global_logger()->should_log(toSpdlogLevel(meta.level))) {
            return;
        }

        size_t len = formatRecordInto(
            buf.data(),
            K_BUF_SIZE,
            msg,
            fields,
            t_trace_ctx.trace_id,
            t_trace_ctx.span_id,
            t_trace_ctx.operation
        );
        logToSpdlog(meta.level, std::string_view(buf.data(), len));
    }
} // namespace cinder
