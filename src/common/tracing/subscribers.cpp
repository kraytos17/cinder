#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "cinder/common/tracing.hpp"

using std::chrono::steady_clock;

namespace cinder {

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

} // namespace cinder
