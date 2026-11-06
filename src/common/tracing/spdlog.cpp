#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <format>
#include <memory>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <string>
#include <string_view>

#include "cinder/common/tracing.hpp"

namespace cinder {

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

    void Event::dispatchEvent(
        const Metadata& meta,
        std::string_view msg,
        std::initializer_list<FieldValue> fields
    ) {
        // Macro path: the callsite was already interest-checked (cached), so
        // route directly. A subscriber swap racing this load is benign — the
        // event simply goes to whichever backend is current.
        const TraceContext ctx = currentTraceContext();
        if (auto sub = cachedSubscriber(); sub != nullptr) {
            EventRecord record{
                .metadata = meta,
                .message = msg,
                .fields = fields,
                .context = ctx,
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
            ctx.trace_id,
            ctx.span_id,
            ctx.operation
        );
        logToSpdlog(meta.level, std::string_view(buf.data(), len));
    }

} // namespace cinder
