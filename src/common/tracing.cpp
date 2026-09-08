#include "cinder/common/tracing.hpp"

#include <chrono>
#include <cstdint>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

using std::chrono::steady_clock;

namespace cinder {

// Thread-local trace context for structured logging.
struct TraceContext {
    uint64_t trace_id = 0;
    uint64_t span_id = 0;
    std::string_view operation;
};

static thread_local TraceContext t_trace_ctx{};

Span::Span(std::string_view operation, uint64_t parent_span_id)
    : prev_trace_id_(t_trace_ctx.trace_id),
      prev_span_id_(t_trace_ctx.span_id),
      span_id_(0),
      operation_(operation) {
    if (parent_span_id == 0) {
        // Fresh trace: generate a unique trace-id from an atomic counter.
        static std::atomic<uint64_t> s_next_trace{1};
        trace_id_ = s_next_trace.fetch_add(1, std::memory_order_relaxed);
    } else {
        // Child span: inherit trace-id from parent.
        trace_id_ = t_trace_ctx.trace_id;
    }

    // Generate a unique span-id via splitmix64.
    static thread_local uint64_t t_rng_state = 0;
    if (t_rng_state == 0) {
        t_rng_state = static_cast<uint64_t>(steady_clock::now().time_since_epoch().count());
    }

    span_id_ = splitmix64(t_rng_state);
    // Set thread-local context for all Event calls in this scope.
    t_trace_ctx.trace_id = trace_id_;
    t_trace_ctx.span_id = span_id_;
    t_trace_ctx.operation = operation_;
}

Span::~Span() {
    // Restore previous context.
    t_trace_ctx.trace_id = prev_trace_id_;
    t_trace_ctx.span_id = prev_span_id_;
    t_trace_ctx.operation = {};
}

static auto
toSpdlogLevel(LogLevel level) -> spdlog::level::level_enum {
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

void
initLogger(std::string_view name, LogLevel level, LogSink sink) {
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
    spdlog::set_default_logger(logger);
}

void
setLogLevel(LogLevel level) {
    spdlog::default_logger()->set_level(toSpdlogLevel(level));
}

void
shutdownLogger() {
    spdlog::shutdown();
}

void
Event::emit(LogLevel level, std::string_view msg, std::initializer_list<Field> fields,
    std::source_location /*loc*/) {
    constexpr size_t K_BUF_SIZE = 256;
    std::array<char, K_BUF_SIZE> buf{};

    size_t len = 0;
    auto [tid, span_id] = std::pair{t_trace_ctx.trace_id, t_trace_ctx.span_id};
    if (tid != 0 || span_id != 0) {
        auto res = std::format_to_n(buf.data() + len,
            static_cast<long>(K_BUF_SIZE - len),
            "[t={:08x}.{:08x}]",
            tid,
            span_id);
        len += static_cast<size_t>(res.size);
    }
    if (!t_trace_ctx.operation.empty()) {
        auto res = std::format_to_n(
            buf.data() + len, static_cast<long>(K_BUF_SIZE - len), " [{}]", t_trace_ctx.operation);
        len += static_cast<size_t>(res.size);
    }
    if (len > 0) {
        buf[len++] = ' ';
    }

    size_t msg_len = std::min(msg.size(), K_BUF_SIZE - len - 1);
    std::memcpy(buf.data() + len, msg.data(), msg_len);
    len += msg_len;

    // Append structured fields: key=value
    for (const auto& field : fields) {
        if (len + field.key.size() + field.value.size() + 3 >= K_BUF_SIZE) {
            break; // Don't overflow buffer
        }

        buf[len++] = ' ';
        std::memcpy(buf.data() + len, field.key.data(), field.key.size());
        len += field.key.size();
        buf[len++] = '=';
        std::memcpy(buf.data() + len, field.value.data(), field.value.size());
        len += field.value.size();
    }

    buf[len] = '\0';
    std::string_view output(buf.data(), len);
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
} // namespace cinder
