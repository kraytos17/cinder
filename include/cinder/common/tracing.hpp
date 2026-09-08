#pragma once

#include <cstdint>
#include <source_location>
#include <string_view>

namespace cinder {

enum class LogLevel : uint8_t {
    Trace,
    Debug,
    Info,
    Warn,
    Error
};

enum class LogSink : uint8_t {
    Stdout, // default — server processes
    Stderr  // CLI tools — errors go where the test harness expects them
};

// Fast PRNG for generating unique span-ids. Seed with trace-id for
// deterministic per-request span generation.
inline auto
splitmix64(uint64_t& state) -> uint64_t {
    state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31U);
}

// RAII span that sets a thread-local trace context. While alive, all Event
// calls on this thread include [t=<trace>.<span>] [<operation>] in the output.
// Nested spans save/restore the previous context. Span-ids are generated
// via splitmix64 for uniqueness even across concurrent requests.
class Span {
  public:

    // Create a new span. If parent_span_id == 0, generates a fresh trace-id.
    // Otherwise, inherits the trace-id from the parent.
    explicit Span(std::string_view operation, uint64_t parent_span_id = 0);
    ~Span();

    Span(const Span&) = delete;
    auto operator=(const Span&) -> Span& = delete;
    Span(Span&&) = delete;
    auto operator=(Span&&) -> Span& = delete;

    [[nodiscard]] auto traceId() const -> uint64_t { return trace_id_; }

    [[nodiscard]] auto spanId() const -> uint64_t { return span_id_; }

    [[nodiscard]] auto operation() const -> std::string_view { return operation_; }

  private:

    uint64_t prev_trace_id_;
    uint64_t prev_span_id_;
    uint64_t trace_id_;
    uint64_t span_id_;
    std::string_view operation_;
};

// Structured field for events. Key and value are string_views pointing to
// string literals (static storage duration). Do not use with temporary strings.
struct Field {
    std::string_view key;
    std::string_view value;
};

// Unified event-based tracing. Events carry structured key-value fields and
// inherit trace context from the current Span. Output format:
//   [t=trace.span|p=parent] [operation] message key=value
//
// Usage:
//   Event::info("request processed", {{"key", key}, {"status", "ok"}});
//   Event::warn("quorum failed", {{"acks", "1"}, {"needed", "2"}});
class Event {
  public:

    static void trace(std::string_view msg, std::initializer_list<Field> fields = {},
        std::source_location loc = std::source_location::current()) {
        emit(LogLevel::Trace, msg, fields, loc);
    }

    static void debug(std::string_view msg, std::initializer_list<Field> fields = {},
        std::source_location loc = std::source_location::current()) {
        emit(LogLevel::Debug, msg, fields, loc);
    }

    static void info(std::string_view msg, std::initializer_list<Field> fields = {},
        std::source_location loc = std::source_location::current()) {
        emit(LogLevel::Info, msg, fields, loc);
    }

    static void warn(std::string_view msg, std::initializer_list<Field> fields = {},
        std::source_location loc = std::source_location::current()) {
        emit(LogLevel::Warn, msg, fields, loc);
    }

    static void error(std::string_view msg, std::initializer_list<Field> fields = {},
        std::source_location loc = std::source_location::current()) {
        emit(LogLevel::Error, msg, fields, loc);
    }

    Event() = delete;

  private:

    static void emit(LogLevel level, std::string_view msg, std::initializer_list<Field> fields,
        std::source_location loc);
};

void
initLogger(std::string_view name = "cinder", LogLevel level = LogLevel::Debug,
    LogSink sink = LogSink::Stdout);

void
setLogLevel(LogLevel level);

void
shutdownLogger();
} // namespace cinder
