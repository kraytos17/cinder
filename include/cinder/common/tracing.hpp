#pragma once

#include <atomic>
#include <concepts>
#include <cstdint>
#include <memory>
#include <mutex>
#include <source_location>
#include <string_view>
#include <vector>

namespace cinder {

enum class LogLevel : uint8_t {
    Trace,
    Debug,
    Info,
    Warn,
    Error
};

// Compile-time minimum log level. Callsites below this generate zero code —
// field expressions are never even evaluated. Override per-build with
// -DCINDER_MIN_LOG_LEVEL=N (0=Trace … 4=Error). Default keeps everything.
#ifndef CINDER_MIN_LOG_LEVEL
#define CINDER_MIN_LOG_LEVEL 0
#endif

inline constexpr LogLevel K_MIN_LOG_LEVEL = static_cast<LogLevel>(CINDER_MIN_LOG_LEVEL);

enum class LogSink : uint8_t {
    Stdout,
    Stderr
};

// Subscriber interest in a callsite (tokio-tracing-inspired). Returned by
// Subscriber::registerCallsite: Always/Sometimes mean the subscriber wants
// notifications from this callsite, Never means skip them entirely.
enum class Interest : uint8_t {
    Always,
    Sometimes,
    Never
};

// Static per-callsite metadata: what operation and where it comes from.
// For spans the target is the span operation; for events it is the enclosing
// function. file/line come from std::source_location at the call site.
struct Metadata {
    std::string_view target;
    LogLevel level;
    std::string_view file;
    uint32_t line;
};

// Per-callsite cached interest, one static instance per CINDER_* expansion
// site (see macros below). generation 0 means unregistered; the global
// generation bumps on every setGlobalSubscriber/resetGlobalSubscriber call,
// transparently invalidating all caches.
struct Callsite {
    Metadata metadata;
    std::atomic<uint64_t> generation{0};
    std::atomic<Interest> interest{Interest::Never};
};

// True when the callsite should emit: consults (and caches) the installed
// subscriber's interest, honoring generation invalidation. No subscriber →
// true (the direct path relies on spdlog level filtering instead).
auto
isCallsiteEnabled(Callsite& callsite) -> bool;

// Thread-local trace context for structured logging. Set by the innermost
// live Span; read by Event::emit to enrich log lines.
struct TraceContext {
    uint64_t trace_id = 0;
    uint64_t span_id = 0;
    std::string_view operation;
};

// Current thread's trace context (a copy; the operation view stays valid
// while its Span is alive).
auto
currentTraceContext() -> TraceContext;

class Subscriber;

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
//
// When a global Subscriber is installed (see setGlobalSubscriber), the span
// additionally notifies it: registerCallsite → newSpan → enter on
// construction, exit → tryClose on destruction. The subscriber assigns the span id;
// without one the splitmix64 id is used.
class Span {
  public:

    // Create a new span. If parent_span_id == 0, generates a fresh trace-id.
    // Otherwise, inherits the trace-id from the parent.
    explicit Span(std::string_view operation, uint64_t parent_span_id = 0,
        std::source_location loc = std::source_location::current());
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
    // Subscriber notified at construction (null when none installed or when
    // it reported Interest::Never for this callsite).
    std::shared_ptr<Subscriber> subscriber_;
    uint64_t sub_span_id_ = 0;
    bool sub_notified_ = false;
};

// Typed structured field for events (tokio-tracing-inspired). Integers and
// bools are stored by value and formatted directly into the emit buffer with
// std::to_chars — no std::to_string heap allocation at the call site.
// Strings are borrowed as string_views; temporaries
// live until the end of the full expression,
// which covers the synchronous emit() call.
//
// Usage:
//   CINDER_INFO("ring rebuilt", {"alive", alive_count}, {"removed", removed_count});
//   CINDER_DEBUG("retry coalesced", {"pending", is_pending});
struct FieldValue {
    enum class Type : uint8_t {
        Str,
        Int,
        UInt,
        Bool
    };

    std::string_view key;
    Type type;

    union {
        int64_t int_val;
        uint64_t uint_val;
        bool bool_val;
        std::string_view str;
    };

    template <std::signed_integral T>
    constexpr FieldValue(std::string_view k, T v) noexcept
        : key(k),
          type(Type::Int),
          int_val(static_cast<int64_t>(v)) {}

    template <std::unsigned_integral T>
        requires(!std::same_as<T, bool>)
    constexpr FieldValue(std::string_view k, T v) noexcept
        : key(k),
          type(Type::UInt),
          uint_val(static_cast<uint64_t>(v)) {}

    constexpr FieldValue(std::string_view k, bool v) noexcept
        : key(k),
          type(Type::Bool),
          bool_val(v) {}

    // Backward compatibility for explicit Field constructions.
    constexpr FieldValue(std::string_view k, std::string_view v) noexcept
        : key(k),
          type(Type::Str),
          str{v} {}

    // String literals and const char* bind here (exact match), NOT to the
    // bool overload — without this, {"key", "literal"} would log as key=1.
    constexpr FieldValue(std::string_view k, const char* v) noexcept
        : key(k),
          type(Type::Str),
          str{v != nullptr ? v : ""} {}

    [[nodiscard]] constexpr auto strView() const noexcept -> std::string_view { return str; }
};

// A single event delivered to a Subscriber: callsite metadata + message +
// typed fields + the trace context active when the event was emitted.
// The field views borrow from the caller's temporaries and are only valid
// for the synchronous event() call
struct EventRecord {
    Metadata metadata;
    std::string_view message;
    std::initializer_list<FieldValue> fields;
    TraceContext context;
};

// Pluggable trace backend (tokio-tracing-inspired Subscriber). Implementors
// observe span lifecycles and events; LayeredSubscriber composes them.
// Lifecycle and event methods are noexcept: tracing must never throw
// into the instrumented code, so fallible backends handle errors internally.
// registerCallsite is exempt (filtering may consult fallible configuration)
// but should stay cheap — without macro-based callsite caching it runs on
// every span construction.
class Subscriber {
  public:

    virtual ~Subscriber() = default;

    Subscriber() = default;
    Subscriber(const Subscriber&) = delete;
    auto operator=(const Subscriber&) -> Subscriber& = delete;
    Subscriber(Subscriber&&) = delete;
    auto operator=(Subscriber&&) -> Subscriber& = delete;

    // Called when a span/event callsite is first seen (and, without
    // macro-based callsite caching, on every construction). Return Never to
    // skip all further notifications from this callsite.
    virtual auto registerCallsite(const Metadata& meta) -> Interest = 0;

    // A span was created. Returns the span id the subscriber assigns (the
    // Span adopts it). parent_span_id is 0 for root spans. Under
    // LayeredSubscriber the layered id is canonical and forwarded to every
    // layer — per-layer return values are ignored there, so layers must not
    // rely on their own return under composition.
    virtual auto newSpan(const Metadata& meta, uint64_t parent_trace_id,
        uint64_t parent_span_id) noexcept -> uint64_t = 0;

    // The span was entered (construction) / exited (destruction).
    virtual void enter(uint64_t span_id) noexcept = 0;
    virtual void exit(uint64_t span_id) noexcept = 0;

    // The span guard was destroyed and will never be entered again
    // (guards are not cloneable, so every exit is final). Layers use this
    // to flush per-span data and compute durations.
    virtual void tryClose(uint64_t span_id) noexcept = 0;

    // An event occurred. Must not call Event methods (no reentrancy) —
    // format with the record's fields directly instead.
    virtual void event(const EventRecord& record) noexcept = 0;
};

// Default backend: preserves the historical behavior (spdlog output with
// [t=trace.span] [operation] message key=value formatting). Lifecycle
// notifications are accepted but otherwise ignored — span timing and
// aggregation belong in dedicated layers.
class SpdlogSubscriber : public Subscriber {
  public:

    SpdlogSubscriber() = default;

    auto registerCallsite(const Metadata& meta) -> Interest override;
    auto newSpan(const Metadata& meta, uint64_t parent_trace_id, uint64_t parent_span_id) noexcept
        -> uint64_t override;
    void enter(uint64_t span_id) noexcept override;
    void exit(uint64_t span_id) noexcept override;
    void tryClose(uint64_t span_id) noexcept override;
    void event(const EventRecord& record) noexcept override;

  private:

    std::atomic<uint64_t> next_span_id_{1};
};

// Composes multiple subscribers into one (tokio-tracing-inspired Layer).
// Notifications fan out to every layer in registration order. Span ids are
// canonical: the LayeredSubscriber assigns one id per span and forwards it
// to all layers (per-layer newSpan returns are ignored). Interest combines
// permissively — Never only when every layer says Never — so each layer
// still filters for itself. Configure with addLayer before installing
// globally; mutation afterwards races with dispatch.
class LayeredSubscriber : public Subscriber {
  public:

    LayeredSubscriber() = default;

    void addLayer(std::shared_ptr<Subscriber> layer);

    auto registerCallsite(const Metadata& meta) -> Interest override;
    auto newSpan(const Metadata& meta, uint64_t parent_trace_id, uint64_t parent_span_id) noexcept
        -> uint64_t override;

    void enter(uint64_t span_id) noexcept override;
    void exit(uint64_t span_id) noexcept override;
    void tryClose(uint64_t span_id) noexcept override;
    void event(const EventRecord& record) noexcept override;

  private:

    std::mutex mutex_;
    std::vector<std::shared_ptr<Subscriber>> layers_;
    std::atomic<uint64_t> next_span_id_{1};
};

// Drops everything below a minimum level before it reaches the wrapped
// subscriber: registerCallsite reports Never for lower-level callsites (so
// the framework skips them without formatting) and event() double-checks
// the record level (a sibling layer may have kept the callsite enabled).
class LevelFilter : public Subscriber {
  public:

    explicit LevelFilter(LogLevel min_level, std::shared_ptr<Subscriber> inner);

    auto registerCallsite(const Metadata& meta) -> Interest override;
    auto newSpan(const Metadata& meta, uint64_t parent_trace_id, uint64_t parent_span_id) noexcept
        -> uint64_t override;
    void enter(uint64_t span_id) noexcept override;
    void exit(uint64_t span_id) noexcept override;
    void tryClose(uint64_t span_id) noexcept override;
    void event(const EventRecord& record) noexcept override;

  private:

    LogLevel min_level_;
    std::shared_ptr<Subscriber> inner_;
};

// Install (or replace) the process-wide subscriber. Thread-safe; spans and
// events observe the subscriber installed at the time they execute. Null by
// default, in which case spans manage context only and events go to spdlog.
void
setGlobalSubscriber(std::shared_ptr<Subscriber> sub);

// Remove the global subscriber (equivalent to setGlobalSubscriber(nullptr)).
void
resetGlobalSubscriber();

// The currently installed subscriber, or null when none is set.
auto
globalSubscriber() -> std::shared_ptr<Subscriber>;

// Unified event-based tracing. Events are emitted via the CINDER_* macros,
// which gate on cached callsite interest and dispatch here. Output format:
//   [t=trace.span|p=parent] [operation] message key=value
//
// Usage:
//   CINDER_INFO("request processed", {"key", key}, {"status", "ok"});
//   CINDER_WARN("quorum failed", {"acks", acks}, {"needed", needed});
class Event {
  public:

    Event() = delete;

    // Dispatch an already-gated event (macro path): routes via the installed
    // subscriber when present, else formats directly to spdlog. Subscribers
    // must not call this (reentrancy) — same rule as event().
    static void dispatchEvent(
        const Metadata& meta, std::string_view msg, std::initializer_list<FieldValue> fields);
};

void
initLogger(std::string_view name = "cinder", LogLevel level = LogLevel::Info,
    LogSink sink = LogSink::Stdout);

void
setLogLevel(LogLevel level);

void
shutdownLogger();
} // namespace cinder

// Structured event macros (tokio-tracing-inspired callsite model). Each
// expansion site owns a static Callsite: first execution registers interest
// with the installed subscriber and caches it, so disabled callsites cost
// two atomic loads and never evaluate field expressions. Callsites below
// K_MIN_LOG_LEVEL compile to nothing.
//
// Usage:
//   CINDER_INFO("ring rebuilt", {"alive", alive_count}, {"removed", removed_count});
//   CINDER_DEBUG("retry coalesced", {"pending", is_pending});
//   CINDER_WARN("no peers");
// Messages containing a top-level comma must be parenthesized
// (CINDER_WARN(("a, b"))) — the preprocessor splits macro args on commas
// without regard for string literals.
//
// Safe in headers for inline/template code (the static callsite merges
// across TUs); prefer .cpp sites otherwise to avoid duplicate caches.
#define CINDER_DETAIL_LOG(level, msg, ...)                                                         \
    do {                                                                                           \
        if constexpr (::cinder::LogLevel::level >= ::cinder::K_MIN_LOG_LEVEL) {                    \
            static ::cinder::Callsite s_callsite{                                                  \
                ::cinder::Metadata{__func__, ::cinder::LogLevel::level, __FILE__, __LINE__}};      \
            if (::cinder::isCallsiteEnabled(s_callsite)) {                                         \
                ::cinder::Event::dispatchEvent(s_callsite.metadata, msg, {__VA_ARGS__});           \
            }                                                                                      \
        }                                                                                          \
    } while (0)

#define CINDER_TRACE(msg, ...) CINDER_DETAIL_LOG(Trace, msg, __VA_ARGS__)
#define CINDER_DEBUG(msg, ...) CINDER_DETAIL_LOG(Debug, msg, __VA_ARGS__)
#define CINDER_INFO(msg, ...) CINDER_DETAIL_LOG(Info, msg, __VA_ARGS__)
#define CINDER_WARN(msg, ...) CINDER_DETAIL_LOG(Warn, msg, __VA_ARGS__)
#define CINDER_ERROR(msg, ...) CINDER_DETAIL_LOG(Error, msg, __VA_ARGS__)
