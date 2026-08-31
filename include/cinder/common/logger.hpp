#pragma once

#include <cstdint>
#include <format>
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

class Logger {
  public:

    static void init(std::string_view name = "cinder", LogLevel level = LogLevel::Debug,
        LogSink sink = LogSink::Stdout);
    static void setLevel(LogLevel level);
    static void shutdown();

    template <typename... Args> static void trace(std::format_string<Args...> fmt, Args&&... args) {
        emit(LogLevel::Trace, std::vformat(fmt.get(), std::make_format_args(args...)));
    }

    template <typename... Args> static void debug(std::format_string<Args...> fmt, Args&&... args) {
        emit(LogLevel::Debug, std::vformat(fmt.get(), std::make_format_args(args...)));
    }

    template <typename... Args> static void info(std::format_string<Args...> fmt, Args&&... args) {
        emit(LogLevel::Info, std::vformat(fmt.get(), std::make_format_args(args...)));
    }

    template <typename... Args> static void warn(std::format_string<Args...> fmt, Args&&... args) {
        emit(LogLevel::Warn, std::vformat(fmt.get(), std::make_format_args(args...)));
    }

    template <typename... Args> static void error(std::format_string<Args...> fmt, Args&&... args) {
        emit(LogLevel::Error, std::vformat(fmt.get(), std::make_format_args(args...)));
    }

    Logger() = delete;

  private:

    static void emit(LogLevel level, std::string_view msg);
};
} // namespace cinder
