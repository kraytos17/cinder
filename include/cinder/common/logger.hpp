#pragma once

#include <cstdint>
#include <string_view>

namespace cinder {

enum class LogLevel : uint8_t {
    Trace,
    Debug,
    Info,
    Warn,
    Error
};

class Logger {
  public:

    static void init(std::string_view name = "cinder", LogLevel level = LogLevel::Info);
    static void shutdown();

    static void trace(std::string_view msg);
    static void debug(std::string_view msg);
    static void info(std::string_view msg);
    static void warn(std::string_view msg);
    static void error(std::string_view msg);

    Logger() = delete;
};
} // namespace cinder
