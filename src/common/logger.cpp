#include "cinder/common/logger.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace cinder {

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
Logger::init(std::string_view name, LogLevel level, LogSink sink) {
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
Logger::shutdown() {
    spdlog::shutdown();
}

void
Logger::emit(LogLevel level, std::string_view msg) {
    switch (level) {
        case LogLevel::Trace:
            spdlog::trace("{}", msg);
            break;
        case LogLevel::Debug:
            spdlog::debug("{}", msg);
            break;
        case LogLevel::Info:
            spdlog::info("{}", msg);
            break;
        case LogLevel::Warn:
            spdlog::warn("{}", msg);
            break;
        case LogLevel::Error:
            spdlog::error("{}", msg);
            break;
    }
}
} // namespace cinder
