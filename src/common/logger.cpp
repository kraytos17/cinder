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
Logger::init(std::string_view name, LogLevel level) {
    auto logger = spdlog::stdout_color_mt(std::string(name));
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");
    logger->set_level(toSpdlogLevel(level));
    spdlog::set_default_logger(logger);
}

void
Logger::shutdown() {
    spdlog::shutdown();
}

void
Logger::trace(std::string_view msg) {
    spdlog::trace("{}", msg);
}

void
Logger::debug(std::string_view msg) {
    spdlog::debug("{}", msg);
}

void
Logger::info(std::string_view msg) {
    spdlog::info("{}", msg);
}

void
Logger::warn(std::string_view msg) {
    spdlog::warn("{}", msg);
}

void
Logger::error(std::string_view msg) {
    spdlog::error("{}", msg);
}
} // namespace cinder
