#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <expected>
#include <source_location>
#include <string_view>
#include <utility>

namespace cinder {

enum class Errc : uint8_t {
    OK,
    NotFound,
    CapacityExceeded,
    InvalidArgument,
    TtlExpired,
    NotSupported,
    InternalError,
    Timeout,
    NotReady,
};

class Error {
  public:

    // For compile-time string literals — zero heap allocation.
    explicit Error(Errc code, std::string_view message = {},
        std::source_location loc = std::source_location::current())
        : code_(code),
          location_(loc) {
        auto n = std::min(message.size(), msg_buf_.size() - 1);
        std::memcpy(msg_buf_.data(), message.data(), n);
        msg_len_ = static_cast<uint8_t>(n);
    }

    [[nodiscard]] auto code() const noexcept -> Errc { return code_; }

    [[nodiscard]] auto message() const noexcept -> std::string_view {
        return {msg_buf_.data(), msg_len_};
    }

    [[nodiscard]] auto location() const noexcept -> const std::source_location& {
        return location_;
    }

  private:

    Errc code_;
    std::array<char, 48> msg_buf_{};
    uint8_t msg_len_ = 0;
    std::source_location location_;
};

template <typename T> using Result = std::expected<T, Error>;

[[nodiscard]] inline constexpr auto
toString(Errc code) -> std::string_view {
    switch (code) {
        case Errc::OK:
            return "OK";
        case Errc::NotFound:
            return "(not found)";
        case Errc::CapacityExceeded:
            return "(capacity exceeded)";
        case Errc::InvalidArgument:
            return "(invalid argument)";
        case Errc::TtlExpired:
            return "(ttl expired)";
        case Errc::NotSupported:
            return "(not supported)";
        case Errc::InternalError:
            return "(internal error)";
        case Errc::Timeout:
            return "(timeout)";
        case Errc::NotReady:
            return "(not ready)";
    }
    return "(unknown)";
}

template <typename T>
auto
ok(T value) -> Result<T> {
    return std::move(value);
}

inline auto
ok() -> Result<void> {
    return {};
}

template <typename T>
auto
err(Error error) -> Result<T> {
    return std::unexpected(error);
}

inline auto
err(Error error) -> Result<void> {
    return std::unexpected(error);
}
} // namespace cinder
