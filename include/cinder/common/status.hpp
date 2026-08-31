#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
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

    explicit Error(Errc code, std::string_view message = {},
        std::source_location loc = std::source_location::current())
        : code_(code),
          location_(loc) {
        auto n = std::min(message.size(), msg_buf_.size() - 1);
        if (n > 0) {
            std::memcpy(msg_buf_.data(), message.data(), n);
        }
        msg_len_ = static_cast<uint8_t>(n);
    }

    Error(const Error& other)
        : code_(other.code_),
          msg_buf_(other.msg_buf_),
          msg_len_(other.msg_len_),
          location_(other.location_) {
        if (other.cause_) {
            cause_ = std::make_unique<Error>(*other.cause_);
        }
    }

    auto operator=(const Error& other) -> Error& {
        if (this != &other) {
            code_ = other.code_;
            msg_buf_ = other.msg_buf_;
            msg_len_ = other.msg_len_;
            location_ = other.location_;
            cause_ = other.cause_ ? std::make_unique<Error>(*other.cause_) : nullptr;
        }
        return *this;
    }

    Error(Error&&) noexcept = default;
    auto operator=(Error&&) noexcept -> Error& = default;
    ~Error() = default;

    [[nodiscard]] auto code() const noexcept -> Errc { return code_; }

    [[nodiscard]] auto message() const noexcept -> std::string_view {
        return {msg_buf_.data(), msg_len_};
    }

    [[nodiscard]] auto location() const noexcept -> const std::source_location& {
        return location_;
    }

    // Chain error provenance: wrap this error as the cause of a new one.
    [[nodiscard]] auto wrap(Errc code, std::string_view message = {},
        std::source_location loc = std::source_location::current()) const -> Error {
        Error e(code, message, loc);
        e.cause_ = std::make_unique<Error>(*this);
        return e;
    }

    [[nodiscard]] auto cause() const noexcept -> const Error* { return cause_.get(); }

  private:

    Errc code_;
    std::array<char, 48> msg_buf_{};
    uint8_t msg_len_ = 0;
    std::source_location location_;
    std::unique_ptr<Error> cause_;
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
