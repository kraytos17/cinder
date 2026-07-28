#pragma once

#include <cstdint>
#include <optional>
#include <string>
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

    explicit Error(Errc code, std::string message = {})
        : code_(code),
          message_(std::move(message)) {}

    [[nodiscard]] auto code() const noexcept -> Errc { return code_; }

    [[nodiscard]] auto message() const noexcept -> std::string_view { return message_; }

  private:

    Errc code_;
    std::string message_;
};

template <typename T> class Result {
  public:

    using value_type = T;

    Result(T value)
        : value_(std::move(value)) {}

    Result(Error error)
        : error_(std::move(error)) {}

    static auto ok(T value) -> Result { return Result(std::move(value)); }

    static auto err(Error error) -> Result { return Result(std::move(error)); }

    [[nodiscard]] auto has_value() const noexcept -> bool { return value_.has_value(); }

    auto value() & -> T& { return *value_; }

    auto value() const& -> const T& { return *value_; }

    auto value() && -> T { return std::move(*value_); }

    [[nodiscard]] auto error() const -> const Error& { return error_; }

    auto value_or(T fallback) const& -> T { return has_value() ? value() : fallback; }

    auto value_or(T fallback) && -> T { return has_value() ? std::move(*this).value() : fallback; }

  private:

    std::optional<T> value_;
    Error error_{Errc::OK, {}};
};

template <> class Result<void> {
  public:

    Result()
        : has_value_(true) {}

    Result(Error error)
        : has_value_(false),
          error_(std::move(error)) {}

    static auto ok() -> Result { return {}; }

    static auto err(Error error) -> Result { return {std::move(error)}; }

    [[nodiscard]] auto has_value() const noexcept -> bool { return has_value_; }

    [[nodiscard]] auto error() const -> const Error& { return error_; }

  private:

    bool has_value_;
    Error error_{Errc::OK, {}};
};

template <typename T>
auto
ok(T value) -> Result<T> {
    return Result<T>::ok(std::move(value));
}

inline auto
ok() -> Result<void> {
    return Result<void>::ok();
}

inline auto
err(Error error) -> Result<void> {
    return Result<void>::err(std::move(error));
}

template <typename T>
auto
err(Error error) -> Result<T> {
    return Result<T>::err(std::move(error));
}
} // namespace cinder
