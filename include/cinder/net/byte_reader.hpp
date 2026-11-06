#pragma once

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <string_view>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder::net {

    // Sequential reader from a byte span. All reads return Result<T> for safe
    // error handling on adversarial input. The caller must validate the frame
    // header (size, magic, version, payload_len) before calling read methods.
    class ByteReader {
    public:

        explicit ByteReader(std::span<const std::byte> data) :
            data_(data) {}

        template <std::integral T> auto read() -> Result<T> {
            if (sizeof(T) > remaining()) {
                return err<T>(Error(Errc::InvalidArgument, "truncated"));
            }

            T val{};
            std::memcpy(&val, &data_[off_], sizeof(T));
            // Wire order is big-endian; byteswap only where the host differs.
            if constexpr (std::endian::native == std::endian::little) {
                val = std::byteswap(val);
            }

            off_ += sizeof(T);
            return val;
        }

        auto readByte() -> Result<uint8_t> {
            if (off_ >= data_.size()) {
                return err<uint8_t>(Error(Errc::InvalidArgument, "truncated"));
            }
            return std::to_integer<uint8_t>(data_[off_++]);
        }

        auto readBytes(size_t n) -> Result<std::span<const std::byte>> {
            if (n > remaining()) {
                return err<std::span<const std::byte>>(Error(Errc::InvalidArgument, "truncated"));
            }

            auto result = data_.subspan(off_, n);
            off_ += n;
            return result;
        }

        auto readString(size_t len) -> Result<std::string> {
            auto view = readStringView(len);
            if (!view) {
                return err<std::string>(view.error());
            }
            return std::string(*view);
        }

        auto readStringView(size_t len) -> Result<std::string_view> {
            auto bytes = readBytes(len);
            if (!bytes) {
                return err<std::string_view>(bytes.error());
            }
            return asChars(*bytes);
        }

        [[nodiscard]]
        auto offset() const -> size_t {
            return off_;
        }

        [[nodiscard]]
        auto remaining() const -> size_t {
            return off_ <= data_.size() ? data_.size() - off_ : 0;
        }

    private:

        std::span<const std::byte> data_;
        size_t off_ = 0;
    };
} // namespace cinder::net
