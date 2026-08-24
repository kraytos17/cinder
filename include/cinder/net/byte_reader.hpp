#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

#include "cinder/common/status.hpp"

namespace cinder::net {

// Sequential reader from a byte span. All reads return Result<T> for safe
// error handling on adversarial input. The caller must validate the frame
// header (size, magic, version, payload_len) before calling read methods.
class ByteReader {
  public:

    explicit ByteReader(std::span<const std::byte> data)
        : data_(data) {}

    template <typename T>
    auto read() -> Result<T> {
        if (off_ + sizeof(T) > data_.size()) {
            return err<T>(Error(Errc::InvalidArgument, "truncated"));
        }
        T val{};
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        std::memcpy(&val, &data_[off_], sizeof(T));
#else
        std::memcpy(&val, &data_[off_], sizeof(T));
        val = std::byteswap(val);
#endif
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
        if (off_ + n > data_.size()) {
            return err<std::span<const std::byte>>(Error(Errc::InvalidArgument, "truncated"));
        }
        auto result = data_.subspan(off_, n);
        off_ += n;
        return result;
    }

    auto readString(size_t len) -> Result<std::string> {
        auto bytes = readBytes(len);
        if (!bytes) {
            return err<std::string>(bytes.error());
        }
        return std::string(reinterpret_cast<const char*>(bytes->data()), len);
    }

    [[nodiscard]] auto offset() const -> size_t { return off_; }
    [[nodiscard]] auto remaining() const -> size_t { return data_.size() - off_; }

  private:

    std::span<const std::byte> data_;
    size_t off_ = 0;
};
} // namespace cinder::net
