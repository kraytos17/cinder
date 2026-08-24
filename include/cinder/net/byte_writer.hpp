#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace cinder::net {

// Sequential writer into a byte buffer. All methods are void — the caller
// must pre-size the buffer to exactly the number of bytes that will be
// written (encode path guarantees this). Debug builds assert on overflow.
// Zero-cost when inlined; no Result overhead on the hot path.
class ByteWriter {
  public:

    explicit ByteWriter(std::vector<std::byte>& out)
        : out_(out) {}

    template <typename T>
    void write(T val) {
        assert(off_ + sizeof(T) <= out_.size());
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        std::memcpy(&out_[off_], &val, sizeof(T));
#else
        auto net = std::byteswap(val);
        std::memcpy(&out_[off_], &net, sizeof(T));
#endif
        off_ += sizeof(T);
    }

    void writeByte(uint8_t val) {
        assert(off_ < out_.size());
        out_[off_++] = std::byte{val};
    }

    void writeBytes(std::span<const std::byte> data) {
        assert(off_ + data.size() <= out_.size());
        std::memcpy(&out_[off_], data.data(), data.size());
        off_ += data.size();
    }

    void writeString(std::string_view s) {
        writeBytes(std::span<const std::byte>(
            reinterpret_cast<const std::byte*>(s.data()), s.size()));
    }

    [[nodiscard]] auto offset() const -> size_t { return off_; }

  private:

    std::vector<std::byte>& out_;
    size_t off_ = 0;
};
} // namespace cinder::net
