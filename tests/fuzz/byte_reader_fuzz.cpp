#include <cassert>
#include <cstddef>
#include <cstdint>
#include <fuzzer/FuzzedDataProvider.h>
#include <string>
#include <vector>

#include "cinder/net/byte_reader.hpp"

namespace {

    // Model the reader offset in the harness: every successful op advances by
    // exactly its width, every failed op leaves the offset untouched (the
    // implementation bounds-checks before advancing). Any divergence is a
    // bounds bug.
    static void offsets(FuzzedDataProvider& fdp) {
        const size_t buf_len = fdp.ConsumeIntegralInRange<size_t>(0, 256);
        auto bytes = fdp.ConsumeBytes<uint8_t>(buf_len);
        std::span<const std::byte> span(
            reinterpret_cast<const std::byte*>(bytes.data()),
            bytes.size()
        );
        cinder::net::ByteReader reader(span);
        size_t model = 0;

        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 64);
        for (size_t i = 0; i < n; ++i) {
            const uint8_t op = fdp.ConsumeIntegralInRange<uint8_t>(0, 5);
            size_t width = 0;
            bool ok = false;
            switch (op) {
                case 0: {
                    auto v = reader.read<uint8_t>();
                    ok = v.has_value();
                    width = sizeof(uint8_t);
                    if (ok) {
                        assert(*v == bytes[model]);
                    }
                    break;
                }
                case 1: {
                    auto v = reader.read<uint16_t>();
                    ok = v.has_value();
                    width = sizeof(uint16_t);
                    if (ok) {
                        const uint16_t expected = static_cast<uint16_t>(
                            (static_cast<uint16_t>(bytes[model]) << 8U) | bytes[model + 1]
                        );
                        assert(*v == expected); // big-endian wire order
                    }
                    break;
                }
                case 2: {
                    auto v = reader.read<uint32_t>();
                    ok = v.has_value();
                    width = sizeof(uint32_t);
                    if (ok) {
                        uint32_t expected = 0;
                        for (size_t b = 0; b < 4; ++b) {
                            expected = (expected << 8U) | bytes[model + b];
                        }
                        assert(*v == expected);
                    }
                    break;
                }
                case 3: {
                    auto v = reader.read<uint64_t>();
                    ok = v.has_value();
                    width = sizeof(uint64_t);
                    if (ok) {
                        uint64_t expected = 0;
                        for (size_t b = 0; b < 8; ++b) {
                            expected = (expected << 8U) | bytes[model + b];
                        }
                        assert(*v == expected);
                    }
                    break;
                }
                case 4: {
                    const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, 16);
                    auto v = reader.readBytes(len);
                    ok = v.has_value();
                    width = len;
                    if (ok) {
                        assert(v->size() == len);
                        assert(v->data() == span.data() + model); // zero-copy view
                    }
                    break;
                }
                default: {
                    const size_t len = fdp.ConsumeIntegralInRange<size_t>(0, 16);
                    auto v = reader.readStringView(len);
                    ok = v.has_value();
                    width = len;
                    if (ok) {
                        assert(v->size() == len);
                        assert(
                            reinterpret_cast<const std::byte*>(v->data()) == span.data() + model
                        );
                    }
                    break;
                }
            }
            if (ok) {
                model += width;
            }
            assert(reader.offset() == model);
            assert(reader.remaining() == bytes.size() - model);
        }
    }

    // Single-shot reads over adversarial lengths: must never advance past the
    // end or return a view outside the span.
    static void bounds(FuzzedDataProvider& fdp) {
        const size_t buf_len = fdp.ConsumeIntegralInRange<size_t>(0, 256);
        auto bytes = fdp.ConsumeBytes<uint8_t>(buf_len);
        std::span<const std::byte> span(
            reinterpret_cast<const std::byte*>(bytes.data()),
            bytes.size()
        );
        cinder::net::ByteReader reader(span);
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 32);
        for (size_t i = 0; i < n; ++i) {
            const size_t len = fdp.ConsumeIntegral<size_t>() % (bytes.size() + 8);
            switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
                case 0: {
                    auto v = reader.readBytes(len);
                    if (v.has_value()) {
                        assert(reader.offset() <= bytes.size());
                        assert(v->data() >= span.data());
                        assert(v->data() + v->size() <= span.data() + span.size());
                    }
                    break;
                }
                case 1: {
                    auto v = reader.readString(len);
                    if (v.has_value()) {
                        assert(v->size() == len);
                    }
                    break;
                }
                default: {
                    auto v = reader.readStringView(len);
                    if (v.has_value()) {
                        assert(v->size() == len);
                        assert(reinterpret_cast<const std::byte*>(v->data()) >= span.data());
                        assert(
                            reinterpret_cast<const std::byte*>(v->data())
                            + v->size()
                            <= span.data()
                            + span.size()
                        );
                    }
                    break;
                }
            }
            assert(reader.offset() <= bytes.size());
            assert(reader.remaining() == bytes.size() - reader.offset());
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    if (fdp.ConsumeBool()) {
        offsets(fdp);
    } else {
        bounds(fdp);
    }
    return 0;
}
