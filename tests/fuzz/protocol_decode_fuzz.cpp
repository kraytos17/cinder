#include <cinder/net/protocol.hpp>
#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    auto span = std::as_bytes(std::span<const uint8_t>{data, size});
    // Must not crash on any input — only return errors.
    (void)cinder::net::decode(span);         // NOLINT
    (void)cinder::net::decodeResponse(span); // NOLINT
    return 0;
}
