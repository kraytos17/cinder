#include <cinder/net/http_parser.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view input(reinterpret_cast<const char*>(data), size);
    // Fuzz parseHttpRequest — must not crash on any input.
    auto result = cinder::net::parseHttpRequest(input);
    if (result.has_value()) {
        // Also fuzz the formatters with the parsed output to ensure
        // round-trip safety on the response path.
        (void)cinder::net::formatHttpResponse(result->path);
    }
    // Also fuzz formatHttp404 (should never crash).
    (void)cinder::net::formatHttp404();
    return 0;
}
