#include <cinder/store/lru_store.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }

    std::string_view input(reinterpret_cast<const char*>(data), size);
    cinder::LruStore store(1'024);
    // Split: first 4 bytes = key, rest = value
    auto key = input.substr(0, 4);
    auto value = input.substr(4);

    (void)store.put(std::string(key), std::string(value)); // NOLINT
    (void)store.get(std::string(key));
    (void)store.remove(std::string(key));
    return 0;
}
