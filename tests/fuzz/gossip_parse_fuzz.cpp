#include <cinder/cluster/gossip.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view input(reinterpret_cast<const char*>(data), size);
    // Fuzz parseEntry directly — must not crash on any input.
    cinder::NodeInfo info;
    (void)cinder::gossip::parseEntry(input, info);
    return 0;
}
