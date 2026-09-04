#pragma once

#include <cstdint>
#include <iostream>
#include <optional>
#include <string>

namespace cinder::detail {

inline void
writeU32(std::ostream& out, uint32_t v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

inline void
writeU64(std::ostream& out, uint64_t v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

inline void
writeU8(std::ostream& out, uint8_t v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

inline auto
readU32(std::istream& in) -> std::optional<uint32_t> {
    uint32_t v = 0;
    if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
        return std::nullopt;
    }
    return v;
}

inline auto
readU64(std::istream& in) -> std::optional<uint64_t> {
    uint64_t v = 0;
    if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
        return std::nullopt;
    }
    return v;
}

inline auto
readU8(std::istream& in) -> std::optional<uint8_t> {
    uint8_t v = 0;
    if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
        return std::nullopt;
    }
    return v;
}

inline auto
readString(std::istream& in, size_t len) -> std::optional<std::string> {
    // Bounds-check: refuse to allocate more than the remaining stream size
    // to prevent OOM on malformed inputs (e.g. fuzzed WAL with huge key_len).
    auto pos = in.tellg();
    in.seekg(0, std::ios::end);
    auto end = in.tellg();
    in.seekg(pos);
    if (end < pos || static_cast<uint64_t>(end - pos) < len) {
        return std::nullopt;
    }
    std::string buf(len, '\0');
    if (!in.read(buf.data(), static_cast<std::streamsize>(len))) {
        return std::nullopt;
    }
    return buf;
}
} // namespace cinder::detail
