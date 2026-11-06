#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace cinder::detail {

    // Durability helpers shared by WAL/snapshot/journal rotation.
    // POSIX: fsync the file, then fsync the containing directory so the
    // rename itself survives an OS crash. Windows: best-effort no-op
    // (ofstream flush is the durability story there).
    inline auto syncFileContents(const std::string& path) -> bool {
#ifdef _WIN32
        (void)path;
        return true;
#else
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return false;
        }

        const bool ok = ::fsync(fd) == 0;
        ::close(fd);
        return ok;
#endif
    }

    inline auto syncDirectory(const std::string& dir_or_file_path) -> bool {
#ifdef _WIN32
        (void)dir_or_file_path;
        return true;
#else
        std::error_code ec;
        auto dir = std::filesystem::is_directory(dir_or_file_path, ec)
            ? std::filesystem::path(dir_or_file_path)
            : std::filesystem::path(dir_or_file_path).parent_path();
        if (ec || dir.empty()) {
            dir = ".";
        }

        int fd = ::open(dir.c_str(), O_RDONLY);
        if (fd < 0) {
            return false;
        }

        const bool ok = ::fsync(fd) == 0;
        ::close(fd);
        return ok;
#endif
    }

    inline void writeU32(std::ostream& out, uint32_t v) {
        out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    inline void writeU64(std::ostream& out, uint64_t v) {
        out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    inline void writeU8(std::ostream& out, uint8_t v) {
        out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    // Buffer variants for pre-sized scratch encoding (e.g. WalWriter::append).
    // Caller must ensure buf.size() >= off + sizeof(v) / len.
    inline void writeU8(std::vector<std::byte>& buf, size_t& off, uint8_t v) {
        std::memcpy(&buf[off], &v, sizeof(v));
        off += sizeof(v);
    }

    inline void writeU32(std::vector<std::byte>& buf, size_t& off, uint32_t v) {
        std::memcpy(&buf[off], &v, sizeof(v));
        off += sizeof(v);
    }

    inline void writeU64(std::vector<std::byte>& buf, size_t& off, uint64_t v) {
        std::memcpy(&buf[off], &v, sizeof(v));
        off += sizeof(v);
    }

    inline void writeBytes(std::vector<std::byte>& buf, size_t& off, const void* data, size_t len) {
        std::memcpy(&buf[off], data, len);
        off += len;
    }

    inline auto readU32(std::istream& in) -> std::optional<uint32_t> {
        uint32_t v = 0;
        if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
            return std::nullopt;
        }
        return v;
    }

    inline auto readU64(std::istream& in) -> std::optional<uint64_t> {
        uint64_t v = 0;
        if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
            return std::nullopt;
        }
        return v;
    }

    inline auto readU8(std::istream& in) -> std::optional<uint8_t> {
        uint8_t v = 0;
        if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
            return std::nullopt;
        }
        return v;
    }

    inline auto readString(std::istream& in, size_t len) -> std::optional<std::string> {
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
