#pragma once

#include <concepts>
#include <cstddef>
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

    // Little-endian scalar codec, one template per sink instead of N
    // near-identical overloads. Native-endian bytes, matching the on-disk
    // WAL0/CSNP layout; use ByteWriter/ByteReader for big-endian wire data.
    template <typename T>
        requires std::unsigned_integral<T>
    inline void writeLE(std::ostream& out, T v) {
        out.write(reinterpret_cast<const char*>(&v), sizeof(v));
    }

    // Buffer variant for pre-sized scratch encoding (e.g. WalWriter::append).
    // Caller must ensure buf.size() >= off + sizeof(v).
    template <typename T>
        requires std::unsigned_integral<T>
    inline void writeLE(std::vector<std::byte>& buf, size_t& off, T v) {
        std::memcpy(&buf[off], &v, sizeof(v));
        off += sizeof(v);
    }

    inline void writeBytes(std::vector<std::byte>& buf, size_t& off, const void* data, size_t len) {
        std::memcpy(&buf[off], data, len);
        off += len;
    }

    template <typename T>
        requires std::unsigned_integral<T>
    inline auto readLE(std::istream& in) -> std::optional<T> {
        T v = 0;
        if (!in.read(reinterpret_cast<char*>(&v), sizeof(v))) {
            return std::nullopt;
        }
        return v;
    }
} // namespace cinder::detail
