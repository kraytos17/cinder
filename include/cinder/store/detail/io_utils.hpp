#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "cinder/common/status.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

#include <cerrno>

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

#ifndef _WIN32
    // Complete short writes, retrying on EINTR. Returns false on any hard
    // error (true for zero-length writes). Shared by the WAL writer and the
    // hint journal; both previously hand-rolled this loop.
    inline auto writeAll(int fd, const void* data, size_t len) -> bool {
        const auto* in = static_cast<const char*>(data);
        size_t done = 0;
        while (done < len) {
            const auto n = ::write(fd, in + done, len - done);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (n == 0) {
                return false;
            }
            done += static_cast<size_t>(n);
        }
        return true;
    }

    // Complete short reads; false on EOF or error (torn tail ⇒ heal by
    // truncation, never by allocating).
    inline auto readExactly(int fd, void* data, size_t len) -> bool {
        auto* out = static_cast<char*>(data);
        size_t done = 0;
        while (done < len) {
            const auto n = ::read(fd, out + done, len - done);
            if (n <= 0) {
                return false;
            }
            done += static_cast<size_t>(n);
        }
        return true;
    }
#endif

    // Pre-sized little-endian buffer writer: one constrained template
    // instead of N near-identical writeU* overloads plus manual offset
    // threading at every call site. The caller pre-sizes the buffer to the
    // exact byte count (encode paths guarantee this); overflow is a debug
    // assert, matching net::ByteWriter. Native-endian bytes, matching the
    // on-disk WAL0/CSNP layout; use net::ByteWriter for big-endian wire data.
    class BufferWriter {
    public:

        explicit BufferWriter(std::vector<std::byte>& buf) :
            buf_(buf) {}

        template <typename T>
            requires std::unsigned_integral<T>
        void write(T v) {
            assert(offset_ + sizeof(T) <= buf_.size());
            std::memcpy(&buf_[offset_], &v, sizeof(v));
            offset_ += sizeof(v);
        }

        void writeBytes(const void* data, size_t len) {
            assert(offset_ + len <= buf_.size());
            if (len > 0) {
                std::memcpy(&buf_[offset_], data, len);
            }
            offset_ += len;
        }

        void writeString(std::string_view s) {
            writeBytes(s.data(), s.size());
        }

        [[nodiscard]]
        auto offset() const -> size_t {
            return offset_;
        }

        [[nodiscard]]
        auto bytes() const -> std::span<const std::byte> {
            return {buf_.data(), buf_.size()};
        }

    private:

        std::vector<std::byte>& buf_;
        size_t offset_ = 0;
    };

    // Bounded little-endian buffer reader: replaces hand-rolled cursor
    // structs and (p, end, out) free-function pairs. Bool-style reads compose
    // in validation chains; string reads refuse to allocate past the end
    // (fuzzed length prefixes must yield false, not a huge allocation).
    class BufferReader {
    public:

        BufferReader(const char* data, size_t size) :
            data_(data),
            size_(size) {}

        explicit BufferReader(std::string_view data) :
            data_(data.data()),
            size_(data.size()) {}

        template <typename T>
            requires std::unsigned_integral<T>
        auto read(T& out) -> bool {
            if (pos_ + sizeof(T) > size_) {
                return false;
            }

            std::memcpy(&out, data_ + pos_, sizeof(T));
            pos_ += sizeof(T);
            return true;
        }

        auto readString(size_t len, std::string& out) -> bool {
            if (len > size_ - pos_) {
                return false;
            }

            out.assign(data_ + pos_, len);
            pos_ += len;
            return true;
        }

        // Zero-copy borrow of the next n bytes, advancing past them.
        // Lets large embedded blobs (meta-row blocks, sketches) decode
        // in place instead of copying through a scratch string.
        auto tryView(size_t len) -> std::optional<std::string_view> {
            if (len > size_ - pos_) {
                return std::nullopt;
            }

            std::string_view view(data_ + pos_, len);
            pos_ += len;
            return view;
        }

        [[nodiscard]]
        auto pos() const -> size_t {
            return pos_;
        }

        void seek(size_t pos) {
            pos_ = std::min(pos, size_);
        }

        [[nodiscard]]
        auto remaining() const -> size_t {
            return size_ - pos_;
        }

    private:

        const char* data_ = nullptr;
        size_t size_ = 0;
        size_t pos_ = 0;
    };

    // Stream sink for the few header writes that do not go through a scratch
    // buffer (e.g. WalWriter's file header).
    template <typename T>
        requires std::unsigned_integral<T>
    inline void writeLE(std::ostream& out, T v) {
        out.write(reinterpret_cast<const char*>(&v), sizeof(v));
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

    // Atomic file publish: fsync the staged tmp file, rename over the final
    // path, fsync the directory so the rename itself survives an OS crash.
    // Shared by snapshot, WAL rotation, CURRENT, and hint-journal compaction
    // (whose old hand-rolled sequence skipped the dir fsync). Removes tmp on
    // failure. The `what` label identifies the caller in error messages.
    inline auto publishTempFile(
        const std::string& tmp_path,
        const std::string& final_path,
        std::string_view what
    ) -> Result<void> {
        if (!syncFileContents(tmp_path)) {
            std::error_code ec;
            std::filesystem::remove(tmp_path, ec);
            return err(Error(Errc::InternalError, std::string(what) + " tmp fsync failed"));
        }

        std::error_code ec;
        std::filesystem::rename(tmp_path, final_path, ec);
        if (ec) {
            std::filesystem::remove(tmp_path, ec);
            return err(
                Error(Errc::InternalError, std::string(what) + " rename failed: " + ec.message())
            );
        }
        if (!syncDirectory(final_path)) {
            return err(Error(Errc::InternalError, std::string(what) + " dir fsync failed"));
        }
        return ok();
    }
} // namespace cinder::detail
