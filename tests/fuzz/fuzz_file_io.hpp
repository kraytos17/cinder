#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

// Shared file-backed fuzz harness: writers are path-based (no istream
// injection point), so each input gets a PID-scoped temp dir (libFuzzer
// -jobs forks workers; the PID separates them) plus a per-garden counter.
// RAII unlink keeps /tmp clean, including `<path>.tmp` staging files.
namespace cinder_fuzz {

    class FileGuard {
    public:

        explicit FileGuard(std::string path) :
            path_(std::move(path)) {}

        ~FileGuard() {
            std::error_code ec;
            std::filesystem::remove(path_, ec);
            std::filesystem::remove(path_ + ".tmp", ec);
        }

        FileGuard(const FileGuard&) = delete;
        auto operator=(const FileGuard&) -> FileGuard& = delete;

        auto path() const -> const std::string& {
            return path_;
        }

    private:

        std::string path_;
    };

    // One garden per fuzz target: TempGarden("wal", "wal_", ".wal").
    // Noexcept construction with error_code overloads throughout: static
    // instances must not throw during dynamic initialization.
    // Allocation failure terminates, which is acceptable for a test harness.
    class TempGarden {
    public:

        TempGarden(std::string_view tag, std::string_view prefix, std::string_view suffix) noexcept
            :
            prefix_(prefix),
            suffix_(suffix) {
            std::error_code ec;
            const auto base = std::filesystem::temp_directory_path(ec);
            dir_ = base.string()
                + "/cinder_fuzz_"
                + std::string(tag)
                + "_"
                + std::to_string(::getpid());
            std::filesystem::create_directories(dir_, ec);
        }

        auto next() -> FileGuard {
            return FileGuard(dir_ + "/" + prefix_ + std::to_string(counter_++) + suffix_);
        }

    private:

        std::string dir_;
        std::string prefix_;
        std::string suffix_;
        uint64_t counter_ = 0;
    };

    inline auto slurp(const std::string& path) -> std::vector<uint8_t> {
        std::ifstream in(path, std::ios::binary);
        std::ostringstream ss;
        ss << in.rdbuf();
        auto str = ss.str();
        return {str.begin(), str.end()};
    }

    inline auto spit(const std::string& path, const uint8_t* data, size_t size) -> void {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    }
} // namespace cinder_fuzz
