#pragma once

// Scratch files and directories for tests.
//
// Design follows Rust's `tempfile` crate: names carry enough randomness that
// creation is exclusive and collision-free, cleanup is best-effort RAII on
// drop, and `keep()` opts out for post-mortem debugging.
//
// Why randomness instead of pid+counter: CTest runs every case in its own
// process, so workers routinely share heap addresses and a `this`-pointer
// name collides across processes (one TearDown deleting another test's live
// dir). Pid+counter fixes the live collision but not stale leftovers — a
// killed run leaves `cinder_scratch_<pid>_<n>` behind, and pid reuse plus
// the same counter sequence reproduces the exact name, so a new test would
// silently inherit a stale dir. Random names with O_CREAT|O_EXCL-style
// exclusive creation close both holes: a stale name is never regenerated,
// and a live collision is retried, never clobbered.
//
// No static state: randomness comes from `std::random_device` per call, so
// there is nothing with static storage duration to trip CERT ERR58-CPP.
// `TMPDIR` is honored via `temp_directory_path`, like tempfile's `env::temp_dir()`.

#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <random>
#include <string>
#include <string_view>
#include <unistd.h>

namespace cinder::test {

    namespace scratch_detail {
        // tempfile-style alphabet: unambiguous alphanumerics only.
        constexpr std::string_view K_ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        // Attempts before giving up on a unique name. tempfile retries
        // effectively unboundedly; 12 random chars (~71 bits) make even one
        // collision implausible, so a small bound only guards pathological
        // filesystems.
        constexpr int K_ATTEMPTS = 100;

        inline auto randomToken(std::string& out, size_t len) -> void {
            std::random_device rd;
            for (size_t i = 0; i < len; ++i) {
                out += K_ALPHABET[rd() % K_ALPHABET.size()];
            }
        }

        inline auto makeName(std::string_view stem, std::string_view suffix) -> std::string {
            std::string name(stem);
            name += '_';
            name += std::to_string(::getpid());
            name += '_';
            randomToken(name, 12);
            name += suffix;
            return name;
        }
    } // namespace scratch_detail

    // RAII scratch directory, removed on destruction unless kept. Fallible:
    // check validity (`ASSERT_TRUE(scratch)`) before use — a failed create
    // (read-only $TMPDIR, exhaustion) must not masquerade as an empty dir.
    class ScratchDir {
    public:

        explicit ScratchDir(std::string_view stem = "cinder_scratch") {
            std::filesystem::path base;
            try {
                base = std::filesystem::temp_directory_path();
            } catch (const std::filesystem::filesystem_error&) {
                return;
            }
            for (int i = 0; i < scratch_detail::K_ATTEMPTS; ++i) {
                auto candidate = base / scratch_detail::makeName(stem, "");
                // Single-level create: EEXIST (a stale leftover with the
                // same random name) is distinguishable and retried.
                std::error_code ec;
                if (std::filesystem::create_directory(candidate, ec)) {
                    dir_ = std::move(candidate);
                    return;
                }
                if (ec != std::errc::file_exists) {
                    return;
                }
            }
        }

        ~ScratchDir() {
            if (keep_ || dir_.empty()) {
                return;
            }

            std::error_code ec;
            std::filesystem::remove_all(dir_, ec);
        }

        ScratchDir(const ScratchDir&) = delete;
        auto operator=(const ScratchDir&) -> ScratchDir& = delete;
        ScratchDir(ScratchDir&&) = delete;
        auto operator=(ScratchDir&&) -> ScratchDir& = delete;

        [[nodiscard]]
        explicit operator bool() const {
            return !dir_.empty();
        }

        // Opt out of cleanup (tempfile's `keep`): returns the path so a
        // failing test can log it for post-mortem inspection.
        auto keep() -> std::filesystem::path {
            keep_ = true;
            return dir_;
        }

        [[nodiscard]]
        auto path() const -> const std::filesystem::path& {
            return dir_;
        }

        [[nodiscard]]
        auto file(std::string_view name) const -> std::filesystem::path {
            return dir_ / name;
        }

    private:

        std::filesystem::path dir_;
        bool keep_ = false;
    };

    // Unique scratch file path, reserved with O_CREAT|O_EXCL (tempfile's
    // `NamedTempFile` semantics): the file is created exclusively up front,
    // so the name cannot be stolen between reservation and first write. A
    // leftover (e.g. written by the test) is removed on destruction unless
    // kept. Nothing is an error to open with trunc afterwards.
    class ScratchFile {
    public:

        explicit ScratchFile(std::string_view stem, std::string_view suffix = "") {
            std::filesystem::path base;
            try {
                base = std::filesystem::temp_directory_path();
            } catch (const std::filesystem::filesystem_error&) {
                return;
            }
            for (int i = 0; i < scratch_detail::K_ATTEMPTS; ++i) {
                auto candidate = base / scratch_detail::makeName(stem, suffix);
                // O_EXCL makes the reservation atomic: a stale leftover or
                // a racing worker yields EEXIST and we retry with a new name.
                int fd = ::open(candidate.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
                if (fd >= 0) {
                    ::close(fd);
                    path_ = std::move(candidate);
                    return;
                }
                if (errno != EEXIST) {
                    return;
                }
            }
        }

        ~ScratchFile() {
            if (keep_ || path_.empty()) {
                return;
            }

            std::error_code ec;
            std::filesystem::remove(path_, ec);
        }

        ScratchFile(const ScratchFile&) = delete;
        auto operator=(const ScratchFile&) -> ScratchFile& = delete;
        ScratchFile(ScratchFile&&) = delete;
        auto operator=(ScratchFile&&) -> ScratchFile& = delete;

        [[nodiscard]]
        explicit operator bool() const {
            return !path_.empty();
        }

        // Opt out of cleanup (tempfile's `persist`, path form): returns the
        // path so a failing test can log it for post-mortem inspection.
        auto keep() -> std::filesystem::path {
            keep_ = true;
            return path_;
        }

        [[nodiscard]]
        auto path() const -> const std::filesystem::path& {
            return path_;
        }

        [[nodiscard]]
        auto string() const -> std::string {
            return path_.string();
        }

    private:

        std::filesystem::path path_;
        bool keep_ = false;
    };
} // namespace cinder::test
