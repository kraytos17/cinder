#include <cassert>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <fuzzer/FuzzedDataProvider.h>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "cinder/store/wal.hpp"

namespace {

    // Writers are path-based (no istream injection point), so each input gets a
    // PID-scoped temp dir (libFuzzer -jobs forks workers; the PID separates
    // them) plus a per-input counter. RAII unlink keeps /tmp clean.
    auto tempDir() -> const std::string& {
        static const std::string dir = std::filesystem::temp_directory_path().string()
            + "/cinder_fuzz_wal_"
            + std::to_string(::getpid());
        static const bool created = (std::filesystem::create_directories(dir), true);
        (void)created;
        return dir;
    }

    class FileGuard {
    public:

        explicit FileGuard(std::string path) :
            path_(std::move(path)) {}

        ~FileGuard() {
            std::error_code ec;
            std::filesystem::remove(path_, ec);
        }

        FileGuard(const FileGuard&) = delete;
        auto operator=(const FileGuard&) -> FileGuard& = delete;

        auto path() const -> const std::string& {
            return path_;
        }

    private:

        std::string path_;
    };

    auto nextPath() -> FileGuard {
        static uint64_t counter = 0;
        return FileGuard(tempDir() + "/wal_" + std::to_string(counter++) + ".wal");
    }

    auto slurp(const std::string& path) -> std::vector<uint8_t> {
        std::ifstream in(path, std::ios::binary);
        std::ostringstream ss;
        ss << in.rdbuf();
        auto str = ss.str();
        return {str.begin(), str.end()};
    }

    auto spit(const std::string& path, const uint8_t* data, size_t size) -> void {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    }

    auto operator==(const cinder::WalEntry& a, const cinder::WalEntry& b) -> bool {
        return a.op
            == b.op
            && a.key
            == b.key
            && a.value
            == b.value
            && a.version
            == b.version
            && a.writer_node_hash
            == b.writer_node_hash
            && a.expires_at_ms
            == b.expires_at_ms
            && a.has_ttl
            == b.has_ttl;
    }

    // Header is magic(4) + version(4): mutations below this may flip the legacy
    // fallback (no checksums, shifted framing), so only crash-freedom applies.
    constexpr size_t K_HEADER_SIZE = 8;

    static auto makeEntry(FuzzedDataProvider& fdp) -> cinder::WalEntry {
        cinder::WalEntry e;
        e.op = fdp.PickValueInArray({cinder::WalEntry::Op::Set, cinder::WalEntry::Op::Del});
        e.key = fdp.ConsumeRandomLengthString(256);
        // Occasional large value exercises the capacity/size paths.
        e.value = fdp.ConsumeBool() ? fdp.ConsumeRandomLengthString(256)
                                    : fdp.ConsumeRandomLengthString(16'384);
        e.version = fdp.ConsumeIntegral<cinder::Version>();
        e.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
        e.expires_at_ms = fdp.ConsumeIntegral<uint64_t>();
        e.has_ttl = fdp.ConsumeBool();
        return e;
    }

    static void roundtrip(FuzzedDataProvider& fdp) {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 16);
        std::vector<cinder::WalEntry> entries;
        entries.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            entries.push_back(makeEntry(fdp));
        }

        auto guard = nextPath();
        {
            cinder::WalWriter writer(guard.path());
            for (const auto& e : entries) {
                auto res = writer.append(e);
                assert(res.has_value());
            }
        }
        cinder::WalReader reader(guard.path());
        for (const auto& expected : entries) {
            auto got = reader.next();
            assert(got.has_value());
            assert(*got == expected);
        }
        // No trailing garbage: the writer's bytes decode to exactly N entries.
        assert(!reader.next().has_value());
    }

    static void mutate(FuzzedDataProvider& fdp) {
        const size_t n = fdp.ConsumeIntegralInRange<size_t>(1, 16);
        std::vector<cinder::WalEntry> entries;
        entries.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            entries.push_back(makeEntry(fdp));
        }

        auto base = nextPath();
        {
            cinder::WalWriter writer(base.path());
            for (const auto& e : entries) {
                auto res = writer.append(e);
                assert(res.has_value());
            }
        }
        auto bytes = slurp(base.path());
        if (bytes.empty()) {
            return;
        }

        // Flip-only and truncate-only: never insert, so records before the
        // mutation point keep identical bytes (and valid checksums). `at` stays
        // strictly inside the buffer so byte flips never write out of bounds.
        const size_t at = fdp.ConsumeIntegralInRange<size_t>(0, bytes.size() - 1);
        switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
            case 0:
                bytes[at] ^= static_cast<uint8_t>(1U << fdp.ConsumeIntegralInRange(0, 7));
                break;
            case 1:
                bytes[at] ^= fdp.ConsumeIntegral<uint8_t>();
                break;
            default:
                bytes.resize(at);
                break;
        }

        auto mutated = nextPath();
        spit(mutated.path(), bytes.data(), bytes.size());
        cinder::WalReader reader(mutated.path());
        size_t returned = 0;
        while (auto got = reader.next()) {
            // Prefix oracle: every byte before the mutation is intact, so each
            // record fully preceding it checksums clean and decodes identically.
            // The first record touching the mutation fails its checksum (or hits
            // truncation) and iteration stops — never a wrong-data accept.
            if (at < K_HEADER_SIZE) {
                break; // header flip may switch legacy framing: crash-freedom only
            }
            assert(returned < entries.size());
            assert(*got == entries[returned]);
            ++returned;
        }
        assert(returned <= entries.size());
    }

    static void raw(FuzzedDataProvider& fdp) {
        // Direct passthrough: arbitrary bytes (bad magic, truncations, lying
        // lengths) must never crash and never yield an invalid op.
        auto bytes = fdp.ConsumeRemainingBytes<uint8_t>();
        auto guard = nextPath();
        spit(guard.path(), bytes.data(), bytes.size());
        cinder::WalReader reader(guard.path());
        while (auto got = reader.next()) {
            assert(got->op == cinder::WalEntry::Op::Set || got->op == cinder::WalEntry::Op::Del);
        }
    }

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<uint8_t>(0, 2)) {
        case 0:
            roundtrip(fdp);
            break;
        case 1:
            mutate(fdp);
            break;
        default:
            raw(fdp);
            break;
    }
    return 0;
}
