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

#include "cinder/store/snapshot.hpp"

namespace {

// Writers are path-based (no istream injection point), so each input gets a
// PID-scoped temp dir (libFuzzer -jobs forks workers; the PID separates
// them) plus a per-input counter. RAII unlink keeps /tmp clean. Note the
// writer itself stages through `<path>.tmp` + rename, so two paths per
// input are touched and both are guarded.
auto tempDir() -> const std::string& {
    static const std::string dir =
        std::filesystem::temp_directory_path().string() + "/cinder_fuzz_snap_"
        + std::to_string(::getpid());
    static const bool created =
        (std::filesystem::create_directories(dir), true);
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

auto nextPath() -> FileGuard {
    static uint64_t counter = 0;
    return FileGuard(
        tempDir() + "/snap_" + std::to_string(counter++) + ".csnp"
    );
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

auto operator==(const cinder::SnapshotEntry& a, const cinder::SnapshotEntry& b) -> bool {
    return a.key == b.key
        && a.value == b.value
        && a.version == b.version
        && a.writer_node_hash == b.writer_node_hash
        && a.expires_at_ms == b.expires_at_ms
        && a.has_ttl == b.has_ttl
        && a.freq == b.freq;
}

// Header is magic(4) + version(4) + next_version(8) + count(4): the writer
// normalizes has_ttl to 0/1, so roundtripped entries compare exactly.
constexpr size_t K_HEADER_SIZE = 20;

static auto makeEntry(FuzzedDataProvider& fdp) -> cinder::SnapshotEntry {
    cinder::SnapshotEntry e;
    e.key = fdp.ConsumeRandomLengthString(256);
    // Occasional large value exercises the size paths.
    e.value = fdp.ConsumeBool() ? fdp.ConsumeRandomLengthString(256)
                                 : fdp.ConsumeRandomLengthString(16'384);
    e.version = fdp.ConsumeIntegral<cinder::Version>();
    e.writer_node_hash = fdp.ConsumeIntegral<uint64_t>();
    e.expires_at_ms = fdp.ConsumeIntegral<uint64_t>();
    e.has_ttl = fdp.ConsumeBool();
    e.freq = fdp.ConsumeIntegral<size_t>();
    return e;
}

static void roundtrip(FuzzedDataProvider& fdp) {
    const size_t n = fdp.ConsumeIntegralInRange<size_t>(0, 16);
    std::vector<cinder::SnapshotEntry> entries;
    entries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        entries.push_back(makeEntry(fdp));
    }
    const auto next_version = fdp.ConsumeIntegral<cinder::Version>();

    auto guard = nextPath();
    {
        cinder::SnapshotWriter writer(guard.path());
        auto res = writer.write(next_version, entries);
        assert(res.has_value());
    }
    cinder::SnapshotReader reader(guard.path());
    auto got = reader.readAll();
    assert(got.has_value());
    assert(got->next_version == next_version);
    assert(got->entries.size() == entries.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        assert(got->entries[i] == entries[i]);
    }
}

static void mutate(FuzzedDataProvider& fdp) {
    const size_t n = fdp.ConsumeIntegralInRange<size_t>(1, 16);
    std::vector<cinder::SnapshotEntry> entries;
    entries.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        entries.push_back(makeEntry(fdp));
    }
    const auto next_version = fdp.ConsumeIntegral<cinder::Version>();

    auto base = nextPath();
    {
        cinder::SnapshotWriter writer(base.path());
        auto res = writer.write(next_version, entries);
        assert(res.has_value());
    }
    auto bytes = slurp(base.path());
    if (bytes.size() <= K_HEADER_SIZE) {
        return;
    }

    // Flip-only and truncate-only: never insert. Snapshots carry no
    // checksums, so a body mutation can decode to *different* entry data —
    // only framing invariants survive: with the 20-byte header intact the
    // count field is untouched, so a successful read returns exactly N
    // entries with the original next_version (any truncation or length
    // corruption surfaces as err instead). Header mutations: crash-freedom
    // only.
    const size_t at = fdp.ConsumeIntegralInRange<size_t>(0, bytes.size() - 1);
    const bool header_hit = at < K_HEADER_SIZE;
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
    cinder::SnapshotReader reader(mutated.path());
    auto got = reader.readAll(); // NOLINT: intentionally ignoring the Result
    if (got.has_value() && !header_hit) {
        assert(got->next_version == next_version);
        assert(got->entries.size() == entries.size());
    }
}

static void raw(FuzzedDataProvider& fdp) {
    // Direct passthrough: arbitrary bytes (bad magic, lying counts,
    // truncations) must never crash — only return errors.
    auto bytes = fdp.ConsumeRemainingBytes<uint8_t>();
    auto guard = nextPath();
    spit(guard.path(), bytes.data(), bytes.size());
    cinder::SnapshotReader reader(guard.path());
    (void)reader.readAll(); // NOLINT: intentionally ignoring the Result
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
