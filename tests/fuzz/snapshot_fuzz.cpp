#include <cinder/store/snapshot.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // Write fuzz data as a fake snapshot file, then try to read it.
    // Must not crash — only return errors.
    // Use a thread_local counter to avoid path collisions across fuzz invocations.
    static thread_local uint64_t counter = 0;
    auto id = counter++;
    std::string path = "/tmp/cinder_fuzz_snapshot_" + std::to_string(id);
    {
        FILE* f = std::fopen(path.c_str(), "wb");
        if (!f) {
            return 0;
        }
        (void)std::fwrite(data, 1, size, f);
        (void)std::fclose(f);
    }

    cinder::SnapshotReader reader(path);
    (void)reader.readAll(); // NOLINT
    std::filesystem::remove(path);
    return 0;
}
