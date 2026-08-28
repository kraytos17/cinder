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
    std::string path =
        "/tmp/cinder_fuzz_snapshot_" + std::to_string(reinterpret_cast<uintptr_t>(data));
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
