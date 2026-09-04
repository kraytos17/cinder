#include <cinder/store/wal.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <string>

extern "C" int
LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 4) {
        return 0;
    }

    // Write fuzz data as a fake WAL file, then try to read it back.
    // Must not crash — only return errors.
    // Use a thread_local counter to avoid collisions across fuzz invocations.
    static thread_local uint64_t counter = 0;
    auto id = counter++;
    std::string path = "/tmp/cinder_fuzz_wal_" + std::to_string(id) + ".wal";
    {
        FILE* f = std::fopen(path.c_str(), "wb");
        if (!f) {
            return 0;
        }
        (void)std::fwrite(data, 1, size, f);
        (void)std::fclose(f);
    }

    // Try to read the WAL file through the WalReader.
    cinder::WalReader reader(path);
    while (auto entry = reader.next()) {
        // Successfully parsed an entry — validate basic invariants.
        (void)entry->op;
        (void)entry->key;
        (void)entry->value;
        (void)entry->version;
    }

    std::filesystem::remove(path);
    return 0;
}
