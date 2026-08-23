#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

constexpr uint32_t K_SNAPSHOT_MAGIC = 0x43534E50; // "CSNP"
constexpr uint32_t K_SNAPSHOT_FORMAT_VERSION = 1;

struct SnapshotEntry {
    std::string key;
    std::string value;
    Version version = 0;
    uint64_t writer_node_hash = 0;
    uint64_t expires_at_ms = 0; // wall-clock ms since epoch, 0 if no TTL
    bool has_ttl = false;
    size_t freq = 0; // 0 for LRU
};

struct SnapshotData {
    Version next_version = 0;
    std::vector<SnapshotEntry> entries;
};

class SnapshotWriter {
  public:

    explicit SnapshotWriter(const std::string& path);
    ~SnapshotWriter() = default;

    SnapshotWriter(const SnapshotWriter&) = delete;
    auto operator=(const SnapshotWriter&) -> SnapshotWriter& = delete;
    SnapshotWriter(SnapshotWriter&&) = delete;
    auto operator=(SnapshotWriter&&) -> SnapshotWriter& = delete;

    auto write(Version next_version, const std::vector<SnapshotEntry>& entries) -> Result<void>;

  private:

    std::string path_;
};

class SnapshotReader {
  public:

    explicit SnapshotReader(const std::string& path);
    ~SnapshotReader() = default;

    SnapshotReader(const SnapshotReader&) = delete;
    auto operator=(const SnapshotReader&) -> SnapshotReader& = delete;
    SnapshotReader(SnapshotReader&&) = delete;
    auto operator=(SnapshotReader&&) -> SnapshotReader& = delete;

    auto readAll() -> Result<SnapshotData>;

  private:

    std::ifstream in_;
};
} // namespace cinder
