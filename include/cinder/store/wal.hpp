#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"

namespace cinder {

constexpr uint32_t K_WAL_MAGIC = 0x57414C30; // "WAL0"
constexpr uint32_t K_WAL_FORMAT_VERSION = 1;

struct WalEntry {
    enum class Op : uint8_t {
        Set = 1,
        Del = 2,
    };

    Op op = Op::Set;
    std::string key;
    std::string value;
    Version version = 0;
    uint64_t writer_node_hash = 0;
    uint64_t expires_at_ms = 0; // wall-clock ms since epoch, 0 if no TTL
    bool has_ttl = false;
};

class WalWriter {
  public:

    explicit WalWriter(const std::string& path);
    ~WalWriter();

    WalWriter(const WalWriter&) = delete;
    auto operator=(const WalWriter&) -> WalWriter& = delete;
    WalWriter(WalWriter&&) = delete;
    auto operator=(WalWriter&&) -> WalWriter& = delete;

    auto append(const WalEntry& entry) -> Result<void>;
    void flush();

  private:

    std::ofstream out_;
};

class WalReader {
  public:

    explicit WalReader(const std::string& path);
    ~WalReader() = default;

    WalReader(const WalReader&) = delete;
    auto operator=(const WalReader&) -> WalReader& = delete;
    WalReader(WalReader&&) = delete;
    auto operator=(WalReader&&) -> WalReader& = delete;

    auto next() -> std::optional<WalEntry>;
    auto hasMore() -> bool;

  private:

    std::ifstream in_;
    bool has_checksums_ = false;
    bool header_checked_ = false;
};
} // namespace cinder
