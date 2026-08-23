#include "cinder/store/snapshot.hpp"

#include <filesystem>
#include <fstream>

#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {

SnapshotWriter::SnapshotWriter(const std::string& path)
    : path_(path) {}

auto
SnapshotWriter::write(Version next_version, const std::vector<SnapshotEntry>& entries)
    -> Result<void> {
    std::ofstream out(path_ + ".tmp", std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        return err(Error(Errc::InternalError, "cannot open snapshot file for writing"));
    }

    writeU32(out, K_SNAPSHOT_MAGIC);
    writeU32(out, K_SNAPSHOT_FORMAT_VERSION);
    writeU64(out, next_version);
    writeU32(out, static_cast<uint32_t>(entries.size()));

    for (const auto& entry : entries) {
        auto key_len = static_cast<uint32_t>(entry.key.size());
        auto val_len = static_cast<uint32_t>(entry.value.size());

        writeU32(out, key_len);
        out.write(entry.key.data(), static_cast<std::streamsize>(key_len));
        writeU32(out, val_len);
        out.write(entry.value.data(), static_cast<std::streamsize>(val_len));
        writeU64(out, entry.version);
        writeU64(out, entry.writer_node_hash);
        writeU64(out, entry.expires_at_ms);
        writeU8(out, entry.has_ttl ? 1 : 0);
        writeU64(out, entry.freq);
    }

    out.flush();
    if (!out.good()) {
        return err(Error(Errc::InternalError, "snapshot write failed"));
    }

    std::error_code ec;
    std::filesystem::rename(path_ + ".tmp", path_, ec);
    if (ec) {
        return err(Error(Errc::InternalError, "snapshot rename failed: " + ec.message()));
    }
    return ok();
}

SnapshotReader::SnapshotReader(const std::string& path)
    : in_(path, std::ios::binary) {}

auto
SnapshotReader::readAll() -> Result<SnapshotData> {
    if (!in_.is_open()) {
        return err<SnapshotData>(Error(Errc::InternalError, "snapshot file not open"));
    }

    auto magic = readU32(in_);
    if (!magic.has_value() || magic.value() != K_SNAPSHOT_MAGIC) {
        return err<SnapshotData>(Error(Errc::InternalError, "invalid snapshot magic"));
    }

    auto format_ver = readU32(in_);
    if (!format_ver.has_value() || format_ver.value() != K_SNAPSHOT_FORMAT_VERSION) {
        return err<SnapshotData>(Error(Errc::InternalError, "unsupported snapshot format version"));
    }

    auto next_ver = readU64(in_);
    if (!next_ver.has_value()) {
        return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot header"));
    }

    auto entry_count = readU32(in_);
    if (!entry_count.has_value()) {
        return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot entry count"));
    }

    SnapshotData data;
    data.next_version = next_ver.value();
    data.entries.reserve(entry_count.value());
    for (uint32_t i = 0; i < entry_count.value(); ++i) {
        auto key_len = readU32(in_);
        if (!key_len.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot key"));
        }

        auto key = readString(in_, key_len.value());
        if (!key.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot key data"));
        }

        auto val_len = readU32(in_);
        if (!val_len.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot value"));
        }

        auto value = readString(in_, val_len.value());
        if (!value.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot value data"));
        }

        auto version = readU64(in_);
        if (!version.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot version"));
        }

        auto writer_hash = readU64(in_);
        if (!writer_hash.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot writer_hash"));
        }

        auto expires_ms = readU64(in_);
        if (!expires_ms.has_value()) {
            return err<SnapshotData>(
                Error(Errc::InternalError, "truncated snapshot expires_at_ms"));
        }

        auto has_ttl = readU8(in_);
        if (!has_ttl.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot has_ttl"));
        }

        auto freq = readU64(in_);
        if (!freq.has_value()) {
            return err<SnapshotData>(Error(Errc::InternalError, "truncated snapshot freq"));
        }

        data.entries.push_back(SnapshotEntry{
            .key = std::move(key.value()),
            .value = std::move(value.value()),
            .version = version.value(),
            .writer_node_hash = writer_hash.value(),
            .expires_at_ms = expires_ms.value(),
            .has_ttl = has_ttl.value() != 0,
            .freq = freq.value(),
        });
    }
    return data;
}
} // namespace cinder
