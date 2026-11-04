#include "cinder/store/snapshot.hpp"

#include <filesystem>
#include <fstream>

#include "cinder/common/tracing.hpp"
#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {

    SnapshotWriter::SnapshotWriter(const std::string& path) :
        path_(path) {}

    auto SnapshotWriter::write(Version next_version, std::span<const SnapshotEntry> entries)
        -> Result<void> {
        Span span("snapshot.write", captureTraceContext());
        std::ofstream out(path_ + ".tmp", std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "cannot open snapshot file for writing"));
        }

        // Header in one write: magic + version + next_version + entry_count.
        scratch_.resize(sizeof(uint32_t) * 3 + sizeof(uint64_t));
        size_t off = 0;
        writeU32(scratch_, off, K_SNAPSHOT_MAGIC);
        writeU32(scratch_, off, K_SNAPSHOT_FORMAT_VERSION);
        writeU64(scratch_, off, next_version);
        writeU32(scratch_, off, static_cast<uint32_t>(entries.size()));
        out.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );

        // One write per entry. Field order, widths, and native-endian byte
        // layout are unchanged from the per-field stream writes, so files
        // stay byte-identical (WAL0/CSNP magic, fuzz corpora still decode).
        // v2 appends a trailing tombstone byte after freq.
        for (const auto& entry : entries) {
            auto key_len = static_cast<uint32_t>(entry.key.size());
            auto val_len = static_cast<uint32_t>(entry.value.size());
            const size_t size = sizeof(uint32_t)
                + key_len
                + sizeof(uint32_t)
                + val_len
                + sizeof(uint64_t)
                * 4
                + sizeof(uint8_t)
                + sizeof(uint8_t); // has_ttl + tombstone

            scratch_.resize(size);
            off = 0;
            writeU32(scratch_, off, key_len);
            writeBytes(scratch_, off, entry.key.data(), key_len);
            writeU32(scratch_, off, val_len);
            writeBytes(scratch_, off, entry.value.data(), val_len);
            writeU64(scratch_, off, entry.version);
            writeU64(scratch_, off, entry.writer_node_hash);
            writeU64(scratch_, off, entry.expires_at_ms);
            writeU8(scratch_, off, entry.has_ttl ? 1 : 0);
            writeU64(scratch_, off, entry.freq);
            writeU8(scratch_, off, entry.tombstone ? 1 : 0);
            out.write(
                reinterpret_cast<const char*>(scratch_.data()),
                static_cast<std::streamsize>(scratch_.size())
            );
        }

        out.flush();
        if (!out.good()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot write failed"));
        }

        std::error_code ec;
        std::filesystem::rename(path_ + ".tmp", path_, ec);
        if (ec) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot rename failed: " + ec.message()));
        }

        span.setAttribute({"entries", entries.size()});
        span.setStatus(SpanStatus::Ok);
        return ok();
    }

    SnapshotReader::SnapshotReader(const std::string& path) :
        in_(path, std::ios::binary) {}

    auto SnapshotReader::readAll() -> Result<SnapshotData> {
        Span span("snapshot.read", captureTraceContext());
        if (!in_.is_open()) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(Error(Errc::InternalError, "snapshot file not open"));
        }

        auto magic = readU32(in_);
        if (!magic.has_value() || magic.value() != K_SNAPSHOT_MAGIC) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(Error(Errc::CorruptData, "invalid snapshot magic"));
        }

        auto format_ver = readU32(in_);
        const bool is_v2 =
            format_ver.has_value() && format_ver.value() == K_SNAPSHOT_FORMAT_VERSION;
        const bool is_v1 =
            format_ver.has_value() && format_ver.value() == K_SNAPSHOT_FORMAT_VERSION_V1;
        if (!is_v1 && !is_v2) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(
                Error(Errc::CorruptData, "unsupported snapshot format version")
            );
        }

        auto next_ver = readU64(in_);
        if (!next_ver.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot header"));
        }

        auto entry_count = readU32(in_);
        if (!entry_count.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot entry count"));
        }

        // Validate entry_count against remaining file bytes. Each entry requires
        // at least 41 bytes v1 (key_len + val_len + version + writer_hash +
        // expires_ms + has_ttl + freq, excluding variable-length key/value),
        // 42 bytes v2 (trailing tombstone byte).
        constexpr size_t K_MIN_ENTRY_BYTES_V1 = 41;
        constexpr size_t K_MIN_ENTRY_BYTES_V2 = 42;
        const size_t min_entry_bytes = is_v2 ? K_MIN_ENTRY_BYTES_V2 : K_MIN_ENTRY_BYTES_V1;
        auto pos = in_.tellg();
        in_.seekg(0, std::ios::end);

        auto file_size = in_.tellg();
        in_.seekg(pos);
        auto remaining = static_cast<uint64_t>(file_size - pos);
        if (static_cast<uint64_t>(entry_count.value()) > remaining / min_entry_bytes) {
            span.setStatus(SpanStatus::Error);
            return err<SnapshotData>(Error(Errc::InvalidArgument, "entry_count exceeds file size"));
        }

        SnapshotData data;
        data.next_version = next_ver.value();
        data.entries.reserve(entry_count.value());
        for (uint32_t i = 0; i < entry_count.value(); ++i) {
            auto key_len = readU32(in_);
            if (!key_len.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot key"));
            }

            auto key = readString(in_, key_len.value());
            if (!key.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot key data"));
            }

            auto val_len = readU32(in_);
            if (!val_len.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot value"));
            }

            auto value = readString(in_, val_len.value());
            if (!value.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot value data"));
            }

            auto version = readU64(in_);
            if (!version.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot version"));
            }

            auto writer_hash = readU64(in_);
            if (!writer_hash.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(
                    Error(Errc::CorruptData, "truncated snapshot writer_hash")
                );
            }

            auto expires_ms = readU64(in_);
            if (!expires_ms.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(
                    Error(Errc::CorruptData, "truncated snapshot expires_at_ms")
                );
            }

            auto has_ttl = readU8(in_);
            if (!has_ttl.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot has_ttl"));
            }

            auto freq = readU64(in_);
            if (!freq.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err<SnapshotData>(Error(Errc::CorruptData, "truncated snapshot freq"));
            }

            bool tombstone = false;
            if (is_v2) {
                auto tombstone_byte = readU8(in_);
                if (!tombstone_byte.has_value() || tombstone_byte.value() > 1) {
                    span.setStatus(SpanStatus::Error);
                    return err<SnapshotData>(
                        Error(Errc::CorruptData, "truncated snapshot tombstone")
                    );
                }
                tombstone = tombstone_byte.value() != 0;
            }

            data.entries.push_back(
                SnapshotEntry{
                    .key = std::move(key.value()),
                    .value = std::move(value.value()),
                    .version = version.value(),
                    .writer_node_hash = writer_hash.value(),
                    .expires_at_ms = expires_ms.value(),
                    .has_ttl = has_ttl.value() != 0,
                    .tombstone = tombstone,
                    .freq = freq.value(),
                }
            );
        }

        span.setAttribute({"entries", data.entries.size()});
        span.setStatus(SpanStatus::Ok);
        return data;
    }
} // namespace cinder
