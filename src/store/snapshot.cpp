#include "cinder/store/snapshot.hpp"

#include <filesystem>
#include <fstream>
#include <limits>

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
        writeLE(scratch_, off, K_SNAPSHOT_MAGIC);
        writeLE(scratch_, off, K_SNAPSHOT_FORMAT_VERSION);
        writeLE(scratch_, off, next_version);
        writeLE(scratch_, off, static_cast<uint32_t>(entries.size()));
        out.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );

        // One write per entry. Field order, widths, and native-endian byte
        // layout are unchanged from the per-field stream writes, so files
        // stay byte-identical (WAL0/CSNP magic, fuzz corpora still decode).
        // v2 appends a trailing tombstone byte after freq.
        for (const auto& entry : entries) {
            if (entry.key.size()
                > std::numeric_limits<uint32_t>::max()
                || entry.value.size()
                > std::numeric_limits<uint32_t>::max()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::InvalidArgument, "snapshot entry too large"));
            }

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
            writeLE(scratch_, off, key_len);
            writeBytes(scratch_, off, entry.key.data(), key_len);
            writeLE(scratch_, off, val_len);
            writeBytes(scratch_, off, entry.value.data(), val_len);
            writeLE(scratch_, off, entry.version);
            writeLE(scratch_, off, entry.writer_node_hash);
            writeLE(scratch_, off, entry.expires_at_ms);
            writeLE(scratch_, off, entry.has_ttl ? uint8_t{1} : uint8_t{0});
            writeLE(scratch_, off, entry.freq);
            writeLE(scratch_, off, entry.tombstone ? uint8_t{1} : uint8_t{0});
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

        out.close();
        if (!out.good()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot close failed"));
        }

        // Crash safety: persist file contents before exposing the rename,
        // then persist the directory entry itself (file-then-dir fsync).
        const std::string tmp_path = path_ + ".tmp";
        if (!syncFileContents(tmp_path)) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot fsync failed"));
        }

        std::error_code ec;
        std::filesystem::rename(tmp_path, path_, ec);
        if (ec) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot rename failed: " + ec.message()));
        }
        if (!syncDirectory(path_)) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot dir fsync failed"));
        }

        span.setAttribute({"entries", entries.size()});
        span.setStatus(SpanStatus::Ok);
        return ok();
    }

    SnapshotReader::SnapshotReader(const std::string& path) :
        in_(path, std::ios::binary) {}

    auto SnapshotReader::readStream(
        Version& next_version,
        std::move_only_function<void(SnapshotEntry&)> fn
    ) -> Result<void> {
        Span span("snapshot.read", captureTraceContext());
        if (!in_.is_open()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "snapshot file not open"));
        }

        auto magic = readLE<uint32_t>(in_);
        if (!magic.has_value() || magic.value() != K_SNAPSHOT_MAGIC) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::CorruptData, "invalid snapshot magic"));
        }

        auto format_ver = readLE<uint32_t>(in_);
        const bool is_v2 =
            format_ver.has_value() && format_ver.value() == K_SNAPSHOT_FORMAT_VERSION;
        const bool is_v1 =
            format_ver.has_value() && format_ver.value() == K_SNAPSHOT_FORMAT_VERSION_V1;
        if (!is_v1 && !is_v2) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::CorruptData, "unsupported snapshot format version"));
        }

        auto next_ver = readLE<uint64_t>(in_);
        if (!next_ver.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::CorruptData, "truncated snapshot header"));
        }

        auto entry_count = readLE<uint32_t>(in_);
        if (!entry_count.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::CorruptData, "truncated snapshot entry count"));
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
            return err(Error(Errc::InvalidArgument, "entry_count exceeds file size"));
        }

        next_version = next_ver.value();
        size_t streamed = 0;
        // Single-seek string reads: one tellg per string against the known
        // file size, instead of re-statting the file on every string.
        const auto total_size = static_cast<uint64_t>(file_size);
        auto read_sized = [&](size_t len) -> std::optional<std::string> {
            const auto cur = in_.tellg();
            if (cur < pos) {
                return std::nullopt;
            }

            const auto cur_off = static_cast<uint64_t>(cur);
            if (total_size < cur_off || total_size - cur_off < len) {
                return std::nullopt;
            }

            std::string buf(len, '\0');
            if (!in_.read(buf.data(), static_cast<std::streamsize>(len))) {
                return std::nullopt;
            }
            return buf;
        };

        for (uint32_t i = 0; i < entry_count.value(); ++i) {
            auto key_len = readLE<uint32_t>(in_);
            if (!key_len.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot key"));
            }

            auto key = read_sized(key_len.value());
            if (!key.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot key data"));
            }

            auto val_len = readLE<uint32_t>(in_);
            if (!val_len.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot value"));
            }

            auto value = read_sized(val_len.value());
            if (!value.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot value data"));
            }

            auto version = readLE<uint64_t>(in_);
            if (!version.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot version"));
            }

            auto writer_hash = readLE<uint64_t>(in_);
            if (!writer_hash.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot writer_hash"));
            }

            auto expires_ms = readLE<uint64_t>(in_);
            if (!expires_ms.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot expires_at_ms"));
            }

            auto has_ttl = readLE<uint8_t>(in_);
            if (!has_ttl.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot has_ttl"));
            }

            auto freq = readLE<uint64_t>(in_);
            if (!freq.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot freq"));
            }

            bool tombstone = false;
            if (is_v2) {
                auto tombstone_byte = readLE<uint8_t>(in_);
                if (!tombstone_byte.has_value() || tombstone_byte.value() > 1) {
                    span.setStatus(SpanStatus::Error);
                    return err(Error(Errc::CorruptData, "truncated snapshot tombstone"));
                }
                tombstone = tombstone_byte.value() != 0;
            }

            SnapshotEntry entry{
                .key = std::move(key.value()),
                .value = std::move(value.value()),
                .version = version.value(),
                .writer_node_hash = writer_hash.value(),
                .expires_at_ms = expires_ms.value(),
                .has_ttl = has_ttl.value() != 0,
                .tombstone = tombstone,
                .freq = freq.value(),
            };

            fn(entry);
            ++streamed;
        }

        span.setAttribute({"entries", streamed});
        span.setStatus(SpanStatus::Ok);
        return ok();
    }

    auto SnapshotReader::readAll() -> Result<SnapshotData> {
        SnapshotData data;
        auto result = readStream(data.next_version, [&](SnapshotEntry& entry) {
            data.entries.push_back(std::move(entry));
        });
        if (!result.has_value()) {
            return err<SnapshotData>(result.error());
        }
        return data;
    }
} // namespace cinder
