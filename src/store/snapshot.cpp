#include "cinder/store/snapshot.hpp"

#include <array>
#include <fstream>
#include <limits>

#include "cinder/common/tracing.hpp"
#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {
    namespace {
        // One entry's framing, shared by the chunked path and the huge-entry
        // bypass below. Field order/widths are the frozen CSNP layout.
        void encodeSnapshotEntry(
            detail::BufferWriter& out,
            const SnapshotEntry& entry,
            uint32_t key_len,
            uint32_t val_len
        ) {
            out.write(key_len);
            out.writeString(entry.key);
            out.write(val_len);
            out.writeString(entry.value);
            out.write(entry.version);
            out.write(entry.writer_node_hash);
            out.write(entry.expires_at_ms);
            out.write(entry.has_ttl ? uint8_t{1} : uint8_t{0});
            out.write(entry.freq);
            out.write(entry.tombstone ? uint8_t{1} : uint8_t{0});
        }
    } // namespace

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
        {
            BufferWriter header(scratch_);
            header.write(K_SNAPSHOT_MAGIC);
            header.write(K_SNAPSHOT_FORMAT_VERSION);
            header.write(next_version);
            header.write(static_cast<uint32_t>(entries.size()));
        }

        out.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );

        // Batch entries into bounded chunks: one stream write per chunk
        // instead of per entry. Same bytes, same order. An entry larger
        // than the chunk bypasses it (after flushing) so memory stays
        // bounded no matter the value sizes. Field order, widths, and
        // native-endian layout are unchanged, so files stay byte-identical
        // (WAL0/CSNP magic, fuzz corpora still decode).
        constexpr size_t K_CHUNK_BYTES = 256U << 10U;
        std::vector<std::byte> chunk(K_CHUNK_BYTES);
        size_t chunk_pos = 0;
        auto flush_chunk = [&] {
            if (chunk_pos == 0) {
                return;
            }

            out.write(
                reinterpret_cast<const char*>(chunk.data()),
                static_cast<std::streamsize>(chunk_pos)
            );
            chunk_pos = 0;
        };

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

            if (size > K_CHUNK_BYTES) {
                flush_chunk();
                scratch_.resize(size);
                BufferWriter body(scratch_);
                encodeSnapshotEntry(body, entry, key_len, val_len);
                out.write(
                    reinterpret_cast<const char*>(scratch_.data()),
                    static_cast<std::streamsize>(scratch_.size())
                );
                continue;
            }
            if (chunk_pos + size > chunk.size()) {
                flush_chunk();
            }

            BufferWriter body(chunk, chunk_pos);
            encodeSnapshotEntry(body, entry, key_len, val_len);
            chunk_pos = body.offset();
        }

        flush_chunk();
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
        if (auto published = publishTempFile(path_ + ".tmp", path_, "snapshot");
            !published.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(published.error());
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
        // Remaining-bytes counter instead of a tellg per string: tellg
        // forces a sentry + seekoff query on every key and value.
        uint64_t left = remaining;
        auto take = [&](size_t n) -> bool {
            if (n > left) {
                return false;
            }

            left -= n;
            return true;
        };
        auto read_sized = [&](size_t len) -> std::optional<std::string> {
            if (len > left) {
                return std::nullopt;
            }

            std::string buf(len, '\0');
            if (!in_.read(buf.data(), static_cast<std::streamsize>(len))) {
                return std::nullopt;
            }

            left -= len;
            return buf;
        };
        // Fixed trailing block per entry: version/writer/expires u64,
        // has_ttl u8, freq u64, tombstone u8. One stream read instead of
        // six sentry-guarded readLE calls.
        constexpr size_t K_TAIL_BYTES =
            3 * sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint8_t);

        for (uint32_t i = 0; i < entry_count.value(); ++i) {
            auto key_len = readLE<uint32_t>(in_);
            if (!key_len.has_value() || !take(sizeof(uint32_t))) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot key"));
            }

            auto key = read_sized(key_len.value());
            if (!key.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot key data"));
            }

            auto val_len = readLE<uint32_t>(in_);
            if (!val_len.has_value() || !take(sizeof(uint32_t))) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot value"));
            }

            auto value = read_sized(val_len.value());
            if (!value.has_value()) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot value data"));
            }

            // Fixed trailing block in one stream read: version/writer/
            // expires u64, has_ttl u8, freq u64, plus the v2 tombstone u8.
            // v1 entries have no tombstone byte on disk.
            const size_t tail_size = is_v2 ? K_TAIL_BYTES : K_TAIL_BYTES - 1;
            std::array<char, K_TAIL_BYTES> tail{};
            if (!take(tail_size)
                || !in_.read(tail.data(), static_cast<std::streamsize>(tail_size))) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot entry"));
            }

            BufferReader fixed(tail.data(), tail_size);
            uint64_t version = 0;
            uint64_t writer_hash = 0;
            uint64_t expires_ms = 0;
            uint8_t has_ttl = 0;
            uint64_t freq = 0;
            uint8_t tombstone_byte = 0;
            if (!fixed.read(version)
                || !fixed.read(writer_hash)
                || !fixed.read(expires_ms)
                || !fixed.read(has_ttl)
                || !fixed.read(freq)) {
                span.setStatus(SpanStatus::Error);
                return err(Error(Errc::CorruptData, "truncated snapshot entry"));
            }

            bool tombstone = false;
            if (is_v2) {
                if (!fixed.read(tombstone_byte) || tombstone_byte > 1) {
                    span.setStatus(SpanStatus::Error);
                    return err(Error(Errc::CorruptData, "truncated snapshot tombstone"));
                }
                tombstone = tombstone_byte != 0;
            }

            SnapshotEntry entry{
                .key = std::move(key.value()),
                .value = std::move(value.value()),
                .version = version,
                .writer_node_hash = writer_hash,
                .expires_at_ms = expires_ms,
                .has_ttl = has_ttl != 0,
                .tombstone = tombstone,
                .freq = freq,
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
