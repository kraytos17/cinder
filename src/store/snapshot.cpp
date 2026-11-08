#include "cinder/store/snapshot.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <limits>
#include <xxhash.h>
#include <zstd.h>

#include "cinder/common/tracing.hpp"
#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {
    namespace {
        // One entry's framing. Field order/widths are the frozen CSNP
        // layout shared by the v2 writer, the v3 writer, and the reader.
        [[nodiscard]]
        auto encodedEntrySize(size_t key_len, size_t val_len) -> size_t {
            return sizeof(uint32_t)
                + key_len
                + sizeof(uint32_t)
                + val_len
                + 4
                * sizeof(uint64_t)
                + 2
                * sizeof(uint8_t); // has_ttl + tombstone
        }

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

        // Parse one entry from a decoded byte range. Mirrors the v2
        // streaming field sequence. False means truncated *or* corrupt —
        // the caller cannot tell them apart (a failed read consumes
        // nothing), so it always decodes the next block and retries,
        // bounded by remaining file bytes and the declared uncompressed
        // budget; only block validation failures and running out of
        // blocks are errors.
        auto parseSnapshotEntry(BufferReader& in, SnapshotEntry& out) -> bool {
            uint32_t key_len = 0;
            std::string key;
            if (!in.read(key_len) || !in.readString(key_len, key)) {
                return false;
            }

            uint32_t val_len = 0;
            std::string value;
            if (!in.read(val_len) || !in.readString(val_len, value)) {
                return false;
            }

            uint64_t version = 0;
            uint64_t writer = 0;
            uint64_t expires = 0;
            uint8_t has_ttl = 0;
            uint64_t freq = 0;
            uint8_t tomb = 0;
            if (!in.read(version)
                || !in.read(writer)
                || !in.read(expires)
                || !in.read(has_ttl)
                || !in.read(freq)
                || !in.read(tomb)
                || tomb
                > 1) {
                return false;
            }

            out.key = std::move(key);
            out.value = std::move(value);
            out.version = version;
            out.writer_node_hash = writer;
            out.expires_at_ms = expires;
            out.has_ttl = has_ttl != 0;
            out.tombstone = tomb != 0;
            out.freq = freq;
            return true;
        }

        // v3 block framing: one independently compressed block per chunk.
        // Encode-time knob; the wire uses ZSTD level 1 (fast path, cold
        // data — recovery reads once, compaction writes once a minute).
        constexpr int K_SNAPSHOT_ZSTD_LEVEL = 1;

        // A block that compresses poorly is stored raw: skipping the
        // few-bytes win keeps incompressible data (already-compressed
        // values, random bytes) from growing.
        auto emitSnapshotBlock(
            std::ostream& out,
            const std::byte* plain,
            size_t plain_len,
            std::vector<std::byte>& scratch
        ) -> bool {
            scratch.resize(ZSTD_compressBound(plain_len));
            const size_t compressed = ZSTD_compress(
                scratch.data(),
                scratch.size(),
                plain,
                plain_len,
                K_SNAPSHOT_ZSTD_LEVEL
            );

            const bool use_compressed = !ZSTD_isError(compressed) && compressed < plain_len;
            const SnapshotBlockCodec codec =
                use_compressed ? SnapshotBlockCodec::Zstd : SnapshotBlockCodec::Raw;
            const size_t stored_len = use_compressed ? compressed : plain_len;
            const void* payload = use_compressed ? static_cast<const void*>(scratch.data())
                                                 : static_cast<const void*>(plain);

            std::vector<std::byte> header(K_SNAPSHOT_BLOCK_HEADER_BYTES);
            detail::BufferWriter hw(header);
            hw.write(static_cast<uint8_t>(codec));
            hw.write(static_cast<uint32_t>(stored_len));
            hw.write(static_cast<uint32_t>(plain_len));
            // Checksum covers the uncompressed bytes in both cases, so the
            // reader verifies after decoding regardless of codec.
            hw.write(XXH64(plain, plain_len, 0));
            out.write(
                reinterpret_cast<const char*>(header.data()),
                static_cast<std::streamsize>(header.size())
            );

            out.write(static_cast<const char*>(payload), static_cast<std::streamsize>(stored_len));
            return static_cast<bool>(out);
        }

        // Decode one v3 block and append its plaintext to staging. Returns
        // false on truncation (short stream) or corruption (bad codec,
        // oversize block, checksum mismatch, zstd error). Every block is
        // bounded by K_SNAPSHOT_BLOCK_BYTES uncompressed, so no single
        // allocation can blow up recovery regardless of header claims;
        // `budget` additionally caps total decoded output at the declared
        // uncompressed size.
        auto decodeV3Block(
            std::istream& in,
            uint64_t& left,
            std::vector<std::byte>& staging,
            uint64_t& budget,
            Span& span
        ) -> Result<bool> {
            auto take = [&](size_t n) -> bool {
                if (n > left) {
                    return false;
                }
                left -= n;
                return true;
            };
            auto fail = [&](Error e) -> Result<bool> {
                span.setStatus(SpanStatus::Error);
                return err<bool>(std::move(e));
            };

            std::array<char, K_SNAPSHOT_BLOCK_HEADER_BYTES> hdr{};
            if (!take(hdr.size())
                || !in.read(hdr.data(), static_cast<std::streamsize>(hdr.size()))
                || static_cast<size_t>(in.gcount())
                != hdr.size()) {
                return fail(Error(Errc::CorruptData, "truncated snapshot block"));
            }

            BufferReader hb(hdr.data(), hdr.size());
            uint8_t codec = 0;
            uint32_t stored = 0;
            uint32_t uncompressed = 0;
            uint64_t checksum = 0;
            if (!hb.read(codec)
                || !hb.read(stored)
                || !hb.read(uncompressed)
                || !hb.read(checksum)) {
                return fail(Error(Errc::CorruptData, "truncated snapshot block"));
            }
            if (codec
                != static_cast<uint8_t>(SnapshotBlockCodec::Raw)
                && codec
                != static_cast<uint8_t>(SnapshotBlockCodec::Zstd)) {
                return fail(Error(Errc::CorruptData, "unknown snapshot block codec"));
            }
            if (stored
                == 0
                || uncompressed
                == 0
                || uncompressed
                > K_SNAPSHOT_BLOCK_BYTES
                || uncompressed
                > budget) {
                return fail(Error(Errc::CorruptData, "corrupt snapshot block"));
            }
            if (codec == static_cast<uint8_t>(SnapshotBlockCodec::Raw) && stored != uncompressed) {
                return fail(Error(Errc::CorruptData, "corrupt snapshot block"));
            }

            std::vector<std::byte> payload(stored);
            if (!take(stored)
                || !in.read(
                    reinterpret_cast<char*>(payload.data()),
                    static_cast<std::streamsize>(stored)
                )
                || static_cast<size_t>(in.gcount())
                != stored) {
                return fail(Error(Errc::CorruptData, "truncated snapshot block"));
            }

            const void* plain = payload.data();
            std::vector<std::byte> decoded;
            if (codec == static_cast<uint8_t>(SnapshotBlockCodec::Zstd)) {
                decoded.resize(uncompressed);
                const size_t out_len =
                    ZSTD_decompress(decoded.data(), decoded.size(), payload.data(), payload.size());
                if (ZSTD_isError(out_len) || out_len != uncompressed) {
                    return fail(Error(Errc::CorruptData, "corrupt snapshot block"));
                }
                plain = decoded.data();
            }
            if (XXH64(plain, uncompressed, 0) != checksum) {
                return fail(Error(Errc::CorruptData, "corrupt snapshot block"));
            }

            budget -= uncompressed;
            staging.insert(
                staging.end(),
                reinterpret_cast<const std::byte*>(plain),
                reinterpret_cast<const std::byte*>(plain) + uncompressed
            );
            return true;
        }

        // Streaming v3 body read: decode blocks on demand into a staging
        // buffer and parse entries out of it. The staging buffer holds at
        // most one block plus one partial entry, so recovery stays bounded
        // no matter how large the snapshot is.
        auto readCompressedBody(
            std::istream& in,
            uint64_t& left,
            uint32_t entry_count,
            std::move_only_function<void(SnapshotEntry&)> fn,
            Span& span
        ) -> Result<void> {
            auto fail = [&](Error e) -> Result<void> {
                span.setStatus(SpanStatus::Error);
                return err(std::move(e));
            };

            uint64_t uncompressed_total = 0;
            if (auto v = readLE<uint64_t>(in); v.has_value() && left >= sizeof(uint64_t)) {
                left -= sizeof(uint64_t);
                uncompressed_total = v.value();
            } else {
                return fail(Error(Errc::CorruptData, "truncated snapshot header"));
            }

            // v3 always carries tombstones: 42 bytes/entry minimum.
            constexpr size_t K_MIN_ENTRY_BYTES = 42;
            if (entry_count > uncompressed_total / K_MIN_ENTRY_BYTES) {
                return fail(Error(Errc::InvalidArgument, "entry_count exceeds declared size"));
            }

            std::vector<std::byte> staging;
            size_t staging_pos = 0;
            uint64_t budget = uncompressed_total;
            size_t streamed = 0;
            while (streamed < entry_count) {
                SnapshotEntry entry;
                while (true) {
                    BufferReader body(
                        reinterpret_cast<const char*>(staging.data()) + staging_pos,
                        staging.size() - staging_pos
                    );
                    if (parseSnapshotEntry(body, entry)) {
                        staging_pos += body.pos();
                        break;
                    }
                    // A failed parse means either truncation (more blocks will
                    // complete the entry) or corruption (validation inside
                    // decodeV3Block fails). Retry with the next block; the
                    // loop is bounded by remaining file bytes and the
                    // declared uncompressed budget, so it cannot spin.
                    if (staging_pos > 0) {
                        staging.erase(
                            staging.begin(),
                            staging.begin()
                                + static_cast<std::vector<std::byte>::difference_type>(staging_pos)
                        );
                        staging_pos = 0;
                    }
                    if (left == 0) {
                        return fail(Error(Errc::CorruptData, "truncated snapshot entry"));
                    }

                    auto decoded = decodeV3Block(in, left, staging, budget, span);
                    if (!decoded.has_value()) {
                        return err(decoded.error());
                    }
                    if (!decoded.value()) {
                        return fail(Error(Errc::CorruptData, "truncated snapshot entry"));
                    }
                }

                fn(entry);
                ++streamed;
            }

            span.setAttribute({"entries", streamed});
            span.setStatus(SpanStatus::Ok);
            return ok();
        }
    } // namespace

    SnapshotWriter::SnapshotWriter(const std::string& path, bool compress) :
        path_(path),
        compress_(compress) {}

    auto SnapshotWriter::write(Version next_version, std::span<const SnapshotEntry> entries)
        -> Result<void> {
        Span span("snapshot.write", captureTraceContext());
        std::ofstream out(path_ + ".tmp", std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::InternalError, "cannot open snapshot file for writing"));
        }

        // Header in one write: magic + version + next_version + entry_count,
        // plus the total uncompressed body size for v3 (lets the reader
        // validate entry_count and bound buffers before decoding).
        const SnapshotFormat format = compress_ ? SnapshotFormat::V3 : SnapshotFormat::V2;
        uint64_t uncompressed_total = 0;
        if (snapshotIsCompressed(format)) {
            for (const auto& entry : entries) {
                uncompressed_total += encodedEntrySize(entry.key.size(), entry.value.size());
            }
        }

        scratch_.resize(
            sizeof(uint32_t)
            * 3
            + sizeof(uint64_t)
            + (snapshotIsCompressed(format) ? sizeof(uint64_t) : 0)
        );

        {
            BufferWriter header(scratch_);
            header.write(K_SNAPSHOT_MAGIC);
            header.write(static_cast<uint32_t>(format));
            header.write(next_version);
            header.write(static_cast<uint32_t>(entries.size()));
            if (snapshotIsCompressed(format)) {
                header.write(uncompressed_total);
            }
        }

        out.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );
        if (compress_) {
            if (auto body = writeCompressedEntries(out, entries); !body.has_value()) {
                span.setStatus(SpanStatus::Error);
                return body;
            }
        } else {
            // Uncompressed v2 body below: batch entries into bounded
            // chunks, one stream write per chunk instead of per entry.
            // Same bytes, same order. An entry larger than the chunk
            // bypasses it (after flushing) so memory stays bounded no
            // matter the value sizes. Field order, widths, and
            // native-endian layout are unchanged, so files stay
            // byte-identical.
            std::vector<std::byte> chunk(K_SNAPSHOT_BLOCK_BYTES);
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

                if (size > K_SNAPSHOT_BLOCK_BYTES) {
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
        } // else: uncompressed v2 body above

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

    auto SnapshotWriter::writeCompressedEntries(
        std::ostream& out,
        std::span<const SnapshotEntry> entries
    ) -> Result<void> {
        // Same encoded entries as v2, packed into block-sized chunks and
        // emitted as independent blocks. Entries of any size split across
        // chunk boundaries, so unlike v2 there is no huge-entry bypass:
        // every block carries at most K_SNAPSHOT_BLOCK_BYTES uncompressed.
        std::vector<std::byte> chunk(K_SNAPSHOT_BLOCK_BYTES);
        size_t chunk_pos = 0;
        std::vector<std::byte> cbuf;
        auto flush_chunk = [&] -> bool {
            if (chunk_pos == 0) {
                return true;
            }

            const bool ok = emitSnapshotBlock(out, chunk.data(), chunk_pos, cbuf);
            chunk_pos = 0;
            return ok;
        };
        auto append_bytes = [&](const char* data, size_t n) -> bool {
            while (n > 0) {
                size_t space = chunk.size() - chunk_pos;
                if (space == 0) {
                    if (!flush_chunk()) {
                        return false;
                    }
                    space = chunk.size();
                }

                const size_t take = std::min(n, space);
                std::memcpy(chunk.data() + chunk_pos, data, take);
                chunk_pos += take;
                data += take;
                n -= take;
            }
            return true;
        };

        for (const auto& entry : entries) {
            if (entry.key.size()
                > std::numeric_limits<uint32_t>::max()
                || entry.value.size()
                > std::numeric_limits<uint32_t>::max()) {
                return err(Error(Errc::InvalidArgument, "snapshot entry too large"));
            }

            const auto key_len = static_cast<uint32_t>(entry.key.size());
            const auto val_len = static_cast<uint32_t>(entry.value.size());
            scratch_.resize(encodedEntrySize(key_len, val_len));
            BufferWriter body(scratch_);

            encodeSnapshotEntry(body, entry, key_len, val_len);
            if (!append_bytes(reinterpret_cast<const char*>(scratch_.data()), scratch_.size())) {
                return err(Error(Errc::InternalError, "snapshot write failed"));
            }
        }
        if (!flush_chunk()) {
            return err(Error(Errc::InternalError, "snapshot write failed"));
        }
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
        const auto format =
            format_ver.has_value() ? parseSnapshotFormat(format_ver.value()) : std::nullopt;
        if (!format.has_value()) {
            span.setStatus(SpanStatus::Error);
            return err(Error(Errc::CorruptData, "unsupported snapshot format version"));
        }

        const bool has_tombstone = snapshotHasTombstone(format.value());
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
        // 42 bytes v2 (trailing tombstone byte). v3 validates against the
        // declared uncompressed size instead (see below).
        constexpr size_t K_MIN_ENTRY_BYTES_V1 = 41;
        constexpr size_t K_MIN_ENTRY_BYTES_V2 = 42;
        const size_t min_entry_bytes = has_tombstone ? K_MIN_ENTRY_BYTES_V2 : K_MIN_ENTRY_BYTES_V1;
        auto pos = in_.tellg();
        in_.seekg(0, std::ios::end);

        auto file_size = in_.tellg();
        in_.seekg(pos);
        auto remaining = static_cast<uint64_t>(file_size - pos);
        const bool compressed = snapshotIsCompressed(format.value());
        if (!compressed
            && static_cast<uint64_t>(entry_count.value())
            > remaining
            / min_entry_bytes) {
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
        if (snapshotIsCompressed(format.value())) {
            return readCompressedBody(in_, left, entry_count.value(), std::move(fn), span);
        }

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
            const size_t tail_size = has_tombstone ? K_TAIL_BYTES : K_TAIL_BYTES - 1;
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
            if (has_tombstone) {
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
