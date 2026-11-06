#include "cinder/store/wal.hpp"

#include <cstring>
#include <limits>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif
#include <xxhash.h>

#include "cinder/common/tracing.hpp"
#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {
    namespace {
        // Parsed WAL record without trailing checksum. Keeps raw u32 lengths so the
        // checksum recomputation uses exactly the on-disk little-endian encoding.
        struct ParsedEntry {
            WalEntry::Op op = WalEntry::Op::Set;
            uint32_t key_len = 0;
            std::string key;
            uint32_t val_len = 0;
            std::string value;
            uint64_t version = 0;
            uint64_t writer_node_hash = 0;
            uint64_t expires_ms = 0;
            uint8_t has_ttl = 0;
            uint8_t tombstone = 0;
        };

        // v2 records carry a trailing tombstone byte after has_ttl; v1
        // records end at has_ttl. The caller gates on the file version.
        // Sets invalid=true when bytes were present but semantically bad
        // (unknown op, tombstone > 1). Truncation (end of image mid-record)
        // leaves invalid=false so callers treat it as a torn tail, not corruption.
        auto readEntryFields(BufferReader& in, bool is_v2, bool& invalid)
            -> std::optional<ParsedEntry> {
            invalid = false;
            uint8_t op_byte = 0;
            if (!in.read(op_byte)) {
                return std::nullopt;
            }

            auto op = static_cast<WalEntry::Op>(op_byte);
            if (op != WalEntry::Op::Set && op != WalEntry::Op::Del) {
                invalid = true;
                return std::nullopt;
            }

            uint32_t key_len = 0;
            if (!in.read(key_len)) {
                return std::nullopt;
            }

            std::string key;
            if (!in.readString(key_len, key)) {
                return std::nullopt;
            }

            uint32_t val_len = 0;
            if (!in.read(val_len)) {
                return std::nullopt;
            }

            std::string value;
            if (!in.readString(val_len, value)) {
                return std::nullopt;
            }

            uint64_t version = 0;
            if (!in.read(version)) {
                return std::nullopt;
            }

            uint64_t writer_hash = 0;
            if (!in.read(writer_hash)) {
                return std::nullopt;
            }

            uint64_t expires_ms = 0;
            if (!in.read(expires_ms)) {
                return std::nullopt;
            }

            uint8_t has_ttl = 0;
            if (!in.read(has_ttl)) {
                return std::nullopt;
            }

            uint8_t tombstone = 0;
            if (is_v2) {
                uint8_t tombstone_byte = 0;
                if (!in.read(tombstone_byte)) {
                    return std::nullopt; // truncated entry
                }
                if (tombstone_byte > 1) {
                    invalid = true;
                    return std::nullopt;
                }
                tombstone = tombstone_byte;
            }
            return ParsedEntry{
                .op = op,
                .key_len = key_len,
                .key = std::move(key),
                .val_len = val_len,
                .value = std::move(value),
                .version = version,
                .writer_node_hash = writer_hash,
                .expires_ms = expires_ms,
                .has_ttl = has_ttl,
                .tombstone = tombstone,
            };
        }

        auto computeEntryChecksum(const ParsedEntry& e, bool is_v2) -> uint64_t {
            XXH3_state_t* state = XXH3_createState();
            XXH3_64bits_reset(state);

            auto op_v = static_cast<uint8_t>(e.op);
            XXH3_64bits_update(state, &op_v, sizeof(op_v));
            XXH3_64bits_update(state, &e.key_len, sizeof(e.key_len));
            XXH3_64bits_update(state, e.key.data(), e.key.size());
            XXH3_64bits_update(state, &e.val_len, sizeof(e.val_len));
            XXH3_64bits_update(state, e.value.data(), e.value.size());
            XXH3_64bits_update(state, &e.version, sizeof(e.version));
            XXH3_64bits_update(state, &e.writer_node_hash, sizeof(e.writer_node_hash));
            XXH3_64bits_update(state, &e.expires_ms, sizeof(e.expires_ms));
            XXH3_64bits_update(state, &e.has_ttl, sizeof(e.has_ttl));
            if (is_v2) {
                XXH3_64bits_update(state, &e.tombstone, sizeof(e.tombstone));
            }

            auto digest = XXH3_64bits_digest(state);
            XXH3_freeState(state);
            return digest;
        }

        auto toWalEntry(ParsedEntry& e) -> WalEntry {
            return WalEntry{
                .op = e.op,
                .key = std::move(e.key),
                .value = std::move(e.value),
                .version = e.version,
                .writer_node_hash = e.writer_node_hash,
                .expires_at_ms = e.expires_ms,
                .has_ttl = e.has_ttl != 0,
                .is_tombstone = e.tombstone != 0,
            };
        }
    } // namespace

    namespace {
        // Encoded record length including the trailing checksum. Must stay
        // exact: append paths pre-size the scratch buffer from it.
        auto encodedRecordSize(const WalEntry& entry) -> size_t {
            return 1
                + 4
                + entry.key.size()
                + 4
                + entry.value.size()
                + 8
                + 8
                + 8
                + 1
                + 1
                + sizeof(uint64_t);
        }

        // Single record encoder shared by append() and appendBatch(): one
        // layout definition, so the two paths cannot drift. The checksum
        // covers the record bytes written so far and is appended in-buffer,
        // keeping the on-disk bytes identical to the historic per-field
        // stream writes (plus the v2 tombstone byte).
        void encodeWalRecord(BufferWriter& out, const WalEntry& entry) {
            const size_t record_start = out.offset();
            out.write(std::to_underlying(entry.op));
            out.write(static_cast<uint32_t>(entry.key.size()));
            out.writeString(entry.key);
            out.write(static_cast<uint32_t>(entry.value.size()));
            out.writeString(entry.value);
            out.write(entry.version);
            out.write(entry.writer_node_hash);
            out.write(entry.expires_at_ms);
            out.write(entry.has_ttl ? uint8_t{1} : uint8_t{0});
            out.write(entry.is_tombstone ? uint8_t{1} : uint8_t{0});
            auto digest =
                XXH3_64bits(out.bytes().data() + record_start, out.offset() - record_start);
            out.write(digest);
        }
    } // namespace

    WalWriter::WalWriter(const std::string& path, WalOpenMode mode) :
        out_(
            path,
            std::ios::binary
                | std::ios::out
                | (mode == WalOpenMode::Truncate ? std::ios::trunc : std::ios::app)
        ) {
        bool needs_header = mode == WalOpenMode::Truncate;
        if (mode == WalOpenMode::Append) {
            // Probe the existing file: valid header → resume after it;
            // missing/empty → fresh header; present-but-invalid → the file is
            // garbage, so fall back to a truncated fresh log.
            std::ifstream probe(path, std::ios::binary);
            auto magic = readLE<uint32_t>(probe);
            auto version = readLE<uint32_t>(probe);
            const bool valid = magic.has_value()
                && magic.value()
                == K_WAL_MAGIC
                && version.has_value()
                && (version.value()
                    == K_WAL_FORMAT_VERSION
                    || version.value()
                    == K_WAL_FORMAT_VERSION_V1);
            if (!magic.has_value()) {
                needs_header = true; // missing or empty file
            } else if (!valid) {
                CINDER_WARN("WAL header invalid; starting fresh log", {"path", path});
                out_.close();
                out_.open(path, std::ios::binary | std::ios::out | std::ios::trunc);
                needs_header = true;
            }
        }
        if (needs_header && out_.is_open()) {
            writeLE(out_, K_WAL_MAGIC);
            writeLE(out_, K_WAL_FORMAT_VERSION);
        }
#ifndef _WIN32
        sync_fd_ = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (sync_fd_ < 0) {
            CINDER_ERROR("WAL sync fd open failed; sync() will be a no-op", {"path", path});
        }
#endif
    }

    WalWriter::~WalWriter() {
        if (out_.is_open()) {
            out_.flush();
        }
#ifndef _WIN32
        if (sync_fd_ >= 0) {
            ::close(sync_fd_);
        }
#endif
    }

    auto WalWriter::append(const WalEntry& entry) -> Result<void> {
        if (!out_.is_open()) {
            return err(Error(Errc::InternalError, "WAL file not open"));
        }
        if (entry.key.size()
            > std::numeric_limits<uint32_t>::max()
            || entry.value.size()
            > std::numeric_limits<uint32_t>::max()) {
            return err(Error(Errc::InvalidArgument, "WAL entry too large"));
        }

        // Pre-size scratch buffer from the exact encoded size; the digest is
        // appended in-buffer so the record lands with a single stream write.
        const size_t size = encodedRecordSize(entry);
        scratch_.resize(size);

        BufferWriter out(scratch_);
        encodeWalRecord(out, entry);
        out_.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );
        if (!out_.good()) {
            return err(Error(Errc::InternalError, "WAL write failed"));
        }
        return ok();
    }

    auto WalWriter::appendBatch(std::span<const WalEntry> entries) -> Result<size_t> {
        if (!out_.is_open()) {
            return err<size_t>(Error(Errc::InternalError, "WAL file not open"));
        }
        if (entries.empty()) {
            return size_t{0};
        }
        for (const auto& entry : entries) {
            if (entry.key.size()
                > std::numeric_limits<uint32_t>::max()
                || entry.value.size()
                > std::numeric_limits<uint32_t>::max()) {
                return err<size_t>(Error(Errc::InvalidArgument, "WAL entry too large"));
            }
        }

        size_t total = 0;
        for (const auto& entry : entries) {
            total += encodedRecordSize(entry);
        }

        scratch_.resize(total);
        BufferWriter out(scratch_);
        for (const auto& entry : entries) {
            encodeWalRecord(out, entry);
        }

        out_.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );
        if (!out_.good()) {
            return err<size_t>(Error(Errc::InternalError, "WAL write failed"));
        }
        return total;
    }

    void WalWriter::flush() {
        if (out_.is_open()) {
            out_.flush();
        }
    }

    auto WalWriter::sync() -> Result<void> {
        flush();
#ifdef _WIN32
        return ok();
#else
        if (sync_fd_ < 0) {
            return err(Error(Errc::InternalError, "WAL sync fd not open"));
        }
        if (::fdatasync(sync_fd_) != 0) {
            return err(Error(Errc::InternalError, "WAL fdatasync failed"));
        }
        return ok();
#endif
    }

    WalReader::WalReader(const std::string& path) {
        // Slurp the whole image once: replay then decodes from memory with
        // zero further syscalls. Segment rotation bounds the image size.
        std::ifstream in(path, std::ios::binary | std::ios::ate);
        if (!in.is_open()) {
            return;
        }

        const auto size = in.tellg();
        if (size <= 0) {
            return;
        }

        in.seekg(0);
        data_.resize(static_cast<size_t>(size));
        if (!in.read(data_.data(), size)) {
            data_.clear();
            return;
        }

        BufferReader header{data_.data(), data_.size()};
        uint32_t magic = 0;
        if (!header.read(magic)) {
            return; // empty image: nothing to decode
        }
        if (magic == K_WAL_MAGIC) {
            uint32_t version = 0;
            if (!header.read(version)) {
                return; // torn header: nothing to decode
            }
            if (version == K_WAL_FORMAT_VERSION || version == K_WAL_FORMAT_VERSION_V1) {
                format_version_ = version;
                pos_ = header.pos();
                return;
            }
            // Unknown version — fall through to legacy decoding from 0.
        }
        // No magic — pre-magic legacy format. Decode from the start.
        pos_ = 0;
    }

    auto WalReader::next() -> std::optional<WalEntry> {
        BufferReader in{data_.data(), data_.size()};
        in.seek(pos_);
        const bool is_v2 = format_version_ == K_WAL_FORMAT_VERSION;
        bool invalid = false;
        auto fields = readEntryFields(in, is_v2, invalid);
        if (!fields.has_value()) {
            // Structurally invalid bytes (bad op / bad tombstone) signal
            // mid-file corruption; short reads are a torn crash tail.
            if (invalid) {
                corrupted_ = true;
            }

            pos_ = in.pos();
            return std::nullopt;
        }
        if (format_version_ == 0) {
            pos_ = in.pos();
            return toWalEntry(fields.value());
        }

        uint64_t stored_checksum = 0;
        if (!in.read(stored_checksum)) {
            pos_ = in.pos();
            return std::nullopt; // truncated entry
        }
        if (computeEntryChecksum(fields.value(), is_v2) != stored_checksum) {
            CINDER_WARN("checksum mismatch at entry — corrupt data");
            corrupted_ = true;
            pos_ = in.pos();
            return std::nullopt;
        }

        pos_ = in.pos();
        return toWalEntry(fields.value());
    }

    auto WalReader::hasMore() -> bool {
        return pos_ < data_.size();
    }
} // namespace cinder
