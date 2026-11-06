#include "cinder/store/wal.hpp"

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
        // (unknown op, tombstone > 1). Truncation (EOF mid-record) leaves
        // invalid=false so callers treat it as a torn tail, not corruption.
        auto readEntryFields(std::istream& in, bool is_v2, bool& invalid)
            -> std::optional<ParsedEntry> {
            invalid = false;
            auto op_byte = readU8(in);
            if (!op_byte.has_value()) {
                return std::nullopt;
            }

            auto op = static_cast<WalEntry::Op>(op_byte.value());
            if (op != WalEntry::Op::Set && op != WalEntry::Op::Del) {
                invalid = true;
                return std::nullopt;
            }

            auto key_len = readU32(in);
            if (!key_len.has_value()) {
                return std::nullopt;
            }

            auto key = readString(in, key_len.value());
            if (!key.has_value()) {
                return std::nullopt;
            }

            auto val_len = readU32(in);
            if (!val_len.has_value()) {
                return std::nullopt;
            }

            auto value = readString(in, val_len.value());
            if (!value.has_value()) {
                return std::nullopt;
            }

            auto version = readU64(in);
            if (!version.has_value()) {
                return std::nullopt;
            }

            auto writer_hash = readU64(in);
            if (!writer_hash.has_value()) {
                return std::nullopt;
            }

            auto expires_ms = readU64(in);
            if (!expires_ms.has_value()) {
                return std::nullopt;
            }

            auto has_ttl = readU8(in);
            if (!has_ttl.has_value()) {
                return std::nullopt;
            }

            uint8_t tombstone = 0;
            if (is_v2) {
                auto tombstone_byte = readU8(in);
                if (!tombstone_byte.has_value() || tombstone_byte.value() > 1) {
                    if (tombstone_byte.has_value()) {
                        invalid = true;
                    }
                    return std::nullopt;
                }
                tombstone = tombstone_byte.value();
            }
            return ParsedEntry{
                .op = op,
                .key_len = key_len.value(),
                .key = std::move(key.value()),
                .val_len = val_len.value(),
                .value = std::move(value.value()),
                .version = version.value(),
                .writer_node_hash = writer_hash.value(),
                .expires_ms = expires_ms.value(),
                .has_ttl = has_ttl.value(),
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
            auto magic = readU32(probe);
            auto version = readU32(probe);
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
            writeU32(out_, K_WAL_MAGIC);
            writeU32(out_, K_WAL_FORMAT_VERSION);
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

        // Pre-size scratch buffer: 1(op) + 4(key_len) + key + 4(val_len) + value
        // + 8(version) + 8(writer_node_hash) + 8(expires_at_ms) + 1(has_ttl)
        // + 1(tombstone) + 8(checksum). Digest appended in-buffer so the
        // record lands with a single stream write; on-disk bytes are unchanged
        // apart from the appended tombstone byte (v2).
        const size_t size = 1 + 4 + entry.key.size() + 4 + entry.value.size() + 8 + 8 + 8 + 1 + 1;
        scratch_.resize(size + sizeof(uint64_t));

        size_t off = 0;
        writeU8(scratch_, off, std::to_underlying(entry.op));
        writeU32(scratch_, off, static_cast<uint32_t>(entry.key.size()));
        writeBytes(scratch_, off, entry.key.data(), entry.key.size());
        writeU32(scratch_, off, static_cast<uint32_t>(entry.value.size()));
        writeBytes(scratch_, off, entry.value.data(), entry.value.size());
        writeU64(scratch_, off, entry.version);
        writeU64(scratch_, off, entry.writer_node_hash);
        writeU64(scratch_, off, entry.expires_at_ms);
        writeU8(scratch_, off, entry.has_ttl ? 1 : 0);
        writeU8(scratch_, off, entry.is_tombstone ? 1 : 0);

        auto digest = XXH3_64bits(scratch_.data(), size);
        writeU64(scratch_, off, digest);
        out_.write(
            reinterpret_cast<const char*>(scratch_.data()),
            static_cast<std::streamsize>(scratch_.size())
        );
        if (!out_.good()) {
            return err(Error(Errc::InternalError, "WAL write failed"));
        }
        return ok();
    }

    namespace {
        // Encoded record length including the trailing checksum, mirroring
        // WalWriter::append so batch sizing stays exact.
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
    } // namespace

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
        size_t off = 0;
        for (const auto& entry : entries) {
            const size_t record_start = off;
            writeU8(scratch_, off, std::to_underlying(entry.op));
            writeU32(scratch_, off, static_cast<uint32_t>(entry.key.size()));
            writeBytes(scratch_, off, entry.key.data(), entry.key.size());
            writeU32(scratch_, off, static_cast<uint32_t>(entry.value.size()));
            writeBytes(scratch_, off, entry.value.data(), entry.value.size());
            writeU64(scratch_, off, entry.version);
            writeU64(scratch_, off, entry.writer_node_hash);
            writeU64(scratch_, off, entry.expires_at_ms);
            writeU8(scratch_, off, entry.has_ttl ? 1 : 0);
            writeU8(scratch_, off, entry.is_tombstone ? 1 : 0);

            auto digest = XXH3_64bits(
                static_cast<const void*>(scratch_.data() + record_start),
                off - record_start
            );
            writeU64(scratch_, off, digest);
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

    WalReader::WalReader(const std::string& path) :
        in_(path, std::ios::binary) {}

    auto WalReader::next() -> std::optional<WalEntry> {
        if (!header_checked_) {
            header_checked_ = true;
            auto magic = readU32(in_);
            if (magic.has_value() && magic.value() == K_WAL_MAGIC) {
                auto version = readU32(in_);
                if (version.has_value()
                    && (version.value()
                        == K_WAL_FORMAT_VERSION
                        || version.value()
                        == K_WAL_FORMAT_VERSION_V1)) {
                    format_version_ = version.value();
                } else {
                    // Unknown version — rewind past magic so old-format reads work.
                    in_.seekg(0);
                }
            } else {
                // No magic — old format. Seek back to start.
                in_.seekg(0);
            }
        }

        const bool is_v2 = format_version_ == K_WAL_FORMAT_VERSION;
        bool invalid = false;
        auto fields = readEntryFields(in_, is_v2, invalid);
        if (!fields.has_value()) {
            // Structurally invalid bytes (bad op / bad tombstone) signal
            // mid-file corruption; short reads are a torn crash tail.
            if (invalid) {
                corrupted_ = true;
            }
            return std::nullopt;
        }
        if (format_version_ == 0) {
            return toWalEntry(fields.value());
        }

        auto stored_checksum = readU64(in_);
        if (!stored_checksum.has_value()) {
            return std::nullopt; // truncated entry
        }
        if (computeEntryChecksum(fields.value(), is_v2) != stored_checksum.value()) {
            CINDER_WARN("checksum mismatch at entry — corrupt data");
            corrupted_ = true;
            return std::nullopt;
        }
        return toWalEntry(fields.value());
    }

    auto WalReader::hasMore() -> bool {
        return in_ && in_.peek() != EOF;
    }
} // namespace cinder
