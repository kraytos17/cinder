#include "cinder/store/wal.hpp"

#include <sstream>
#include <xxhash.h>

#include "cinder/common/logger.hpp"
#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {

WalWriter::WalWriter(const std::string& path)
    : out_(path, std::ios::binary | std::ios::trunc) {
    writeU32(out_, K_WAL_MAGIC);
    writeU32(out_, K_WAL_FORMAT_VERSION);
}

WalWriter::~WalWriter() {
    if (out_.is_open()) {
        out_.flush();
    }
}

auto
WalWriter::append(const WalEntry& entry) -> Result<void> {
    if (!out_.is_open()) {
        return err(Error(Errc::InternalError, "WAL file not open"));
    }

    std::ostringstream buf;
    auto key_len = static_cast<uint32_t>(entry.key.size());
    auto val_len = static_cast<uint32_t>(entry.value.size());
    auto write_buf_u8 = [&buf](uint8_t v) {
        buf.write(reinterpret_cast<const char*>(&v), sizeof(v));
    };
    auto write_buf_u32 = [&buf](uint32_t v) {
        buf.write(reinterpret_cast<const char*>(&v), sizeof(v));
    };
    auto write_buf_u64 = [&buf](uint64_t v) {
        buf.write(reinterpret_cast<const char*>(&v), sizeof(v));
    };

    write_buf_u8(static_cast<uint8_t>(entry.op));
    write_buf_u32(key_len);
    buf.write(entry.key.data(), static_cast<std::streamsize>(key_len));

    write_buf_u32(val_len);
    buf.write(entry.value.data(), static_cast<std::streamsize>(val_len));

    write_buf_u64(entry.version);
    write_buf_u64(entry.writer_node_hash);
    write_buf_u64(entry.expires_at_ms);
    write_buf_u8(entry.has_ttl ? 1 : 0);

    auto entry_str = buf.str();
    auto digest = XXH3_64bits(entry_str.data(), entry_str.size());
    out_.write(entry_str.data(), static_cast<std::streamsize>(entry_str.size()));
    writeU64(out_, digest);
    if (!out_.good()) {
        return err(Error(Errc::InternalError, "WAL write failed"));
    }
    return ok();
}

void
WalWriter::flush() {
    if (out_.is_open()) {
        out_.flush();
    }
}

WalReader::WalReader(const std::string& path)
    : in_(path, std::ios::binary) {}

auto
WalReader::next() -> std::optional<WalEntry> {
    if (!header_checked_) {
        header_checked_ = true;
        auto magic = readU32(in_);
        if (magic.has_value() && magic.value() == K_WAL_MAGIC) {
            auto version = readU32(in_);
            if (version.has_value() && version.value() == K_WAL_FORMAT_VERSION) {
                has_checksums_ = true;
            } else {
                // Unknown version — rewind past magic so old-format reads work.
                in_.seekg(0);
            }
        } else {
            // No magic — old format. Seek back to start.
            in_.seekg(0);
        }
    }
    if (has_checksums_) {
        auto op_byte = readU8(in_);
        if (!op_byte.has_value()) {
            return std::nullopt;
        }

        auto op = static_cast<WalEntry::Op>(op_byte.value());
        if (op != WalEntry::Op::Set && op != WalEntry::Op::Del) {
            return std::nullopt;
        }

        auto key_len = readU32(in_);
        if (!key_len.has_value()) {
            return std::nullopt;
        }

        auto key = readString(in_, key_len.value());
        if (!key.has_value()) {
            return std::nullopt;
        }

        auto val_len = readU32(in_);
        if (!val_len.has_value()) {
            return std::nullopt;
        }

        auto value = readString(in_, val_len.value());
        if (!value.has_value()) {
            return std::nullopt;
        }

        auto version = readU64(in_);
        if (!version.has_value()) {
            return std::nullopt;
        }

        auto writer_hash = readU64(in_);
        if (!writer_hash.has_value()) {
            return std::nullopt;
        }

        auto expires_ms = readU64(in_);
        if (!expires_ms.has_value()) {
            return std::nullopt;
        }

        auto has_ttl = readU8(in_);
        if (!has_ttl.has_value()) {
            return std::nullopt;
        }

        auto stored_checksum = readU64(in_);
        if (!stored_checksum.has_value()) {
            return std::nullopt; // truncated entry
        }

        XXH3_state_t* state = XXH3_createState();
        XXH3_64bits_reset(state);
        auto op_v = static_cast<uint8_t>(op);
        XXH3_64bits_update(state, &op_v, sizeof(op_v));
        auto kl = static_cast<uint32_t>(key_len.value());
        XXH3_64bits_update(state, &kl, sizeof(kl));
        XXH3_64bits_update(state, key->data(), key->size());
        auto vl = static_cast<uint32_t>(val_len.value());
        XXH3_64bits_update(state, &vl, sizeof(vl));
        XXH3_64bits_update(state, value->data(), value->size());
        auto ver = version.value();
        XXH3_64bits_update(state, &ver, sizeof(ver));
        auto wh = writer_hash.value();
        XXH3_64bits_update(state, &wh, sizeof(wh));
        auto em = expires_ms.value();
        XXH3_64bits_update(state, &em, sizeof(em));
        auto ht = has_ttl.value();
        XXH3_64bits_update(state, &ht, sizeof(ht));
        auto computed = XXH3_64bits_digest(state);
        XXH3_freeState(state);

        if (computed != stored_checksum.value()) {
            Logger::warn("cinder wal: checksum mismatch at entry — corrupt data");
            return std::nullopt;
        }
        return WalEntry{
            .op = op,
            .key = std::move(key.value()),
            .value = std::move(value.value()),
            .version = version.value(),
            .writer_node_hash = writer_hash.value(),
            .expires_at_ms = expires_ms.value(),
            .has_ttl = has_ttl.value() != 0,
        };
    }

    // Legacy format (no checksums).
    auto op_byte = readU8(in_);
    if (!op_byte.has_value()) {
        return std::nullopt;
    }

    auto op = static_cast<WalEntry::Op>(op_byte.value());
    if (op != WalEntry::Op::Set && op != WalEntry::Op::Del) {
        return std::nullopt; // corrupt or unsupported
    }

    auto key_len = readU32(in_);
    if (!key_len.has_value()) {
        return std::nullopt;
    }

    auto key = readString(in_, key_len.value());
    if (!key.has_value()) {
        return std::nullopt;
    }

    auto val_len = readU32(in_);
    if (!val_len.has_value()) {
        return std::nullopt;
    }

    auto value = readString(in_, val_len.value());
    if (!value.has_value()) {
        return std::nullopt;
    }

    auto version = readU64(in_);
    if (!version.has_value()) {
        return std::nullopt;
    }

    auto writer_hash = readU64(in_);
    if (!writer_hash.has_value()) {
        return std::nullopt;
    }

    auto expires_ms = readU64(in_);
    if (!expires_ms.has_value()) {
        return std::nullopt;
    }

    auto has_ttl = readU8(in_);
    if (!has_ttl.has_value()) {
        return std::nullopt;
    }
    return WalEntry{
        .op = op,
        .key = std::move(key.value()),
        .value = std::move(value.value()),
        .version = version.value(),
        .writer_node_hash = writer_hash.value(),
        .expires_at_ms = expires_ms.value(),
        .has_ttl = has_ttl.value() != 0,
    };
}

auto
WalReader::hasMore() -> bool {
    return in_ && in_.peek() != EOF;
}
} // namespace cinder
