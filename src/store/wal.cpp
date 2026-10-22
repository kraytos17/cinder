#include "cinder/store/wal.hpp"

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
};

auto
readEntryFields(std::istream& in) -> std::optional<ParsedEntry> {
    auto op_byte = readU8(in);
    if (!op_byte.has_value()) {
        return std::nullopt;
    }

    auto op = static_cast<WalEntry::Op>(op_byte.value());
    if (op != WalEntry::Op::Set && op != WalEntry::Op::Del) {
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
    };
}

auto
computeEntryChecksum(const ParsedEntry& e) -> uint64_t {
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

    auto digest = XXH3_64bits_digest(state);
    XXH3_freeState(state);
    return digest;
}

auto
toWalEntry(ParsedEntry& e) -> WalEntry {
    return WalEntry{
        .op = e.op,
        .key = std::move(e.key),
        .value = std::move(e.value),
        .version = e.version,
        .writer_node_hash = e.writer_node_hash,
        .expires_at_ms = e.expires_ms,
        .has_ttl = e.has_ttl != 0,
    };
}
} // namespace

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

    // Pre-size scratch buffer: 1(op) + 4(key_len) + key + 4(val_len) + value
    // + 8(version) + 8(writer_node_hash) + 8(expires_at_ms) + 1(has_ttl)
    const size_t size = 1 + 4 + entry.key.size() + 4 + entry.value.size() + 8 + 8 + 8 + 1;
    scratch_.resize(size);

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

    auto digest = XXH3_64bits(scratch_.data(), scratch_.size());
    out_.write(reinterpret_cast<const char*>(scratch_.data()),
        static_cast<std::streamsize>(scratch_.size()));
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
    auto fields = readEntryFields(in_);
    if (!fields.has_value()) {
        return std::nullopt;
    }
    if (!has_checksums_) {
        return toWalEntry(fields.value());
    }

    auto stored_checksum = readU64(in_);
    if (!stored_checksum.has_value()) {
        return std::nullopt; // truncated entry
    }
    if (computeEntryChecksum(fields.value()) != stored_checksum.value()) {
        CINDER_WARN("checksum mismatch at entry — corrupt data");
        return std::nullopt;
    }
    return toWalEntry(fields.value());
}

auto
WalReader::hasMore() -> bool {
    return in_ && in_.peek() != EOF;
}
} // namespace cinder
