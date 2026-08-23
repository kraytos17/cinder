#include "cinder/store/wal.hpp"

#include "cinder/store/detail/io_utils.hpp"

using namespace cinder::detail;

namespace cinder {

WalWriter::WalWriter(const std::string& path)
    : out_(path, std::ios::binary | std::ios::trunc) {}

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

    auto key_len = static_cast<uint32_t>(entry.key.size());
    auto val_len = static_cast<uint32_t>(entry.value.size());

    writeU8(out_, static_cast<uint8_t>(entry.op));
    writeU32(out_, key_len);
    out_.write(entry.key.data(), static_cast<std::streamsize>(key_len));
    writeU32(out_, val_len);
    out_.write(entry.value.data(), static_cast<std::streamsize>(val_len));
    writeU64(out_, entry.version);
    writeU64(out_, entry.writer_node_hash);
    writeU64(out_, entry.expires_at_ms);
    writeU8(out_, entry.has_ttl ? 1 : 0);

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
