#include "cinder/node/hint_journal.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>
#include <xxhash.h>

#include "cinder/common/tracing.hpp"
#include "cinder/store/detail/io_utils.hpp"

namespace cinder {
    namespace {

        using std::chrono::milliseconds;
        using std::chrono::steady_clock;

        auto sysNowMs() -> uint64_t {
            return static_cast<uint64_t>(std::chrono::duration_cast<milliseconds>(
                                             std::chrono::system_clock::now().time_since_epoch()
            )
                                             .count());
        }

        auto checksum(std::string_view payload) -> uint64_t {
            return XXH3_64bits(payload.data(), payload.size());
        }

        auto readExactly(int fd, void* dst, size_t len) -> bool {
            auto* out = static_cast<char*>(dst);
            size_t done = 0;
            while (done < len) {
                auto n = ::read(fd, out + done, len - done);
                if (n <= 0) {
                    return false;
                }
                done += static_cast<size_t>(n);
            }
            return true;
        }

        auto writeAll(int fd, const void* src, size_t len) -> bool {
            const auto* in = static_cast<const char*>(src);
            size_t done = 0;
            while (done < len) {
                auto n = ::write(fd, in + done, len - done);
                if (n < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    return false;
                }
                if (n == 0) {
                    return false;
                }
                done += static_cast<size_t>(n);
            }
            return true;
        }
    } // namespace

    HintJournal::~HintJournal() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    auto HintJournal::open(const std::string& path, HintQueue& queue) -> uint64_t {
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
        if (ec) {
            CINDER_WARN("hint journal dir failed", {"path", path}, {"err", ec.message()});
            return 0;
        }

        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            CINDER_WARN("hint journal open failed", {"path", path});
            return 0;
        }

        path_ = path;
        records_ = 0;
        const uint64_t now_ms = sysNowMs();
        // Sequential scan; the first short read, checksum mismatch or
        // impossible length ends recovery and truncates the torn tail.
        // Lengths are validated against the remaining file size before
        // allocating: a corrupt prefix must not cause a huge allocation.
        while (true) {
            uint32_t len = 0;
            if (!readExactly(fd_, &len, sizeof(len))) {
                break; // clean EOF
            }

            struct stat file_stat{};
            auto pos = ::lseek(fd_, 0, SEEK_CUR);
            if (::fstat(fd_, &file_stat)
                != 0
                || pos
                < 0
                || static_cast<off_t>(len)
                > file_stat.st_size
                - pos
                - static_cast<off_t>(sizeof(uint64_t))) {
                break; // corrupt length: not enough room left for crc+payload
            }

            uint64_t expected = 0;
            std::string payload(len, '\0');
            if (!readExactly(fd_, &expected, sizeof(expected))
                || !readExactly(fd_, payload.data(), payload.size())
                || checksum(payload)
                != expected) {
                break; // torn tail
            }

            ParsedRecord record;
            if (!parseRecord(payload, record)) {
                break;
            }
            if (record.is_add) {
                if (record.sys_expiry_ms > now_ms) {
                    (void)queue.push(std::move(record.hint));
                }
            } else {
                net::Request probe;
                probe.key = std::move(record.key);
                probe.version = record.version;
                queue.remove(record.target, probe);
            }
            ++records_;
        }

        // Heal: drop any torn tail so later appends land on valid history.
        auto end = ::lseek(fd_, 0, SEEK_CUR);
        auto size = ::lseek(fd_, 0, SEEK_END);
        if (end >= 0 && size > end) {
            if (::ftruncate(fd_, end) != 0) {
                CINDER_WARN("hint journal truncate failed", {"path", path});
            }
        }

        CINDER_INFO("hint journal recovered", {"path", path}, {"hints", queue.size()});
        return queue.size();
    }

    void HintJournal::appendAdd(const HintQueue::Hint& hint, uint64_t sys_expiry_ms) {
        if (fd_ < 0) {
            return;
        }

        if (!writeRecord(encodeAdd(hint, sys_expiry_ms))) {
            CINDER_WARN("hint journal append failed", {"path", path_}, {"errno", errno});
            return;
        }
        ++records_;
    }

    void HintJournal::appendDel(std::string_view target, std::string_view key, uint64_t version) {
        if (fd_ < 0) {
            return;
        }

        if (!writeRecord(encodeDel(target, key, version))) {
            CINDER_WARN("hint journal append failed", {"path", path_}, {"errno", errno});
            return;
        }
        ++records_;
    }

    void HintJournal::sync() const {
        if (fd_ >= 0 && ::fsync(fd_) != 0) {
            CINDER_WARN("hint journal sync failed", {"path", path_}, {"errno", errno});
        }
    }

    void HintJournal::compact(HintQueue& queue) {
        if (fd_ < 0 || path_.empty()) {
            return;
        }

        auto live = queue.snapshot(steady_clock::now());
        const auto tmp_path = path_ + ".tmp";
        int tmp = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (tmp < 0) {
            CINDER_WARN("hint journal compact open failed", {"path", tmp_path});
            return;
        }

        const uint64_t now_ms = sysNowMs();
        uint64_t kept = 0;
        bool ok = true;
        for (const auto& entry : live) {
            // Preserve remaining TTL rather than granting a fresh one.
            const auto remaining_ms =
                static_cast<int64_t>(std::chrono::duration_cast<milliseconds>(
                                         entry.hint.expires_at - steady_clock::now()
                )
                                         .count());
            if (remaining_ms <= 0) {
                continue;
            }

            auto payload = encodeAdd(entry.hint, now_ms + static_cast<uint64_t>(remaining_ms));
            uint32_t len = static_cast<uint32_t>(payload.size());
            uint64_t sum = checksum(
                std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size())
            );

            ok = writeAll(tmp, &len, sizeof(len))
                && writeAll(tmp, &sum, sizeof(sum))
                && writeAll(tmp, payload.data(), payload.size());
            if (!ok) {
                break;
            }
            ++kept;
        }
        if (ok) {
            ok = ::fsync(tmp) == 0;
        }

        ::close(tmp);
        if (!ok) {
            ::unlink(tmp_path.c_str());
            CINDER_WARN("hint journal compact write failed", {"path", tmp_path});
            return;
        }

        ::close(fd_);
        std::error_code ec;
        std::filesystem::rename(tmp_path, path_, ec);
        if (ec) {
            CINDER_WARN("hint journal compact rename failed", {"err", ec.message()});
            fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
            return;
        }

        fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
        records_ = kept;
        CINDER_INFO("hint journal compacted", {"path", path_}, {"hints", kept});
    }

    auto HintJournal::encodeAdd(const HintQueue::Hint& hint, uint64_t sys_expiry_ms)
        -> std::vector<std::byte> {
        std::vector<std::byte> payload;
        size_t off = 0;
        auto reserve = hint.target.size() + hint.req.key.size() + hint.req.value.size() + 64;
        payload.resize(reserve);

        detail::writeU8(payload, off, K_OP_ADD);
        detail::writeU64(payload, off, sys_expiry_ms);
        detail::writeU32(payload, off, static_cast<uint32_t>(hint.target.size()));
        detail::writeBytes(payload, off, hint.target.data(), hint.target.size());
        detail::writeU32(payload, off, static_cast<uint32_t>(hint.req.key.size()));
        detail::writeBytes(payload, off, hint.req.key.data(), hint.req.key.size());
        detail::writeU32(payload, off, static_cast<uint32_t>(hint.req.value.size()));
        detail::writeBytes(payload, off, hint.req.value.data(), hint.req.value.size());
        detail::writeU64(payload, off, hint.req.version);
        detail::writeU64(payload, off, hint.req.writer_node_hash);
        payload.resize(off);
        return payload;
    }

    auto HintJournal::encodeDel(std::string_view target, std::string_view key, uint64_t version)
        -> std::vector<std::byte> {
        std::vector<std::byte> payload;
        size_t off = 0;
        payload.resize(target.size() + key.size() + 32);

        detail::writeU8(payload, off, K_OP_DEL);
        detail::writeU32(payload, off, static_cast<uint32_t>(target.size()));
        detail::writeBytes(payload, off, target.data(), target.size());
        detail::writeU32(payload, off, static_cast<uint32_t>(key.size()));
        detail::writeBytes(payload, off, key.data(), key.size());
        detail::writeU64(payload, off, version);
        payload.resize(off);
        return payload;
    }

    auto HintJournal::parseRecord(std::string_view payload, ParsedRecord& out) -> bool {
        if (payload.empty()) {
            return false;
        }

        size_t off = 0;
        auto read_u8 = [&]() -> std::optional<uint8_t> {
            if (off >= payload.size()) {
                return std::nullopt;
            }
            return static_cast<uint8_t>(payload[off++]);
        };
        auto read_u32 = [&]() -> std::optional<uint32_t> {
            if (off + 4 > payload.size()) {
                return std::nullopt;
            }

            uint32_t v = 0;
            std::memcpy(&v, payload.data() + off, 4);
            off += 4;
            return v;
        };
        auto read_u64 = [&]() -> std::optional<uint64_t> {
            if (off + 8 > payload.size()) {
                return std::nullopt;
            }

            uint64_t v = 0;
            std::memcpy(&v, payload.data() + off, 8);
            off += 8;
            return v;
        };
        auto read_str = [&](size_t len) -> std::optional<std::string> {
            if (off + len > payload.size()) {
                return std::nullopt;
            }

            std::string s(payload.substr(off, len));
            off += len;
            return s;
        };

        auto op = read_u8();
        if (!op.has_value()) {
            return false;
        }
        if (*op == K_OP_ADD) {
            auto exp = read_u64();
            auto tlen = read_u32();
            if (!exp.has_value() || !tlen.has_value()) {
                return false;
            }

            auto target = read_str(*tlen);
            auto klen = read_u32();
            if (!target.has_value() || !klen.has_value()) {
                return false;
            }

            auto key = read_str(*klen);
            auto vlen = read_u32();
            if (!key.has_value() || !vlen.has_value()) {
                return false;
            }

            auto value = read_str(*vlen);
            auto version = read_u64();
            auto writer = read_u64();
            if (!value.has_value()
                || !version.has_value()
                || !writer.has_value()
                || off
                != payload.size()) {
                return false;
            }

            out.is_add = true;
            out.sys_expiry_ms = *exp;
            out.hint.target = std::move(*target);
            out.hint.req.opcode = net::Opcode::Replicate;
            out.hint.req.key = std::move(*key);
            out.hint.req.value = std::move(*value);
            out.hint.req.version = *version;
            out.hint.req.writer_node_hash = *writer;
            out.hint.expires_at = steady_clock::now() + milliseconds(30'000);
            return true;
        }
        if (*op == K_OP_DEL) {
            auto tlen = read_u32();
            if (!tlen.has_value()) {
                return false;
            }

            auto target = read_str(*tlen);
            auto klen = read_u32();
            if (!target.has_value() || !klen.has_value()) {
                return false;
            }

            auto key = read_str(*klen);
            auto version = read_u64();
            if (!key.has_value() || !version.has_value() || off != payload.size()) {
                return false;
            }

            out.is_add = false;
            out.target = std::move(*target);
            out.key = std::move(*key);
            out.version = *version;
            return true;
        }
        return false;
    }

    auto HintJournal::writeRecord(const std::vector<std::byte>& payload) const -> bool {
        uint32_t len = static_cast<uint32_t>(payload.size());
        uint64_t sum = checksum(
            std::string_view(reinterpret_cast<const char*>(payload.data()), payload.size())
        );

        return writeAll(fd_, &len, sizeof(len))
            && writeAll(fd_, &sum, sizeof(sum))
            && writeAll(fd_, payload.data(), payload.size());
    }
} // namespace cinder
