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
    using std::chrono::duration_cast;

    namespace {

        using std::chrono::milliseconds;
        using std::chrono::steady_clock;

        auto sysNowMs() -> uint64_t {
            return static_cast<uint64_t>(
                duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count()
            );
        }

        auto checksum(std::string_view payload) -> uint64_t {
            return XXH3_64bits(payload.data(), payload.size());
        }

    } // namespace

    HintJournal::~HintJournal() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    auto HintJournal::open(const std::string& path, HintQueue& queue) -> uint64_t {
        Span span("hint.journal.open", captureTraceContext());
        std::error_code ec;
        std::filesystem::create_directories(std::filesystem::path(path).parent_path(), ec);
        if (ec) {
            CINDER_WARN("hint journal dir failed", {"path", path}, {"err", ec.message()});
            span.setStatus(SpanStatus::Error);
            return 0;
        }

        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd_ < 0) {
            CINDER_WARN("hint journal open failed", {"path", path});
            span.setStatus(SpanStatus::Error);
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
            if (!detail::readExactly(fd_, &len, sizeof(len))) {
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
            if (!detail::readExactly(fd_, &expected, sizeof(expected))
                || !detail::readExactly(fd_, payload.data(), payload.size())
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
        if (end >= 0 && size > end && ::ftruncate(fd_, end) != 0) {
            CINDER_WARN("hint journal truncate failed", {"path", path});
        }

        CINDER_INFO("hint journal recovered", {"path", path}, {"hints", queue.size()});
        span.setAttribute({"hints", queue.size()});
        span.setStatus(SpanStatus::Ok);
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
        Span span("hint.journal.compact", captureTraceContext());
        if (fd_ < 0 || path_.empty()) {
            span.setStatus(SpanStatus::Error);
            return;
        }

        auto live = queue.snapshot(steady_clock::now());
        const auto tmp_path = path_ + ".tmp";
        int tmp = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (tmp < 0) {
            CINDER_WARN("hint journal compact open failed", {"path", tmp_path});
            span.setStatus(SpanStatus::Error);
            return;
        }

        const uint64_t now_ms = sysNowMs();
        uint64_t kept = 0;
        bool ok = true;
        for (const auto& entry : live) {
            // Preserve remaining TTL rather than granting a fresh one.
            const auto remaining_ms = static_cast<int64_t>(
                duration_cast<milliseconds>(entry.hint.expires_at - steady_clock::now()).count()
            );
            if (remaining_ms <= 0) {
                continue;
            }

            auto payload = encodeAdd(entry.hint, now_ms + static_cast<uint64_t>(remaining_ms));
            uint32_t len = static_cast<uint32_t>(payload.size());
            uint64_t sum = checksum(asChars(payload));
            ok = detail::writeAll(tmp, &len, sizeof(len))
                && detail::writeAll(tmp, &sum, sizeof(sum))
                && detail::writeAll(tmp, payload.data(), payload.size());
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
            span.setStatus(SpanStatus::Error);
            return;
        }

        // Atomic publish also fsyncs the directory, which the old hand-rolled
        // sequence skipped: a crash could previously lose the compact.
        // fd_ is closed first: Windows cannot rename an open file.
        ::close(fd_);
        if (auto published = detail::publishTempFile(tmp_path, path_, "hint journal");
            !published.has_value()) {
            CINDER_WARN(
                "hint journal compact publish failed",
                {"reason", published.error().message()}
            );

            fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
            span.setStatus(SpanStatus::Error);
            return;
        }

        fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
        records_ = kept;
        CINDER_INFO("hint journal compacted", {"path", path_}, {"hints", kept});
        span.setAttribute({"hints", kept});
        span.setStatus(SpanStatus::Ok);
    }

    auto HintJournal::encodeAdd(const HintQueue::Hint& hint, uint64_t sys_expiry_ms)
        -> std::vector<std::byte> {
        std::vector<std::byte> payload;
        auto reserve =
            hint.target.size() + hint.req.key.size() + hint.req.value.size() + 64 + 8 + 1;

        payload.resize(reserve);
        detail::BufferWriter out(payload);
        out.write(K_OP_ADD);
        out.write(sys_expiry_ms);
        out.write(static_cast<uint32_t>(hint.target.size()));
        out.writeString(hint.target);
        out.write(static_cast<uint32_t>(hint.req.key.size()));
        out.writeString(hint.req.key);
        out.write(static_cast<uint32_t>(hint.req.value.size()));
        out.writeString(hint.req.value);
        out.write(hint.req.version);
        out.write(hint.req.writer_node_hash);
        // Trailing entry-expiry (absolute wall-clock ms, 0 = none) and
        // tombstone marker. TTL writes and delete markers keep their
        // deadlines across a journal recovery; without these, a replayed
        // tombstone would never be GC'd (leak) and a replayed TTL write
        // would live forever (pre-existing gap, fixed as a side effect).
        uint64_t entry_expiry_ms = 0;
        if (hint.req.expires_at.has_value()) {
            entry_expiry_ms = static_cast<uint64_t>(
                duration_cast<milliseconds>(hint.req.expires_at->time_since_epoch()).count()
            );
        }

        out.write(entry_expiry_ms);
        out.write(hint.req.tombstone ? uint8_t{1} : uint8_t{0});
        payload.resize(out.offset());
        return payload;
    }

    auto HintJournal::encodeDel(std::string_view target, std::string_view key, uint64_t version)
        -> std::vector<std::byte> {
        std::vector<std::byte> payload;
        payload.resize(target.size() + key.size() + 32);

        detail::BufferWriter out(payload);
        out.write(K_OP_DEL);
        out.write(static_cast<uint32_t>(target.size()));
        out.writeString(target);
        out.write(static_cast<uint32_t>(key.size()));
        out.writeString(key);
        out.write(version);
        payload.resize(out.offset());
        return payload;
    }

    auto HintJournal::parseRecord(std::string_view payload, ParsedRecord& out) -> bool {
        if (payload.empty()) {
            return false;
        }

        detail::BufferReader in(payload.data(), payload.size());
        uint8_t op = 0;
        if (!in.read(op)) {
            return false;
        }
        if (op == K_OP_ADD) {
            uint64_t exp = 0;
            uint32_t tlen = 0;
            if (!in.read(exp) || !in.read(tlen)) {
                return false;
            }

            std::string target;
            uint32_t klen = 0;
            if (!in.readString(tlen, target) || !in.read(klen)) {
                return false;
            }

            std::string key;
            uint32_t vlen = 0;
            if (!in.readString(klen, key) || !in.read(vlen)) {
                return false;
            }

            std::string value;
            uint64_t version = 0;
            uint64_t writer = 0;
            if (!in.readString(vlen, value) || !in.read(version) || !in.read(writer)) {
                return false;
            }

            // New records carry a trailing entry-expiry + tombstone byte.
            // Legacy records (written before tombstones) end here; they
            // decode as live writes, matching their original semantics.
            bool tombstone = false;
            std::optional<system_clock::time_point> entry_expiry;
            if (in.remaining() != 0) {
                uint64_t exp_ms = 0;
                uint8_t tombstone_byte = 0;
                if (!in.read(exp_ms)
                    || !in.read(tombstone_byte)
                    || tombstone_byte
                    > 1
                    || in.remaining()
                    != 0) {
                    return false;
                }
                if (exp_ms != 0) {
                    entry_expiry = system_clock::time_point(milliseconds(exp_ms));
                }
                tombstone = tombstone_byte != 0;
            }

            out.is_add = true;
            out.sys_expiry_ms = exp;
            out.hint.target = std::move(target);
            out.hint.req.opcode = net::Opcode::Replicate;
            out.hint.req.key = std::move(key);
            out.hint.req.value = std::move(value);
            out.hint.req.version = version;
            out.hint.req.writer_node_hash = writer;
            out.hint.req.expires_at = entry_expiry;
            out.hint.req.tombstone = tombstone;
            out.hint.expires_at = steady_clock::now() + milliseconds(30'000);
            return true;
        }
        if (op == K_OP_DEL) {
            uint32_t tlen = 0;
            if (!in.read(tlen)) {
                return false;
            }

            std::string target;
            uint32_t klen = 0;
            if (!in.readString(tlen, target) || !in.read(klen)) {
                return false;
            }

            std::string key;
            uint64_t version = 0;
            if (!in.readString(klen, key) || !in.read(version) || in.remaining() != 0) {
                return false;
            }

            out.is_add = false;
            out.target = std::move(target);
            out.key = std::move(key);
            out.version = version;
            return true;
        }
        return false;
    }

    auto HintJournal::writeRecord(const std::vector<std::byte>& payload) const -> bool {
        uint32_t len = static_cast<uint32_t>(payload.size());
        uint64_t sum = checksum(asChars(payload));
        return detail::writeAll(fd_, &len, sizeof(len))
            && detail::writeAll(fd_, &sum, sizeof(sum))
            && detail::writeAll(fd_, payload.data(), payload.size());
    }
} // namespace cinder
