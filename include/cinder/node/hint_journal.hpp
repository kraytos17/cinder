#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "cinder/node/hint_queue.hpp"

namespace cinder {

    // Append-only journal for hinted-handoff durability across restarts.
    // Lives next to the WAL in the node's data directory (`hints.journal`).
    //
    // Records are enqueue intents (ADD) and successful-replay removals (DEL
    // by target/key/version). Recovery replays records in order through
    // HintQueue::push/remove, so coalescing, byte/entry bounds and LWW
    // ordering reproduce the pre-crash live set: evictions and replacements
    // need no tombstones (recovery re-derives them), while replayed hints
    // stay gone via their DEL. ADDs carry a wall-clock expiry; expired
    // records are skipped on recovery (hint TTL is 30 s, so anything older
    // than a short outage would have expired anyway).
    //
    // Crash window: appends are write()s flushed by the OS page cache; only
    // compaction and shutdown fsync. A torn tail record is truncated on open.
    // Compaction rewrites the journal from a live snapshot once the record
    // count passes K_COMPACT_RECORDS, keeping long outages from growing the
    // file without bound.
    class HintJournal {
    public:

        static constexpr const char* K_FILENAME = "hints.journal";
        static constexpr uint64_t K_COMPACT_RECORDS = 4096;

        HintJournal() = default;
        ~HintJournal();

        HintJournal(const HintJournal&) = delete;
        auto operator=(const HintJournal&) -> HintJournal& = delete;
        HintJournal(HintJournal&&) = delete;
        auto operator=(HintJournal&&) -> HintJournal& = delete;

        // Opens (creating) the journal at `path`, replaying existing records
        // into `queue`. Returns the live recovered hint count.
        auto open(const std::string& path, HintQueue& queue) -> uint64_t;

        void appendAdd(const HintQueue::Hint& hint, uint64_t sys_expiry_ms);
        void appendDel(std::string_view target, std::string_view key, uint64_t version);

        void sync() const;

        [[nodiscard]]
        auto recordCount() const -> uint64_t {
            return records_;
        }

        [[nodiscard]]
        auto needsCompaction() const -> bool {
            return records_ > K_COMPACT_RECORDS;
        }

        // Rewrite the journal from the queue's live snapshot.
        void compact(HintQueue& queue);

    private:

        static constexpr uint8_t K_OP_ADD = 1;
        static constexpr uint8_t K_OP_DEL = 2;

        struct ParsedRecord {
            bool is_add = false;
            HintQueue::Hint hint;
            uint64_t sys_expiry_ms = 0;
            std::string target;
            std::string key;
            uint64_t version = 0;
        };

        static auto encodeAdd(const HintQueue::Hint& hint, uint64_t sys_expiry_ms)
            -> std::vector<std::byte>;
        static auto encodeDel(std::string_view target, std::string_view key, uint64_t version)
            -> std::vector<std::byte>;
        static auto parseRecord(std::string_view payload, ParsedRecord& out) -> bool;

        auto writeRecord(const std::vector<std::byte>& payload) const -> bool;

        int fd_ = -1;
        std::string path_;
        uint64_t records_ = 0;
    };
} // namespace cinder
