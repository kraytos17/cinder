#pragma once

// Quorum-read coordination: singleflight registry plus an LWW reducer.
//
// The registry coalesces concurrent reads for the same key so one fan-out
// serves all waiters. The reducer accumulates local + replica votes into a
// single LWW winner using the shared comparator (see compareLww): version,
// then writer, then tombstone-wins on exact-stamp ties. Tombstone votes
// count as versioned votes; mapping a tombstone winner to NotFound is the
// caller's job (transport/user-facing concern, not LWW policy).
//
// Flights are keyed by (key, read quorum): waiters with different R never
// share a flight, since the leader's R decides the outcome. Replica-set
// differences across concurrent reads are tolerated (any quorum answer is a
// valid LWW result); the leader's set wins.

#include <ankerl/unordered_dense.h>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cinder/cluster/clock.hpp"
#include "cinder/common/status.hpp"
#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

    // One in-flight coalesced read. Waiters share the leader's LWW result.
    template <typename ReadCallback> struct ReadFlight {
        std::vector<ReadCallback> waiters;
    };

    // Flight identity: key plus the read quorum the leader is collecting.
    struct ReadFlightKey {
        std::string key;
        size_t r = 0;

        bool operator==(const ReadFlightKey&) const = default;
    };

    struct ReadFlightKeyHash {
        auto operator()(const ReadFlightKey& k) const noexcept -> size_t {
            // FNV-1a over the key, then a splitmix64-style combine with r.
            uint64_t h = 14'695'981'039'346'656'037ULL;
            for (char c : k.key) {
                h ^= static_cast<uint64_t>(static_cast<uint8_t>(c));
                h *= 1'099'511'628'211ULL;
            }

            uint64_t z = static_cast<uint64_t>(k.r) + 0x9E3779B97F4A7C15ULL;
            z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
            z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
            z ^= z >> 31U;
            return static_cast<size_t>(h ^ z);
        }
    };

    // Singleflight registry: at most one fan-out per (key, R). The first
    // caller becomes the leader and runs the reducer; later arrivals attach
    // as waiters and receive the leader's LWW result. Waiters never block.
    // Leaf-level mutex, held only for map insert/find/erase — never across
    // I/O or store locks.
    template <typename ReadCallback, typename ReadResult> class ReadFlightRegistry {
    public:

        using Flight = ReadFlight<ReadCallback>;

        // Attach a callback. Returns the flight and whether this caller is
        // the leader (true) or an attached waiter (false).
        auto attach(const std::string& key, size_t r, ReadCallback cb)
            -> std::pair<std::shared_ptr<Flight>, bool> {
            std::scoped_lock lock(mutex_);
            ReadFlightKey id{key, r};
            auto it = flights_.find(id);
            if (it == flights_.end()) {
                auto flight = std::make_shared<Flight>();
                flight->waiters.push_back(std::move(cb));
                flights_.emplace(std::move(id), flight);
                return {std::move(flight), true};
            }

            it->second->waiters.push_back(std::move(cb));
            return {it->second, false};
        }

        // Complete a coalesced read: erase-then-notify so a waiter that
        // synchronously starts a new read for the same key begins a fresh
        // flight instead of deadlocking on itself.
        void finish(
            const std::string& key,
            size_t r,
            const std::shared_ptr<Flight>& flight,
            ReadResult result
        ) {
            std::vector<ReadCallback> waiters;
            {
                std::scoped_lock lock(mutex_);
                auto it = flights_.find(ReadFlightKey{key, r});
                if (it != flights_.end() && it->second == flight) {
                    flights_.erase(it);
                }
                waiters = std::move(flight->waiters);
            }

            // All but the last waiter get a copy; the last takes the move. A
            // single-waiter flight (the common case) moves with zero copies.
            // (Structured without a conditional move inside the loop: the
            // use-after-move check cannot prove a last-iteration-only move
            // safe.)
            for (size_t i = 0; i + 1 < waiters.size(); ++i) {
                waiters[i](result);
            }
            if (!waiters.empty()) {
                waiters.back()(std::move(result));
            }
        }

    private:

        std::mutex mutex_;
        ankerl::unordered_dense::map<ReadFlightKey, std::shared_ptr<Flight>, ReadFlightKeyHash>
            flights_;
    };

    // Single-consumer LWW reduce over one quorum read. Best_* is a compound
    // maximum updated by one reader at a time, so no mutex is needed.
    // Post-decision arrivals are ignored (the async tail drains them); the
    // loop still drains so no transport callback outlives the operation.
    class ReadReducer {
    public:

        ReadReducer(size_t r, size_t total, std::optional<VersionedEntry> local, Clock& clock) :
            r_(r),
            pending_(total),
            clock_(clock) {
            if (local.has_value()) {
                ++acks_;
                best_version_ = local->version();
                best_writer_ = local->writer_node_hash;
                best_tombstone_ = local->isTombstone();
                best_entry_ = std::move(local);
            }
        }

        ReadReducer(const ReadReducer&) = delete;
        auto operator=(const ReadReducer&) -> ReadReducer& = delete;
        ReadReducer(ReadReducer&&) = default;
        auto operator=(ReadReducer&&) -> ReadReducer& = delete;

        // Fold one replica response into the reduce. Inactive (post-decision)
        // completions are ignored entirely, mirroring the previous inline
        // behavior — except the shared fan-out still drains them.
        void noteRemote(Result<net::Response> r, bool active) {
            if (!active) {
                return;
            }

            --pending_;
            if (!r.has_value()) {
                return;
            }
            if (r->status == Errc::OK && r->version != 0) {
                ++acks_;
                const LwwStamp vote{r->version, r->writer_node_hash, r->tombstone};
                const LwwStamp best{best_version_, best_writer_, best_tombstone_};
                if (lwwWinsOver(vote, best)) {
                    needs_repair_ = true;
                    best_version_ = r->version;
                    best_writer_ = r->writer_node_hash;
                    best_tombstone_ = r->tombstone;

                    VersionedEntry entry;
                    // Tombstone votes carry no value (GetVersioned omits it);
                    // the marker itself is the vote.
                    if (r->value.has_value()) {
                        entry.value = std::move(*r->value);
                    }

                    entry.setVersion(r->version);
                    entry.writer_node_hash = r->writer_node_hash;
                    entry.setTombstone(r->tombstone);
                    if (r->expires_at.has_value()) {
                        entry.setHasTtl(true);
                        entry.expires_at = toSteadyExpiry(clock_, *r->expires_at);
                    }
                    best_entry_ = std::move(entry);
                } else if (compareLww(vote, best) != LwwOrder::Equal) {
                    needs_repair_ = true;
                }
            } else if (r->status == Errc::NotFound) {
                // Replica doesn't have the key — stale, needs repair.
                needs_repair_ = true;
            }
        }

        // Fire the decision when quorum is met, or when every replica has
        // answered (fail-fast callers add their own impossibility check).
        [[nodiscard]]
        auto shouldFire() const -> bool {
            return acks_ >= r_ || pending_ == 0;
        }

        [[nodiscard]]
        auto acks() const -> size_t {
            return acks_;
        }

        [[nodiscard]]
        auto needsRepair() const -> bool {
            return needs_repair_;
        }

        // Consume the winner. Call once after shouldFire().
        [[nodiscard]]
        auto takeBest() -> std::optional<VersionedEntry> {
            return std::move(best_entry_);
        }

    private:

        size_t r_ = 0;
        size_t pending_ = 0;
        size_t acks_ = 0;
        Version best_version_ = 0;
        uint64_t best_writer_ = 0;
        bool best_tombstone_ = false;
        bool needs_repair_ = false;
        std::optional<VersionedEntry> best_entry_;
        Clock& clock_;
    };
} // namespace cinder
