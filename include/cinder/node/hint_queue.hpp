#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

    using std::chrono::steady_clock;

    // Bounded FIFO of pending replication hints for unreachable replicas.
    // A mutex protects all queue state. Entries live in a fixed-capacity heap
    // slot array and are linked in FIFO order, so completion callbacks can
    // remove a replayed hint in O(1) using its generation-tagged handle.
    class HintQueue {
    public:

        static constexpr size_t K_CAPACITY = 1024;
        static constexpr size_t K_NONE = K_CAPACITY;

        struct Hint {
            NodeId target;
            net::Request req;
            steady_clock::time_point expires_at;
        };

        struct Handle {
            size_t index = K_NONE;
            uint64_t generation = 0;

            auto operator==(const Handle&) const -> bool = default;
        };

        struct SnapshotEntry {
            Hint hint;
            Handle handle;
        };

        // Allocate the slot storage on the heap: HintQueue is a CacheNodeServer
        // member, and embedding 1024 large Request slots in the server object
        // would inflate its stack footprint.
        HintQueue() :
            slots_(std::make_unique<std::array<Slot, K_CAPACITY>>()) {}

        HintQueue(const HintQueue&) = delete;
        auto operator=(const HintQueue&) -> HintQueue& = delete;

        // Enqueue a hint. If full, discard the oldest queued hint. The array
        // itself never grows after construction.
        auto push(Hint hint) -> bool {
            std::scoped_lock lock(mutex_);
            if (size_ == K_CAPACITY) {
                unlinkAndRelease(head_);
            }

            const size_t index = acquireSlot();
            Slot& slot = (*slots_)[index];
            ++slot.generation;
            slot.hint.emplace(std::move(hint));
            slot.active = true;

            linkAtTail(index);
            ++size_;
            return true;
        }

        // Copy live hints plus stable handles under the lock; callbacks and
        // network sends operate on the snapshot after the lock is released.
        // Expired entries are removed while scanning. Hints remain queued
        // until a successful completion removes their corresponding handle.
        auto snapshot(steady_clock::time_point now) -> std::vector<SnapshotEntry> {
            std::vector<SnapshotEntry> live;
            {
                std::scoped_lock lock(mutex_);
                live.reserve(size_);
                size_t index = head_;
                while (index != K_NONE) {
                    Slot& slot = (*slots_)[index];
                    const size_t next = slot.next;
                    if (slot.hint->expires_at <= now) {
                        unlinkAndRelease(index);
                    } else {
                        live.push_back(
                            SnapshotEntry{
                                .hint = *slot.hint,
                                .handle = Handle{index, slot.generation},
                            }
                        );
                    }
                    index = next;
                }
            }
            return live;
        }

        // Compatibility snapshot visitor. Invoked outside the lock, so a
        // visitor may synchronously call remove().
        template <typename Fn> auto replay(Fn&& fn, steady_clock::time_point now) -> size_t {
            auto live = snapshot(now);
            for (const auto& entry : live) {
                fn(entry.hint);
            }
            return live.size();
        }

        // Remove by generation-tagged handle. If the slot was dropped/reused
        // after snapshot, its generation differs and this stale completion is
        // ignored instead of deleting the replacement hint.
        void remove(Handle handle) {
            std::scoped_lock lock(mutex_);
            if (handle.index >= K_CAPACITY) {
                return;
            }

            Slot& slot = (*slots_)[handle.index];
            if (!slot.active || slot.generation != handle.generation) {
                return;
            }
            unlinkAndRelease(handle.index);
        }

        // Compatibility removal for callers that identify a hint by its
        // target/key/version tuple. Replay completion should prefer Handle.
        void remove(const NodeId& target, const net::Request& req) {
            std::scoped_lock lock(mutex_);
            size_t index = head_;
            while (index != K_NONE) {
                Slot& slot = (*slots_)[index];
                const size_t next = slot.next;
                if (slot.hint->target
                    == target
                    && slot.hint->req.key
                    == req.key
                    && slot.hint->req.version
                    == req.version) {
                    unlinkAndRelease(index);
                    return;
                }
                index = next;
            }
        }

        [[nodiscard]]
        auto size() const -> size_t {
            std::scoped_lock lock(mutex_);
            return size_;
        }

    private:

        struct Slot {
            std::optional<Hint> hint;
            size_t prev = K_NONE;
            size_t next = K_NONE;
            size_t free_next = K_NONE;
            uint64_t generation = 0;
            bool active = false;
        };

        auto acquireSlot() -> size_t {
            if (free_head_ != K_NONE) {
                const size_t index = free_head_;
                free_head_ = (*slots_)[index].free_next;
                (*slots_)[index].free_next = K_NONE;
                return index;
            }
            return next_unused_++;
        }

        void linkAtTail(size_t index) {
            Slot& slot = (*slots_)[index];
            slot.prev = tail_;
            slot.next = K_NONE;
            if (tail_ != K_NONE) {
                (*slots_)[tail_].next = index;
            } else {
                head_ = index;
            }
            tail_ = index;
        }

        void unlinkAndRelease(size_t index) {
            Slot& slot = (*slots_)[index];
            if (slot.prev != K_NONE) {
                (*slots_)[slot.prev].next = slot.next;
            } else {
                head_ = slot.next;
            }
            if (slot.next != K_NONE) {
                (*slots_)[slot.next].prev = slot.prev;
            } else {
                tail_ = slot.prev;
            }

            slot.hint.reset();
            slot.active = false;
            slot.prev = K_NONE;
            slot.next = K_NONE;
            slot.free_next = free_head_;
            free_head_ = index;
            --size_;
        }

        std::unique_ptr<std::array<Slot, K_CAPACITY>> slots_;
        size_t next_unused_ = 0;
        size_t free_head_ = K_NONE;
        size_t head_ = K_NONE;
        size_t tail_ = K_NONE;
        size_t size_ = 0;
        mutable std::mutex mutex_;
    };
} // namespace cinder
