#pragma once

#include <algorithm>
#include <array>
#include <atomic>
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

        // Optional observability sinks. ReplicationManager owns the metrics
        // lifetime and wires these before the node begins serving requests.
        void setMetrics(
            std::atomic<uint64_t>* entries,
            std::atomic<uint64_t>* bytes,
            std::atomic<uint64_t>* bytes_peak
        ) {
            std::scoped_lock lock(mutex_);
            entries_metric_ = entries;
            bytes_metric_ = bytes;
            bytes_peak_metric_ = bytes_peak;
            publishMetrics();
        }

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
            current_bytes_ += estimatedBytes(hint);
            slot.hint.emplace(std::move(hint));
            slot.active = true;

            linkAtTail(index);
            ++size_;
            publishMetrics();
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
                publishMetrics();
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
            publishMetrics();
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
                    publishMetrics();
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

        [[nodiscard]]
        auto estimatedBytes() const -> uint64_t {
            std::scoped_lock lock(mutex_);
            return static_cast<uint64_t>(current_bytes_);
        }

        [[nodiscard]]
        auto peakEstimatedBytes() const -> uint64_t {
            std::scoped_lock lock(mutex_);
            return static_cast<uint64_t>(peak_bytes_);
        }

        [[nodiscard]]
        static auto estimatedPayloadBytes(const Hint& hint) -> uint64_t {
            return static_cast<uint64_t>(
                sizeof(Hint) + hint.target.size() + hint.req.key.size() + hint.req.value.size()
            );
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

        static auto estimatedBytes(const Hint& hint) -> size_t {
            return static_cast<size_t>(estimatedPayloadBytes(hint));
        }

        void publishMetrics() {
            if (entries_metric_ != nullptr) {
                entries_metric_->store(static_cast<uint64_t>(size_), std::memory_order_relaxed);
            }
            if (bytes_metric_ != nullptr) {
                bytes_metric_->store(
                    static_cast<uint64_t>(current_bytes_),
                    std::memory_order_relaxed
                );
            }

            peak_bytes_ = std::max(peak_bytes_, current_bytes_);
            if (bytes_peak_metric_ != nullptr) {
                auto peak = bytes_peak_metric_->load(std::memory_order_relaxed);
                while (peak
                       < peak_bytes_
                       && !bytes_peak_metric_->compare_exchange_weak(
                           peak,
                           static_cast<uint64_t>(peak_bytes_),
                           std::memory_order_relaxed
                       )) {
                }
            }
        }

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
            current_bytes_ -= estimatedBytes(*slot.hint);
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
        size_t current_bytes_ = 0;
        size_t peak_bytes_ = 0;
        std::atomic<uint64_t>* entries_metric_ = nullptr;
        std::atomic<uint64_t>* bytes_metric_ = nullptr;
        std::atomic<uint64_t>* bytes_peak_metric_ = nullptr;
    };
} // namespace cinder
