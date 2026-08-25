#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <optional>

#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

using std::chrono::steady_clock;

// Lock-free bounded MPSC ring buffer for replication hints.
//
// enqueueHint (io thread):  lock-free push via atomic CAS on head_
// replayHints (main thread): single-threaded iterate + remove
// removeHint  (main thread): single-threaded linear scan
//
// The ring buffer is fixed-size (K_CAPACITY). When full, the oldest hint
// is dropped. This bounds memory and avoids allocation on the hot path.
class HintQueue {
  public:

    static constexpr size_t K_CAPACITY = 1'024;

    struct Hint {
        NodeId target;
        net::Request req;
        steady_clock::time_point expires_at;
    };

    // Lock-free push. Returns false if queue is full (hint dropped).
    auto push(Hint hint) -> bool {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t next = (head + 1) % K_CAPACITY;

        // If full, try to advance tail (drop oldest).
        if (next == tail_.load(std::memory_order_acquire)) {
            // Queue full — drop oldest hint to make room.
            auto old_tail = tail_.load(std::memory_order_relaxed);
            if (tail_.compare_exchange_weak(old_tail,
                    (old_tail + 1) % K_CAPACITY,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
                slots_[old_tail].hint.reset();
                slots_[old_tail].occupied.store(false, std::memory_order_release);
                count_.fetch_sub(1, std::memory_order_relaxed);
            } else {
                return false; // concurrent consumer, retry later
            }
        }

        // Claim the slot.
        head = head_.load(std::memory_order_relaxed);
        next = (head + 1) % K_CAPACITY;
        if (!head_.compare_exchange_weak(
                head, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return false; // concurrent producer, retry later
        }

        slots_[head].hint.emplace(std::move(hint));
        slots_[head].occupied.store(true, std::memory_order_release);
        count_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // Main-thread only: iterate all entries, call fn for each valid hint.
    // Removes expired hints. Does NOT advance tail_ — only remove() consumes slots.
    template <typename Fn> auto replay(Fn&& fn, steady_clock::time_point now) -> size_t {
        size_t replayed = 0;
        size_t tail = tail_.load(std::memory_order_relaxed);
        size_t head = head_.load(std::memory_order_acquire);

        while (tail != head) {
            auto& slot = slots_[tail];
            if (slot.occupied.load(std::memory_order_acquire)) {
                auto& hint = *slot.hint;
                if (hint.expires_at <= now) {
                    // Expired — drop.
                    slot.hint.reset();
                    slot.occupied.store(false, std::memory_order_release);
                    count_.fetch_sub(1, std::memory_order_relaxed);
                } else {
                    fn(hint);
                    ++replayed;
                }
            }
            tail = (tail + 1) % K_CAPACITY;
        }
        // Don't advance tail_ — only remove() should consume slots.
        return replayed;
    }

    // Main-thread only: remove a specific hint (by key+target match).
    void remove(const NodeId& target, const net::Request& req) {
        size_t head = head_.load(std::memory_order_acquire);
        for (size_t i = tail_.load(std::memory_order_relaxed); i != head;
            i = (i + 1) % K_CAPACITY) {
            auto& slot = slots_[i];
            if (slot.occupied.load(std::memory_order_relaxed)) {
                auto& hint = *slot.hint;
                if (hint.target == target && hint.req.key == req.key
                    && hint.req.version == req.version) {
                    slot.hint.reset();
                    slot.occupied.store(false, std::memory_order_release);
                    count_.fetch_sub(1, std::memory_order_relaxed);
                    return;
                }
            }
        }
    }

    [[nodiscard]] auto size() const -> size_t { return count_.load(std::memory_order_relaxed); }

  private:

    struct Slot {
        std::optional<Hint> hint;
        std::atomic<bool> occupied{false};
    };

    std::array<Slot, K_CAPACITY> slots_{};
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
    std::atomic<size_t> count_{0};
};
} // namespace cinder
