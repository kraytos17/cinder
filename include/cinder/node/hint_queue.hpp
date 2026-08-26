#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <vector>

#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

// Bounded FIFO of pending replication hints for unreachable replicas.
//
// Thread-safety: all operations take internal_hint_mutex_. The earlier
// CAS-based MPSC ring was only correct while every caller shared one io
// thread; with a pooled io_context, push (write callbacks), replay (timer),
// and remove (send completions) genuinely race on slot storage. Hints are
// failure-path events at low rate, so a plain mutex is the right trade.
class HintQueue {
  public:

    static constexpr size_t K_CAPACITY = 1'024;

    struct Hint {
        NodeId target;
        net::Request req;
        steady_clock::time_point expires_at;
    };

    // Enqueue a hint. If the queue is full the OLDEST hint is dropped to make
    // room; push always succeeds
    auto push(Hint hint) -> bool {
        std::scoped_lock lock(mutex_);
        size_t next = (head_ + 1) % K_CAPACITY;
        if (next == tail_) {
            // Full — drop the oldest entry.
            slots_[tail_].reset();
            tail_ = (tail_ + 1) % K_CAPACITY;
            --count_;
        }

        slots_[head_].emplace(std::move(hint));
        head_ = next;
        ++count_;
        return true;
    }

    // Snapshot live hints under the lock, then invoke fn outside it — the
    // visitor may synchronously complete sends whose callbacks call remove()
    // on this queue, so holding the lock across fn would self-deadlock.
    // Expired hints are dropped during the snapshot.
    // Returns the number of live hints snapshotted.
    template <typename Fn> auto replay(Fn&& fn, steady_clock::time_point now) -> size_t {
        std::vector<Hint> live;
        {
            std::scoped_lock lock(mutex_);
            for (size_t i = tail_; i != head_; i = (i + 1) % K_CAPACITY) {
                auto& hint = slots_[i];
                if (!hint.has_value()) {
                    continue;
                }
                if (hint->expires_at <= now) {
                    hint.reset(); // expired — drop
                    --count_;
                    continue;
                }
                live.push_back(*hint);
            }
        }

        for (auto& hint : live) {
            fn(hint);
        }
        return live.size();
    }

    // Remove a specific hint (by target+key+version match), e.g. after a
    // successful replay send.
    void remove(const NodeId& target, const net::Request& req) {
        std::scoped_lock lock(mutex_);
        for (size_t i = tail_; i != head_; i = (i + 1) % K_CAPACITY) {
            auto& hint = slots_[i];
            if (hint.has_value() && hint->target == target && hint->req.key == req.key
                && hint->req.version == req.version) {
                hint.reset();
                --count_;
                return;
            }
        }
    }

    [[nodiscard]] auto size() const -> size_t {
        std::scoped_lock lock(mutex_);
        return count_;
    }

  private:

    std::array<std::optional<Hint>, K_CAPACITY> slots_{};
    size_t head_ = 0; // next write position
    size_t tail_ = 0; // oldest live entry
    size_t count_ = 0;
    mutable std::mutex mutex_;
};
} // namespace cinder
