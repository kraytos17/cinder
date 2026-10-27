#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

#include "cinder/common/types.hpp"
#include "cinder/net/protocol.hpp"

namespace cinder {

    using std::chrono::milliseconds;
    using std::chrono::steady_clock;

    // Bounded FIFO of pending replication hints for unreachable replicas.
    //
    // Lock-free MPSC ring (Vyukov-style sequence numbers): producers claim
    // tickets from a monotonic dispenser; the single timer-driven consumer
    // scans by ticket range. No mutex on any path.
    //
    // Why the payload is a shared_ptr: slots are recycled (drop-oldest reuses
    // the oldest slot), so a slow scanner can still hold a reference to a
    // hint whose slot has already been reused. A seqlock-style re-read would
    // only *detect* the race after the fact — but the torn read itself (a
    // concurrent std::string read + write) is a data race that can crash, not
    // just return a stale value. Publishing immutable payloads through
    // atomic<shared_ptr> makes every read either see the old hint or the new
    // one, with the refcount keeping the observed object alive. This is what
    // makes the scan TSan-clean and crash-free.
    //
    // Slot lifecycle, per slot (ticket t owns slot t & K_MASK):
    //   free-for-t  <=> seq == t. The constructor seeds seq[i] == i, so every
    //     first-generation ticket starts free.
    //   publish: payload stored (atomic shared_ptr store), tomb cleared, then
    //     seq.store(t + 1, release). The release pairs with the readers'
    //     acquire loads, so a reader that observes seq == t + 1 also observes
    //     the payload store.
    //   free: whoever flips tombstoned false->true decrements the count
    //     exactly once and stores seq = u + K_CAPACITY (free for the next
    //     generation), where u is the ticket it observed. Concurrent freers of
    //     the same ticket resolve through the tombstone exchange; the seq
    //     stores write the same value and are idempotent.
    // Readers accept a slot only when seq == t + 1 for a ticket t in range,
    // load the payload atomically, then re-validate seq + tombstone
    // (seqlock discipline): recycled or removed slots are skipped, never
    // acted on — and the skipped payload is simply dropped by refcount.
    //
    // Liveness: the claim spin and the drop-oldest spin are unbounded (pause
    // loop, never sleep). They resolve as soon as the preempted thread is
    // rescheduled — threads here are preempted, never killed — so this waits
    // rather than corrupts. This is no worse than the mutex this replaces: a
    // holder preempted inside the old critical section blocked ALL producers
    // and consumers, while here only full-window pushes wait, and only on the
    // one stalled slot.
    class HintQueue {
    public:

        // Power-of-two capacity: ticket->slot is a mask, and generations are
        // exactly K_CAPACITY tickets apart. Unlike the old head/tail sentinel
        // ring, no slot is sacrificed: all K_CAPACITY entries are usable.
        static constexpr size_t K_CAPACITY = 1024;
        static_assert(
            (K_CAPACITY & (K_CAPACITY - 1)) == 0,
            "K_CAPACITY must be a power of two for ticket->slot masking"
        );
        static constexpr size_t K_MASK = K_CAPACITY - 1;

        struct Hint {
            NodeId target;
            net::Request req;
            steady_clock::time_point expires_at;
        };

        // Payloads cross generations via make_shared; a throwing move inside
        // the protocol would break the exactly-once accounting below.
        static_assert(
            std::is_nothrow_move_constructible_v<Hint>,
            "Hint must be nothrow move constructible"
        );

        HintQueue() {
            for (size_t i = 0; i < K_CAPACITY; ++i) {
                slots_[i].seq.store(i, std::memory_order_relaxed);
            }
        }

        HintQueue(const HintQueue&) = delete;
        auto operator=(const HintQueue&) -> HintQueue& = delete;

        // Enqueue a hint. If the queue is full the OLDEST hint is dropped to make
        // room; push always succeeds.
        auto push(Hint hint) -> bool {
            const size_t ticket = tail_.fetch_add(1, std::memory_order_relaxed);
            // Bound the live window: each push that observes a full window
            // retires exactly one ticket, preserving tail_ - head_ <= K_CAPACITY.
            while (ticket - head_.load(std::memory_order_acquire) >= K_CAPACITY) {
                dropOldest();
            }

            Cell& cell = slots_[ticket & K_MASK];
            // Wait for our generation's slot. Normally already free: the
            // previous generation was retired (and its slot freed) before
            // head_ advanced past it. Spins only if a freer stalled between
            // winning the tombstone exchange and storing seq.
            spinUntilSeqEq(cell.seq, ticket);
            // Claim-side reset BEFORE publishing: a concurrent remover can only
            // act on published slots (seq == u + 1), so clearing here cannot
            // race a legitimate remove of this generation. A stale reader of
            // the previous generation checks seq first and skips (seq ==
            // ticket, not its generation's published marker).
            auto payload = std::make_shared<const Hint>(std::move(hint));
            cell.tombstoned.store(false, std::memory_order_relaxed);
            cell.payload.store(std::move(payload), std::memory_order_relaxed);
            cell.seq.store(ticket + 1, std::memory_order_release);
            count_.fetch_add(1, std::memory_order_relaxed);
            return true;
        }

        // Snapshot live hints, then invoke fn outside any synchronization — the
        // visitor may synchronously complete sends whose callbacks call remove()
        // on this queue. Expired hints are dropped during the snapshot.
        // Returns the number of live hints snapshotted.
        template <typename Fn> auto replay(Fn&& fn, steady_clock::time_point now) -> size_t {
            std::vector<std::shared_ptr<const Hint>> live;
            const size_t head = head_.load(std::memory_order_acquire);
            const size_t tail = tail_.load(std::memory_order_acquire);
            for (size_t t = head; t != tail; ++t) {
                const Cell& cell = slots_[t & K_MASK];
                // Accept only a slot provably holding ticket t: guards against
                // recycled generations.
                if (cell.seq.load(std::memory_order_acquire) != t + 1) {
                    continue;
                }
                if (cell.tombstoned.load(std::memory_order_acquire)) {
                    continue;
                }
                // Atomic load: either the old hint or the recycled
                // generation's — never a torn object. The shared ownership
                // keeps it alive past any concurrent slot reuse.
                std::shared_ptr<const Hint> owned = cell.payload.load(std::memory_order_acquire);
                if (cell.seq.load(std::memory_order_acquire)
                    != t
                    + 1
                    || cell.tombstoned.load(std::memory_order_acquire)) {
                    continue; // recycled or removed mid-load; skip, don't deliver
                }
                if (owned == nullptr || owned->expires_at <= now) {
                    // Expired — drop now: first freer wins the dec via tombstone
                    // exchange.
                    freeTicket(t);
                    continue;
                }
                live.push_back(std::move(owned));
            }
            for (const auto& hint : live) {
                fn(*hint);
            }
            return live.size();
        }

        // Remove a specific hint (by target+key+version match), e.g. after a
        // successful replay send. Best-effort linear scan; first match wins.
        // A concurrent push that has not published yet is legitimately missed —
        // removal is best-effort and the next replay tick retries.
        void remove(const NodeId& target, const net::Request& req) {
            const size_t head = head_.load(std::memory_order_acquire);
            const size_t tail = tail_.load(std::memory_order_acquire);
            for (size_t t = head; t != tail; ++t) {
                const Cell& cell = slots_[t & K_MASK];
                if (cell.seq.load(std::memory_order_acquire) != t + 1) {
                    continue;
                }
                if (cell.tombstoned.load(std::memory_order_acquire)) {
                    continue;
                }
                // Compare against a stably-owned snapshot: re-validate seq +
                // tombstone so a concurrent recycle cannot make us match a
                // payload from another generation.
                std::shared_ptr<const Hint> owned = cell.payload.load(std::memory_order_acquire);
                if (cell.seq.load(std::memory_order_acquire)
                    != t
                    + 1
                    || cell.tombstoned.load(std::memory_order_acquire)) {
                    continue;
                }
                if (owned
                    != nullptr
                    && owned->target
                    == target
                    && owned->req.key
                    == req.key
                    && owned->req.version
                    == req.version) {
                    freeTicket(t);
                    return;
                }
            }
        }

        [[nodiscard]]
        auto size() const -> size_t {
            // Gauge, not a census: relaxed load is fine — callers use it as a
            // "> 0" gate (scheduleReplay) or a metric, never for exactness.
            return count_.load(std::memory_order_relaxed);
        }

    private:

        struct Cell {
            // Ticket protocol states, for ticket t owning this slot:
            //   seq == t     : free, safe for ticket t's producer to claim.
            //   seq == t + 1 : published, holds ticket t's hint.
            // Any other value: a transition in flight — wait, never assume.
            std::atomic<uint64_t> seq{0};
            // Immutable payload: published once per generation via atomic
            // store, observed via atomic load. Refcounting keeps an observed
            // hint alive across concurrent slot reuse.
            std::atomic<std::shared_ptr<const Hint>> payload;
            // Set exactly once per generation by whichever freer wins the
            // exchange (remove/expire/drop-oldest); only the winner decs
            // count_ (see freeTicket). Cleared by the claiming producer before
            // publishing (a remover can only act on published slots, so the
            // clear cannot race a legitimate remove of the new generation).
            std::atomic<bool> tombstoned{false};
        };

        // Exactly-once accounting for ticket t's slot: only the tombstone
        // exchange winner decs count_. The seq store frees the slot for ticket
        // t + K_CAPACITY; concurrent freers of the same ticket store the same
        // value, so the stores are idempotent.
        void freeTicket(size_t ticket) {
            Cell& cell = slots_[ticket & K_MASK];
            if (!cell.tombstoned.exchange(true, std::memory_order_acq_rel)) {
                count_.fetch_sub(1, std::memory_order_relaxed);
            }
            cell.seq.store(ticket + K_CAPACITY, std::memory_order_release);
        }

        // Retire the oldest ticket: advance head_ past it and free its slot.
        // Called only when the live window is full, so the head slot always
        // belongs to a published-or-dying generation — unless a producer
        // stalled between claiming and publishing, in which case we wait for
        // it (bounded stall, resolves on reschedule; never corrupt).
        void dropOldest() {
            const size_t h = head_.fetch_add(1, std::memory_order_acq_rel);
            Cell& cell = slots_[h & K_MASK];
            spinUntilPublishedOrDead(cell, h);
            freeTicket(h);
        }

        static void spinPause() {
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#elif defined(__aarch64__)
            __asm__ volatile("yield" ::: "memory");
#else
            std::atomic_thread_fence(std::memory_order_acquire);
#endif
        }

        static void spinUntilSeqEq(const std::atomic<uint64_t>& seq, uint64_t want) {
            while (seq.load(std::memory_order_acquire) != want) {
                spinPause();
            }
        }

        static void spinUntilPublishedOrDead(const Cell& cell, size_t ticket) {
            const uint64_t published = ticket + 1;
            while (true) {
                if (cell.seq.load(std::memory_order_acquire) == published) {
                    return;
                }
                if (cell.tombstoned.load(std::memory_order_acquire)) {
                    return;
                }
                spinPause();
            }
        }

        std::array<Cell, K_CAPACITY> slots_{};
        // No alignas() on these counters: padding them to separate cache lines
        // would raise this class's alignment to 64 bytes, and that alignment
        // propagates through ReplicationManager into CacheNodeServer (a
        // singleton), tripping clang-analyzer-optin.performance.Padding for
        // ~112 bytes nobody will ever notice.
        std::atomic<size_t> tail_{0}; // next ticket to hand out
        std::atomic<size_t> head_{0}; // oldest live ticket
        std::atomic<size_t> count_{0}; // live-hint gauge
    };
} // namespace cinder
