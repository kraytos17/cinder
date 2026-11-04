#pragma once

// Generic single-consumer quorum fan-out over asynchronous sends.
//
// Mechanism only: delivers N sends, reduces completions serially, fires the
// decision callback exactly once, then keeps draining late completions so no
// transport callback outlives the operation. Decision policy (what counts as
// an ack, when the decision fires, what the decision does) lives with the
// caller in the on_each / should_fire / fire / fail closures — this facility
// never interprets results.
//
// Threading contract (the whole point of this helper):
//   * Reduction is serialized. Each closure runs either entirely in the
//     INLINE phase on the calling thread, or entirely in the TAIL phase on
//     the system thread pool — never both, never concurrently. The handoff
//     (co_spawn after the inline drain gives up) happens-before the tail
//     reads shared state, and only one phase reads it at a time.
//   * Delivery thread is therefore "caller's thread OR system pool", never a
//     specific io thread. Terminal callbacks must be thread-safe and
//     non-blocking; strand-bound work re-posts itself.
//   * Fully synchronous transports complete synchronously: when every send
//     reports before the inline drain gives up, no coroutine is spawned and
//     the decision fires before this function returns.
//
// Transport contract: every send reports exactly once. Capacity N provably
// fits N single-shot senders, so try_send cannot drop. A duplicate report
// for an already-delivered target is ignored defensively: accounting must
// not depend on a transport never double-firing (an extra --pending can
// underflow the counter or force a premature decision).

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/system_executor.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <cstddef>
#include <deque>
#include <memory>
#include <utility>

#include "cinder/common/tracing.hpp"

namespace cinder {

    // Reduce progress shared by the inline drain and (if needed) the async
    // tail. Only one phase reads it at a time, handed off via co_spawn, so
    // no lock is needed.
    struct FanOutDrain {
        bool fired = false;
        size_t seen = 0;
        // Per-target delivery record. Transports must invoke each send's
        // callback exactly once, but a duplicate delivery must not corrupt
        // pending/ack accounting, so repeats are ignored here. Atomic
        // because duplicate deliveries can race on different threads:
        // exchange admits exactly one winner even under concurrent
        // double-fire; the loser touches nothing.
        std::deque<std::atomic<bool>> delivered;
    };

    template <typename Result>
    using FanOutChannel =
        asio::experimental::concurrent_channel<void(asio::error_code, std::size_t, Result)>;

    template <
        typename Result,
        typename SendAll,
        typename OnEach,
        typename ShouldFire,
        typename Fire,
        typename Fail
    >
    void fanOutQuorum(
        size_t n,
        SendAll&& send_all,
        OnEach&& on_each,
        ShouldFire&& should_fire,
        Fire&& fire,
        Fail&& fail
    ) {
        // Capture the submitting thread's trace identity: completions that
        // drain inline already run under it, but the async tail resumes on
        // system-pool threads with unrelated TLS. The tail re-attaches this
        // context around every terminal callback (never across a suspension:
        // a TraceScope spanning a co_await could resume on a different thread
        // and clobber its TLS on scope exit).
        SpanContext caller = captureTraceContext();
        auto chan = std::make_shared<FanOutChannel<Result>>(asio::system_executor{}, n);
        auto drain = std::make_shared<FanOutDrain>();
        for (size_t k = 0; k < n; ++k) {
            drain->delivered.emplace_back(false);
        }
        // Deduping reporter handed to send_all instead of the raw channel
        // sender: first delivery per target goes through, repeats are
        // dropped before they can touch accounting or the channel.
        auto report_once = [chan, drain, n](std::size_t i, Result r) {
            // Relaxed: the winner's channel send synchronizes with the
            // single-consumer reducer; the loser touches nothing.
            if (i < n && !drain->delivered[i].exchange(true, std::memory_order_relaxed)) {
                (void)chan->try_send(asio::error_code{}, i, std::move(r));
            }
        };

        send_all(report_once);
        // Phase 1: inline drain on the caller thread. try_receive unlocks
        // the channel before invoking the handler, so running user code
        // here cannot deadlock against the channel.
        while (drain->seen < n) {
            bool got = chan->try_receive([&](asio::error_code, std::size_t idx, Result r) {
                ++drain->seen;
                on_each(idx, std::move(r), !drain->fired);
                if (!drain->fired && should_fire()) {
                    drain->fired = true;
                    fire();
                }
            });
            if (!got) {
                break;
            }
        }
        if (drain->seen == n) {
            return;
        }

        // Phase 2: detached tail for outstanding completions. Owns copies
        // of the closures (init-capture), so nothing dangles after return.
        asio::co_spawn(
            asio::system_executor{},
            [chan,
             drain,
             n,
             caller,
             on_each = std::forward<OnEach>(on_each),
             should_fire = std::forward<ShouldFire>(should_fire),
             fire = std::forward<Fire>(fire),
             fail = std::forward<Fail>(fail)] mutable -> asio::awaitable<void> {
            try {
                while (drain->seen < n) {
                    auto [idx, res] = co_await chan->async_receive(asio::use_awaitable);
                    TraceScope scope(caller);
                    ++drain->seen;
                    on_each(idx, std::move(res), !drain->fired);
                    if (!drain->fired && should_fire()) {
                        drain->fired = true;
                        fire();
                    }
                }
                co_return;
            } catch (...) {
                TraceScope scope(caller);
                if (!drain->fired) {
                    drain->fired = true;
                    fail();
                }
                co_return;
            }
        },
            asio::detached
        );
    }

} // namespace cinder
