#include "cinder/node/replication_manager.hpp"

#include <asio/awaitable.hpp>
#include <asio/co_spawn.hpp>
#include <asio/detached.hpp>
#include <asio/experimental/concurrent_channel.hpp>
#include <asio/system_executor.hpp>
#include <asio/use_awaitable.hpp>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "cinder/common/metrics.hpp"
#include "cinder/common/tracing.hpp"

namespace cinder {

    using std::chrono::milliseconds;
    using std::chrono::system_clock;

    namespace {

        // Stable FNV-1a 64-bit hash — deterministic across compilers/platforms, used
        // purely as a per-node write tiebreaker.
        uint64_t fnv1a64(std::string_view s) {
            uint64_t h = 14'695'981'039'346'656'037ULL;
            for (char c : s) {
                h ^= static_cast<uint8_t>(c);
                h *= 1'099'511'628'211ULL;
            }
            return h;
        }

        // Reduce progress shared by the inline drain and (if needed) the async
        // tail. Only one phase reads it at a time, handed off via co_spawn, so
        // no lock is needed. Named here (not a local struct) to keep the tail
        // coroutine frame free of extra no-linkage member types.
        struct FanOutDrain {
            bool fired = false;
            size_t seen = 0;
            // Per-target delivery record. Transports must invoke each send's
            // callback exactly once, but a duplicate delivery must not corrupt
            // pending/ack accounting (an extra --pending can underflow the
            // counter or force a premature decision), so repeats are ignored
            // here. Atomic because duplicate deliveries can race on different
            // threads: exchange admits exactly one winner even under
            // concurrent double-fire; the loser is dropped before touching
            // accounting or the channel. A deque holds the flags because
            // vector growth requires relocating elements and atomics are
            // neither copyable nor movable.
            std::deque<std::atomic<bool>> delivered;
        };

        template <typename Result>
        using FanOutChannel =
            asio::experimental::concurrent_channel<void(asio::error_code, std::size_t, Result)>;

        // Generic quorum fan-out. Launches N sends, then reduces completions
        // through a channel, so the reducer is single-consumer — no locks, and
        // "fire exactly once" is a plain bool. The loop drains ALL N
        // completions (not just up to the decision): late arrivals still run
        // their side effects, exactly as the old per-callback code did, and
        // draining guarantees no transport callback outlives the operation.
        //
        // Executor contract (the whole point of this helper):
        //   * Reduction is serialized. on_each / should_fire / fire / fail run
        //     either entirely in the INLINE phase on the caller's thread, or
        //     entirely in the TAIL phase on the system thread pool — never
        //     both, never concurrently. The handoff (co_spawn after the inline
        //     drain gives up) happens-before the tail reads shared state, and
        //     only one phase reads it at a time.
        //   * Delivery thread is therefore "caller's thread OR system pool",
        //     never a specific io thread. Terminal callbacks must be
        //     thread-safe and non-blocking; strand-bound work re-posts itself.
        //   * Fully synchronous transports complete synchronously: when every
        //     send reports before the inline drain gives up, on_done fires
        //     before this function returns and no coroutine is spawned.
        //
        // Contract: every launched send reports exactly once (all Transport
        // implementations always invoke their callback). Capacity N provably
        // fits N single-shot senders, so try_send cannot drop. A duplicate
        // report for an already-delivered target is ignored defensively (see
        // FanOutDrain::delivered): accounting must not depend on a transport
        // never double-firing.
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
                 on_each = std::forward<OnEach>(on_each),
                 should_fire = std::forward<ShouldFire>(should_fire),
                 fire = std::forward<Fire>(fire),
                 fail = std::forward<Fail>(fail)] mutable -> asio::awaitable<void> {
                try {
                    while (drain->seen < n) {
                        auto [idx, res] = co_await chan->async_receive(asio::use_awaitable);
                        ++drain->seen;
                        on_each(idx, std::move(res), !drain->fired);
                        if (!drain->fired && should_fire()) {
                            drain->fired = true;
                            fire();
                        }
                    }
                } catch (...) {
                    if (!drain->fired) {
                        drain->fired = true;
                        fail();
                    }
                }
                co_return;
            },
                asio::detached
            );
        }
    } // namespace

    ReplicationManager::ReplicationManager(
        CacheStore& local,
        NodeId self,
        Clock& clock,
        Transport& transport
    ) :
        local_(local),
        self_(std::move(self)),
        clock_(clock),
        transport_(transport) {}

    void ReplicationManager::writeAsync(
        const std::string& key,
        std::string value,
        std::optional<milliseconds> ttl,
        const std::vector<NodeId>& replica_nodes,
        ConsistencyMode mode,
        WriteCallback on_done,
        uint64_t trace_id,
        uint64_t span_id
    ) {
        Span span("replication.write.fanout", span_id);
        // Version comes from the store — it is the single version authority. The
        // store's counter has advanced past any observed peer versions (Lamport
        // bump) and is seeded from the clock, so a restarted node wins LWW.
        Version version = local_.mintVersion();
        uint64_t writer = fnv1a64(self_);

        VersionedEntry entry;
        entry.value = value;
        entry.setVersion(version);
        entry.writer_node_hash = writer;
        if (ttl.has_value()) {
            entry.expires_at = clock_.now() + *ttl;
            entry.setHasTtl(true);
        }

        // Capture the absolute wall-clock expiry before `entry` is moved into the
        // store below. Carried on the wire (not a relative ttl) so every replica
        // expires the key at the same instant regardless of delivery delay.
        std::optional<system_clock::time_point> wire_expiry;
        if (entry.hasTtl()) {
            wire_expiry = toSystemExpiry(clock_, entry.expires_at);
        }

        auto local_res = local_.putVersioned(key, std::move(entry));
        if (!local_res.has_value()) {
            CINDER_WARN(
                "local write failed",
                {"key", key},
                {"reason", local_res.error().message()}
            );

            on_done(local_res);
            return;
        }

        net::Request req;
        req.opcode = net::Opcode::Replicate;
        req.key = key;
        req.value = std::move(value);
        req.expires_at = wire_expiry;
        req.version = version;
        req.writer_node_hash = writer;
        req.trace_id = trace_id;
        req.span_id = span_id;
        if (mode == ConsistencyMode::Async) {
            CINDER_DEBUG("async write fan-out", {"key", key}, {"replicas", replica_nodes.size()});
            // Local commit counts; fan-out best-effort, hint on failure.
            for (const auto& node : replica_nodes) {
                transport_.sendAsync(node, req, [this, node, req](Result<void> r) {
                    if (!r.has_value()) {
                        Span fail_span("replication.write.async.fail", req.span_id);
                        CINDER_WARN("replica unreachable", {"node", node}, {"key", req.key});
                        if (metrics_) {
                            metrics_->replicationMetrics().replica_unreachable.fetch_add(
                                1,
                                std::memory_order_relaxed
                            );
                        }
                        enqueueHint(node, req);
                    }
                });
            }
            if (metrics_) {
                metrics_->replicationMetrics().async_writes.fetch_add(1, std::memory_order_relaxed);
            }

            on_done(ok());
            return;
        }

        // Quorum: W = R/2 + 1 acknowledgements including the local write.
        CINDER_DEBUG(
            "quorum write fan-out",
            {"key", key},
            {"replicas", replica_nodes.size()},
            {"W", replica_nodes.size() / 2 + 1}
        );

        size_t total = replica_nodes.size() + 1;
        size_t w = total / 2 + 1;
        if (replica_nodes.empty()) {
            // W == 1: the local ack suffices.
            on_done(ok());
            return;
        }

        // Single-consumer quorum reduce: every completion (including stragglers
        // after the decision) enqueues hints for failures, exactly as the old
        // per-callback code did. on_done fires once, at the decision. All state
        // below lives in the coroutine frame -- resumptions serialize through
        // the channel, so no mutex is needed.
        // Single-consumer quorum reduce: every completion (including stragglers
        // after the decision) enqueues hints for failures, exactly as the old
        // per-callback code did. on_done fires once, at the decision. State is
        // heap-owned so the async tail (if any) cannot dangle; the inline and
        // tail phases never read it concurrently.
        struct WriteQuorum {
            std::vector<NodeId> replicas;
            net::Request req;
            WriteCallback on_done;
            std::string key;
            size_t acks = 1; // local write
            size_t pending = 0;
            size_t w = 0;
        };

        auto st = std::make_shared<WriteQuorum>();
        st->replicas = replica_nodes;
        st->req = std::move(req);
        st->on_done = std::move(on_done);
        st->key = key;
        st->pending = st->replicas.size();
        st->w = w;
        fanOutQuorum<Result<void>>(st->replicas.size(), [this, st](auto report) {
            for (size_t i = 0; i < st->replicas.size(); ++i) {
                transport_.sendAsync(st->replicas[i], st->req, [report, i](Result<void> r) mutable {
                    report(i, std::move(r));
                });
            }
        }, [this, st](std::size_t i, Result<void> r, bool active) {
            if (!r.has_value()) {
                CINDER_WARN("replica unreachable", {"node", st->replicas[i]}, {"key", st->key});
                if (metrics_) {
                    metrics_->replicationMetrics().replica_unreachable.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                enqueueHint(st->replicas[i], st->req);
            } else if (active) {
                ++st->acks;
            }
            --st->pending;
            // Fail fast when quorum is unreachable: even if every outstanding
            // replica acked, acks could not reach W. The decision fires now
            // instead of waiting out the stragglers, but the drain continues
            // so late failures still enqueue hints. (Reads deliberately omit
            // this: firing early could return a stale best_entry while a newer
            // version is still in flight.)
        }, [st] -> bool {
            return st->acks >= st->w || st->pending == 0 || st->acks + st->pending < st->w;
        }, [this, st] {
            if (st->acks >= st->w) {
                Span ok_span("replication.write.quorum.ok");
                CINDER_DEBUG("quorum write succeeded", {"key", st->key}, {"acks", st->acks});
                if (metrics_) {
                    metrics_->replicationMetrics().quorum_writes_ok.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                st->on_done(ok());
            } else {
                Span fail_span("replication.write.quorum.fail");
                CINDER_WARN(
                    "quorum write failed",
                    {"key", st->key},
                    {"acks", st->acks},
                    {"needed", st->w}
                );
                if (metrics_) {
                    metrics_->replicationMetrics().quorum_writes_failed.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                st->on_done(err(Error(Errc::NotReady, "quorum not reached")));
            }
        }, [st] { st->on_done(err(Error(Errc::NotReady, "quorum fan-out failed"))); });
    }

    void ReplicationManager::finishReadFlight(
        const std::string& key,
        const std::shared_ptr<ReadFlight>& flight,
        Result<VersionedEntry> result
    ) {
        std::vector<ReadCallback> waiters;
        {
            std::scoped_lock lock(read_flights_mutex_);
            auto it = read_flights_.find(key);
            if (it != read_flights_.end() && it->second == flight) {
                read_flights_.erase(it);
            }
            waiters = std::move(flight->waiters);
        }

        // All but the last waiter get a copy; the last takes the move. A
        // single-waiter flight (the common case) moves with zero copies.
        // (Structured without a conditional move inside the loop: the
        // use-after-move check cannot prove a last-iteration-only move safe.)
        for (size_t i = 0; i + 1 < waiters.size(); ++i) {
            waiters[i](result);
        }
        if (!waiters.empty()) {
            waiters.back()(std::move(result));
        }
    }

    void ReplicationManager::readAsync(
        const std::string& key,
        const std::vector<NodeId>& replica_nodes,
        size_t R,
        ReadCallback on_done,
        uint64_t trace_id,
        uint64_t span_id
    ) {
        Span span("replication.read.fanout", span_id);
        // Local read — always attempted first.
        auto local_entry = local_.getVersioned(key);
        if (!local_entry.has_value()) {
            CINDER_DEBUG("local read miss", {"key", key});
        }
        if (replica_nodes.empty() || R <= 1) {
            // No replicas or trivial quorum: return whatever the local store has.
            if (local_entry.has_value()) {
                on_done(std::move(*local_entry));
            } else {
                on_done(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
            }
            return;
        }

        net::Request req;
        req.opcode = net::Opcode::GetVersioned;
        req.key = key;
        // Propagate structured trace context from the originating request.
        req.trace_id = trace_id;
        req.span_id = span_id;
        CINDER_DEBUG("quorum read fan-out", {"key", key}, {"replicas", replica_nodes.size()});
        if (metrics_) {
            metrics_->replicationMetrics().quorum_reads.fetch_add(1, std::memory_order_relaxed);
        }

        // Duplicate suppression (singleflight): concurrent reads for the same
        // key share one fan-out. The first caller becomes the leader; later
        // arrivals attach as waiters and receive the leader's LWW result.
        // No blocking anywhere — completion is async. Safe against
        // self-deadlock: replica GetVersioned requests are served from the
        // local store only, so a flight never re-enters this table for its
        // own key, and finishReadFlight erases before notifying (a waiter
        // that synchronously starts a new read begins a fresh flight).
        std::shared_ptr<ReadFlight> flight;
        {
            std::scoped_lock lock(read_flights_mutex_);
            auto it = read_flights_.find(key);
            if (it == read_flights_.end()) {
                flight = std::make_shared<ReadFlight>();
                flight->waiters.push_back(std::move(on_done));
                read_flights_.emplace(key, flight);
            } else {
                flight = it->second;
                flight->waiters.push_back(std::move(on_done));
                if (metrics_) {
                    metrics_->replicationMetrics().reads_coalesced.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                return;
            }
        }

        // Single-consumer LWW reduce: best_* is a compound maximum updated by
        // one reader at a time, so no mutex is needed. Post-decision arrivals
        // are ignored (as the old early-return did); the loop still drains so
        // no transport callback outlives the operation. Heap-owned for the
        // async tail.
        struct ReadQuorum {
            std::vector<NodeId> replicas;
            net::Request req;
            ReadCallback on_done;
            std::string key;
            size_t r = 0;
            Version best_version = 0;
            uint64_t best_writer_hash = 0;
            std::optional<VersionedEntry> best_entry;
            bool needs_repair = false;
            size_t acks = 0;
            size_t pending = 0;
        };

        auto st = std::make_shared<ReadQuorum>();
        st->replicas = replica_nodes;
        st->req = std::move(req);
        // Route the quorum decision through the flight: every waiter (leader
        // first) receives the LWW result, and the entry is erased before any
        // waiter runs. Captures the key by value — capturing `st` here would
        // form a shared_ptr cycle (st owns on_done owns st) and leak.
        st->on_done = [this, key, flight](Result<VersionedEntry> r) mutable {
            finishReadFlight(key, flight, std::move(r));
        };

        st->key = key;
        st->r = R;
        st->acks = local_entry.has_value() ? 1 : 0;
        st->pending = st->replicas.size();
        if (local_entry.has_value()) {
            st->best_version = local_entry->version();
            st->best_writer_hash = local_entry->writer_node_hash;
            st->best_entry = std::move(*local_entry);
        }

        fanOutQuorum<Result<net::Response>>(st->replicas.size(), [this, st](auto report) {
            for (size_t i = 0; i < st->replicas.size(); ++i) {
                transport_.sendRequestAsync(
                    st->replicas[i],
                    st->req,
                    [report, i](Result<net::Response> r) mutable { report(i, std::move(r)); }
                );
            }
        }, [this, st](std::size_t, Result<net::Response> r, bool active) {
            if (!active) {
                return;
            }
            if (r.has_value() && r->status == Errc::OK && r->version != 0) {
                ++st->acks;
                if (r->version
                    > st->best_version
                    || (r->version
                        == st->best_version
                        && r->writer_node_hash
                        > st->best_writer_hash)) {
                    st->needs_repair = true;
                    st->best_version = r->version;
                    st->best_writer_hash = r->writer_node_hash;

                    VersionedEntry entry;
                    entry.value = std::move(*r->value);
                    entry.setVersion(r->version);
                    entry.writer_node_hash = r->writer_node_hash;
                    if (r->expires_at.has_value()) {
                        entry.setHasTtl(true);
                        entry.expires_at = toSteadyExpiry(clock_, *r->expires_at);
                    }
                    st->best_entry = std::move(entry);
                } else if (
                    r->version != st->best_version || r->writer_node_hash != st->best_writer_hash
                ) {
                    st->needs_repair = true;
                }
            } else if (r.has_value() && r->status == Errc::NotFound) {
                // Replica doesn't have the key — stale, needs repair.
                st->needs_repair = true;
            }
            --st->pending;
        }, [st] -> bool { return st->acks >= st->r || st->pending == 0; }, [this, st] {
            std::optional<VersionedEntry> result;
            if (st->best_entry.has_value()) {
                result = std::move(st->best_entry);
            }
            if (result.has_value() && st->needs_repair) {
                // Self-heal local store: if local was behind a replica, update it.
                auto heal = local_.putVersioned(st->key, *result);
                if (!heal.has_value()) {
                    CINDER_DEBUG(
                        "local self-heal skipped",
                        {"key", st->key},
                        {"reason", static_cast<int>(heal.error().code())}
                    );
                }
                if (metrics_) {
                    metrics_->replicationMetrics().read_repairs.fetch_add(
                        1,
                        std::memory_order_relaxed
                    );
                }
                sendRepairFanOut(st->key, st->replicas, *result);
            }
            if (result.has_value()) {
                st->on_done(std::move(*result));
            } else {
                st->on_done(err<VersionedEntry>(Error(Errc::NotFound, "key not found")));
            }
        }, [st] {
            st->on_done(err<VersionedEntry>(Error(Errc::NotReady, "read fan-out failed")));
        });
    }

    auto ReplicationManager::hintCount() const -> size_t {
        return hints_.size();
    }

    void ReplicationManager::enqueueHint(const NodeId& target, const net::Request& req) {
        CINDER_DEBUG("hint enqueued", {"target", target}, {"key", req.key});
        hints_.push({target, req, clock_.now() + K_HINT_TTL});
        if (metrics_) {
            metrics_->replicationMetrics().hints_enqueued.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ReplicationManager::sendRepairFanOut(
        const std::string& key,
        const std::vector<NodeId>& targets,
        const VersionedEntry& winner
    ) {
        Span span("replication.repair.fanout");
        CINDER_INFO(
            "repair fan-out",
            {"key", key},
            {"version", winner.version()},
            {"targets", targets.size()}
        );

        net::Request repair;
        repair.opcode = net::Opcode::Replicate;
        repair.key = key;
        repair.value = winner.value;
        repair.version = winner.version();
        repair.writer_node_hash = winner.writer_node_hash;
        // Propagate trace context for end-to-end correlation.
        repair.trace_id = span.traceId();
        repair.span_id = span.spanId();
        if (winner.hasTtl()) {
            repair.expires_at = toSystemExpiry(clock_, winner.expires_at);
        }
        for (const auto& target : targets) {
            transport_.sendAsync(target, repair, [](Result<void>) {});
        }
    }

    void ReplicationManager::replayHints(ReplayCallback on_done) {
        Span span("replication.hint.replay");
        // Single-consumer: a replay already in flight (overlapping timer ticks or
        // concurrent pool threads) reports zero rather than double-sending hints.
        bool expected = false;
        if (!replaying_.compare_exchange_strong(expected, true)) {
            on_done(0);
            return;
        }

        auto now = clock_.now();
        auto batch = hints_.snapshot(now);
        // Wrap the user callback: report metrics, release the single-consumer
        // guard, then deliver. Used for both the empty fast path and fan-out.
        auto finish = [this, done = std::move(on_done)](size_t n) mutable {
            CINDER_INFO("replay completed", {"replayed", n});
            if (metrics_ && n > 0) {
                metrics_->replicationMetrics().hints_replayed.fetch_add(
                    n,
                    std::memory_order_relaxed
                );
            }

            replaying_.store(false);
            done(n);
        };

        if (batch.empty()) {
            finish(0);
            return;
        }

        struct HintReplay {
            std::vector<HintQueue::SnapshotEntry> batch;
            ReplayCallback finish;
            size_t pending = 0;
            size_t replayed = 0;
        };

        auto st = std::make_shared<HintReplay>();
        st->batch = std::move(batch);
        st->finish = std::move(finish);
        st->pending = st->batch.size();
        fanOutQuorum<Result<void>>(st->batch.size(), [this, st](auto report) {
            for (size_t i = 0; i < st->batch.size(); ++i) {
                transport_.sendAsync(
                    st->batch[i].hint.target,
                    st->batch[i].hint.req,
                    [report, i](Result<void> r) mutable { report(i, std::move(r)); }
                );
            }
        }, [this, st](std::size_t i, Result<void> r, bool) {
            if (r.has_value()) {
                hints_.remove(st->batch[i].handle);
                ++st->replayed;
            }
            --st->pending;
        }, [st] -> bool { return st->pending == 0; }, [st] { st->finish(st->replayed); }, [st] {
            st->finish(0);
        });
    }
} // namespace cinder
