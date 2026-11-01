#include <algorithm>
#include <benchmark/benchmark.h>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "cinder/node/hint_queue.hpp"
#include "cinder/store/lfu_store.hpp"
#include "cinder/store/lru_store.hpp"
#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {
    namespace {

        void bmLruStorePut(benchmark::State& state) {
            LruStore store(static_cast<size_t>(state.range(0)));
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                // NOLINTNEXTLINE
                (void)store.put(std::to_string(i++), std::string(256, 'x'));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLruStorePut)->Range(1 << 16, 1 << 20);

        void bmLruStoreGet(benchmark::State& state) {
            LruStore store(static_cast<size_t>(state.range(0)));
            for (int i = 0; i < 1000; i++) {
                // NOLINTNEXTLINE
                (void)store.put(std::to_string(i), std::string(256, 'x'));
            }

            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                store.get(std::to_string(i++ % 1000));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLruStoreGet)->Range(1 << 16, 1 << 20);

        void bmLfuStorePut(benchmark::State& state) {
            LfuStore store(static_cast<size_t>(state.range(0)));
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                // NOLINTNEXTLINE
                (void)store.put(std::to_string(i++), std::string(256, 'x'));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLfuStorePut)->Range(1 << 16, 1 << 20);

        void bmLfuStoreGet(benchmark::State& state) {
            LfuStore store(static_cast<size_t>(state.range(0)));
            for (int i = 0; i < 1000; i++) {
                // NOLINTNEXTLINE
                (void)store.put(std::to_string(i), std::string(256, 'x'));
            }

            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                store.get(std::to_string(i++ % 1000));
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLfuStoreGet)->Range(1 << 16, 1 << 20);

        // These cases intentionally create distinct frequencies, unlike the
        // simple 1000-key round-robin benchmark above which quickly converges
        // to a single bucket. Setup needs no PauseTiming: the timer starts
        // lazily on the first KeepRunning, so everything before the loop is
        // excluded from both time counters. (Pausing before the loop starts
        // is invalid — see State::StartKeepRunning — and corrupts the run.)
        // A plain function also keeps the benchmark cppcheck-clean (no
        // BENCHMARK_DEFINE_F macro expansion to parse).
        void bmLfuStoreFrequencySpread(benchmark::State& state) {
            const size_t count = static_cast<size_t>(state.range(0));
            const bool skewed = state.range(1) != 0;
            auto store = std::make_unique<LfuStore>(4U << 20U);
            std::vector<std::string> keys;
            keys.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                keys.push_back("lfu-key-" + std::to_string(i));
                // NOLINTNEXTLINE
                (void)store->put(keys.back(), "v");
            }
            // Seed distinct exact frequencies: key i ends at freq i+1.
            for (size_t i = 0; i < count; ++i) {
                for (size_t hit = 0; hit < i; ++hit) {
                    (void)store->get(keys[i]);
                }
            }
            std::vector<size_t> trace;
            trace.reserve(4096);
            if (!skewed) {
                for (size_t i = 0; i < count; ++i) {
                    trace.push_back(i);
                }
            } else {
                const size_t hot_count = std::max<size_t>(1, count / 16);
                for (size_t i = 0; i < 4096; ++i) {
                    trace.push_back((i % 5) != 0 ? i % hot_count : i % count);
                }
            }
            size_t pos = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                (void)store->get(keys[trace[pos++ % trace.size()]]);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLfuStoreFrequencySpread)
            ->Args({64, 0})
            ->Args({256, 0})
            ->Args({1024, 0})
            ->Args({64, 1})
            ->Args({256, 1})
            ->Args({1024, 1});

        // Eviction-pressure variant: capacity holds roughly a quarter of the
        // key pool, so the timed loop continuously evicts through the
        // ordered-minimum path — the operation the B-tree bucket map exists
        // to serve. The pure-get benchmark above never reaches evictOne.
        // Round-robin puts over the oversize pool are insert-evicts in steady
        // state (each key is evicted long before its turn returns); gets
        // mostly hit. Warmup runs the same mix before the loop so the timed
        // region measures steady state, not fill transient.
        void bmLfuStoreEvictionPressure(benchmark::State& state) {
            const size_t count = static_cast<size_t>(state.range(0));
            const size_t value_size = static_cast<size_t>(state.range(1));
            const std::string value(value_size, 'x');
            // Entry ≈ key + value + node overhead; hold ~1/4 of the pool.
            const size_t capacity = count * (value_size + 160) / 4;
            auto store = std::make_unique<LfuStore>(capacity);
            std::vector<std::string> keys;
            keys.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                keys.push_back("lfu-key-" + std::to_string(i));
                // NOLINTNEXTLINE
                (void)store->put(keys.back(), value);
            }
            // Seed distinct exact frequencies: key i ends at freq i+1.
            for (size_t i = 0; i < count; ++i) {
                for (size_t hit = 0; hit < i; ++hit) {
                    (void)store->get(keys[i]);
                }
            }
            size_t pos = 0;
            // Hot-skewed get trace (80% hot 1/16) so gets mostly hit while
            // round-robin puts churn the cold tail through eviction.
            const size_t hot_count = std::max<size_t>(1, count / 16);
            std::vector<size_t> get_trace;
            get_trace.reserve(4096);
            for (size_t i = 0; i < 4096; ++i) {
                get_trace.push_back((i % 5) != 0 ? i % hot_count : i % count);
            }
            size_t get_pos = 0;
            size_t put_pos = 0;
            auto mix_op = [&]() {
                for (int g = 0; g < 7; ++g) {
                    (void)store->get(keys[get_trace[get_pos++ % get_trace.size()]]);
                }
                // NOLINTNEXTLINE
                (void)store->put(keys[put_pos++ % count], value);
            };
            for (size_t w = 0; w < count * 2; ++w) {
                mix_op();
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                mix_op();
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLfuStoreEvictionPressure)
            ->Args({256, 64})
            ->Args({1024, 64})
            ->Args({256, 256})
            ->Args({1024, 256});

        auto makeHint(size_t index, size_t value_size) -> HintQueue::Hint {
            HintQueue::Hint hint;
            hint.target = "replica-" + std::to_string(index % 3);
            hint.req.key = "key-" + std::to_string(index);
            hint.req.value.assign(value_size, 'v');
            hint.req.version = index + 1;
            hint.expires_at = steady_clock::time_point::max();
            return hint;
        }

        void bmHintQueueSnapshotSingleCopy(benchmark::State& state) {
            const auto count = static_cast<size_t>(state.range(0));
            const auto value_size = static_cast<size_t>(state.range(1));
            HintQueue queue;
            for (size_t i = 0; i < count; ++i) {
                queue.push(makeHint(i, value_size));
            }
            const auto now = steady_clock::now();

            // Production snapshot path: one owned copy of the live hints,
            // moved into the async send batch (no second per-hint copy).
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto batch = queue.snapshot(now);
                auto* batch_data = batch.data();
                benchmark::DoNotOptimize(batch_data);
                auto batch_size = batch.size();
                benchmark::DoNotOptimize(batch_size);
            }

            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(count));
            state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(count * value_size));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueSnapshotSingleCopy)
            ->Args({64, 64})
            ->Args({1024, 64})
            ->Args({256, 4096})
            ->Args({1024, 1024});

        enum class HintRemoveOrder {
            Fifo,
            Reverse,
            Shuffled,
        };

        // Production-shaped path: snapshot once, then resolve completions in
        // the given order. Only the removal identity differs: legacy
        // target/key/version scan versus generation-tagged handle.
        void bmHintQueueReplayAndRemove(
            benchmark::State& state,
            HintRemoveOrder order,
            bool use_handle
        ) {
            const auto count = static_cast<size_t>(state.range(0));
            std::vector<size_t> indices(count);
            std::iota(indices.begin(), indices.end(), 0);
            if (order == HintRemoveOrder::Reverse) {
                std::reverse(indices.begin(), indices.end());
            } else if (order == HintRemoveOrder::Shuffled) {
                // Seed from the case size: deterministic across runs so the
                // case is reproducible, without a fixed-constant seed.
                std::mt19937 rng(static_cast<std::mt19937::result_type>(count));
                std::shuffle(indices.begin(), indices.end(), rng);
            }

            HintQueue queue;
            for (size_t i = 0; i < count; ++i) {
                queue.push(makeHint(i, 64));
            }
            const auto now = steady_clock::now();
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto batch = queue.snapshot(now);
                auto* batch_data = batch.data();
                benchmark::DoNotOptimize(batch_data);
                for (const size_t i : indices) {
                    if (use_handle) {
                        queue.remove(batch[i].handle);
                    } else {
                        queue.remove(batch[i].hint.target, batch[i].hint.req);
                    }
                }
                auto queue_size = queue.size();
                benchmark::DoNotOptimize(queue_size);
                state.PauseTiming();
                for (const auto& entry : batch) {
                    queue.push(entry.hint);
                }
                state.ResumeTiming();
            }

            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(count));
        }

        void bmHintQueueReplayAndKeyRemoveFifo(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Fifo, false);
        }

        void bmHintQueueReplayAndKeyRemoveReverse(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Reverse, false);
        }

        void bmHintQueueReplayAndKeyRemoveShuffled(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Shuffled, false);
        }

        void bmHintQueueReplayAndHandleRemoveFifo(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Fifo, true);
        }

        void bmHintQueueReplayAndHandleRemoveReverse(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Reverse, true);
        }

        void bmHintQueueReplayAndHandleRemoveShuffled(benchmark::State& state) {
            bmHintQueueReplayAndRemove(state, HintRemoveOrder::Shuffled, true);
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndKeyRemoveFifo)->Arg(64)->Arg(256)->Arg(1024);
        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndKeyRemoveReverse)->Arg(64)->Arg(256)->Arg(1024);
        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndKeyRemoveShuffled)->Arg(64)->Arg(256)->Arg(1024);
        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndHandleRemoveFifo)->Arg(64)->Arg(256)->Arg(1024);
        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndHandleRemoveReverse)->Arg(64)->Arg(256)->Arg(1024);
        // NOLINTNEXTLINE
        BENCHMARK(bmHintQueueReplayAndHandleRemoveShuffled)->Arg(64)->Arg(256)->Arg(1024);

        auto makeWalEntry(size_t index, size_t value_size) -> WalEntry {
            WalEntry entry;
            entry.op = WalEntry::Op::Set;
            entry.key = "key-" + std::to_string(index);
            entry.value.assign(value_size, 'v');
            entry.version = index + 1;
            return entry;
        }

        // Production-shaped drain: append a batch, then one flush, optionally
        // followed by one fdatasync (the wal_fsync knob). Bench files live in
        // the build dir (real disk) — never /tmp (tmpfs makes fsync free).
        void bmWalAppendBatch(benchmark::State& state, bool fsync) {
            const auto batch = static_cast<size_t>(state.range(0));
            const auto value_size = static_cast<size_t>(state.range(1));
            std::filesystem::create_directories("bench-tmp");
            const std::string path = "bench-tmp/wal-bench.log";

            std::vector<WalEntry> entries;
            entries.reserve(batch);
            for (size_t i = 0; i < batch; ++i) {
                entries.push_back(makeWalEntry(i, value_size));
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                state.PauseTiming();
                std::filesystem::remove(path);
                WalWriter writer(path);
                state.ResumeTiming();
                for (const auto& entry : entries) {
                    auto res = writer.append(entry);
                    bool appended = res.has_value();
                    benchmark::DoNotOptimize(appended);
                }

                writer.flush();
                if (fsync) {
                    auto synced = writer.sync();
                    bool synced_ok = synced.has_value();
                    benchmark::DoNotOptimize(synced_ok);
                }
            }
            // Timing already stopped: plain cleanup.
            std::filesystem::remove(path);
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(batch));
            state.SetBytesProcessed(
                state.iterations() * static_cast<int64_t>(batch * (value_size + 64))
            );
        }

        void bmWalAppendNoFsync(benchmark::State& state) {
            bmWalAppendBatch(state, false);
        }

        void bmWalAppendFsync(benchmark::State& state) {
            bmWalAppendBatch(state, true);
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmWalAppendNoFsync)->Args({1, 64})->Args({64, 64})->Args({256, 256});
        // NOLINTNEXTLINE
        BENCHMARK(bmWalAppendFsync)->Args({1, 64})->Args({64, 64})->Args({256, 256});

        auto makeSnapshotBatch(size_t count, size_t value_size) -> std::vector<SnapshotEntry> {
            std::vector<SnapshotEntry> entries;
            entries.reserve(count);
            for (size_t i = 0; i < count; ++i) {
                SnapshotEntry entry;
                entry.key = "key-" + std::to_string(i);
                entry.value.assign(value_size, 'v');
                entry.version = i + 1;
                entries.push_back(std::move(entry));
            }
            return entries;
        }

        // Snapshot write: header + one buffered write per entry (the
        // single-copy encode path). Entries are built once outside timing.
        void bmSnapshotWrite(benchmark::State& state) {
            const auto count = static_cast<size_t>(state.range(0));
            const auto value_size = static_cast<size_t>(state.range(1));
            auto entries = makeSnapshotBatch(count, value_size);

            std::filesystem::create_directories("bench-tmp");
            const std::string path = "bench-tmp/snap-bench.dat";
            // NOLINTNEXTLINE
            for (auto _ : state) {
                state.PauseTiming();
                std::filesystem::remove(path);
                SnapshotWriter writer(path);
                state.ResumeTiming();

                auto res = writer.write(1, entries);
                bool written = res.has_value();
                benchmark::DoNotOptimize(written);
            }

            // Timing already stopped: plain cleanup.
            std::filesystem::remove(path);
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(count));
            state.SetBytesProcessed(
                state.iterations() * static_cast<int64_t>(count * (value_size + 64))
            );
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmSnapshotWrite)->Args({64, 64})->Args({1024, 64})->Args({1024, 1024});
    } // namespace
} // namespace cinder
