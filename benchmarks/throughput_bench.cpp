#include <algorithm>
#include <benchmark/benchmark.h>
#include <filesystem>
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
            for (auto _ : state) {
                auto batch = queue.snapshot(now);
                benchmark::DoNotOptimize(batch.data());
                benchmark::DoNotOptimize(batch.size());
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
                std::mt19937 rng(0xC1D3U);
                std::shuffle(indices.begin(), indices.end(), rng);
            }

            HintQueue queue;
            for (size_t i = 0; i < count; ++i) {
                queue.push(makeHint(i, 64));
            }
            const auto now = steady_clock::now();
            for (auto _ : state) {
                auto batch = queue.snapshot(now);
                benchmark::DoNotOptimize(batch.data());
                for (const size_t i : indices) {
                    if (use_handle) {
                        queue.remove(batch[i].handle);
                    } else {
                        queue.remove(batch[i].hint.target, batch[i].hint.req);
                    }
                }
                benchmark::DoNotOptimize(queue.size());
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
            for (auto _ : state) {
                state.PauseTiming();
                std::filesystem::remove(path);
                WalWriter writer(path);
                state.ResumeTiming();
                for (const auto& entry : entries) {
                    auto res = writer.append(entry);
                    benchmark::DoNotOptimize(res.has_value());
                }

                writer.flush();
                if (fsync) {
                    auto synced = writer.sync();
                    benchmark::DoNotOptimize(synced.has_value());
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
            for (auto _ : state) {
                state.PauseTiming();
                std::filesystem::remove(path);
                SnapshotWriter writer(path);
                state.ResumeTiming();

                auto res = writer.write(1, entries);
                benchmark::DoNotOptimize(res.has_value());
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
