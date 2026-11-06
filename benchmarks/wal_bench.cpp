#include <benchmark/benchmark.h>
#include <filesystem>
#include <string>
#include <vector>

#include "cinder/store/snapshot.hpp"
#include "cinder/store/wal.hpp"

namespace cinder {
    namespace {

        auto benchDir() -> std::filesystem::path {
            // Shared scratch dir; file names are unique per iteration and
            // overwritten across runs, so no cleanup is required.
            static const auto dir = std::filesystem::temp_directory_path() / "cinder_wal_bench";
            std::error_code ec;
            std::filesystem::create_directories(dir, ec);
            return dir;
        }

        auto makeWalEntries(size_t n) -> std::vector<WalEntry> {
            std::vector<WalEntry> entries;
            entries.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                entries.push_back(
                    WalEntry{
                        .op = WalEntry::Op::Set,
                        .key = "bench-key-" + std::to_string(i),
                        .value = std::string(256, 'x'),
                        .version = i + 1,
                        .writer_node_hash = 42,
                    }
                );
            }
            return entries;
        }

        void bmWalAppendSequential(benchmark::State& state) {
            const auto entries = makeWalEntries(static_cast<size_t>(state.range(0)));
            uint64_t file_no = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                WalWriter writer(
                    (benchDir() / ("seq_" + std::to_string(file_no++) + ".wal")).string()
                );
                for (const auto& e : entries) {
                    auto res = writer.append(e);
                    benchmark::DoNotOptimize(res.has_value());
                }
                benchmark::ClobberMemory();
            }
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(entries.size()));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmWalAppendSequential)->Range(8, 512);

        void bmWalAppendBatch(benchmark::State& state) {
            const auto entries = makeWalEntries(static_cast<size_t>(state.range(0)));
            uint64_t file_no = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                WalWriter writer(
                    (benchDir() / ("batch_" + std::to_string(file_no++) + ".wal")).string()
                );
                auto res = writer.appendBatch(entries);
                benchmark::DoNotOptimize(res.has_value());
                benchmark::ClobberMemory();
            }
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(entries.size()));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmWalAppendBatch)->Range(8, 512);

        void bmWalReplayDecode(benchmark::State& state) {
            const auto entries = makeWalEntries(1024);
            const auto path = (benchDir() / "replay.wal").string();
            {
                WalWriter writer(path);
                for (const auto& e : entries) {
                    // NOLINTNEXTLINE
                    (void)writer.append(e);
                }
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                WalReader reader(path);
                size_t count = 0;
                while (auto e = reader.next()) {
                    benchmark::DoNotOptimize(e->key.data());
                    ++count;
                }
                benchmark::DoNotOptimize(count);
                benchmark::ClobberMemory();
            }
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(entries.size()));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmWalReplayDecode);

        void bmSnapshotWriteRead(benchmark::State& state) {
            const size_t n = static_cast<size_t>(state.range(0));
            std::vector<SnapshotEntry> entries;
            entries.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                entries.push_back(
                    SnapshotEntry{
                        .key = "snap-key-" + std::to_string(i),
                        .value = std::string(256, 'y'),
                        .version = i + 1,
                    }
                );
            }
            uint64_t round = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                state.PauseTiming();
                const auto round_path =
                    (benchDir() / ("snap_" + std::to_string(round++) + ".dat")).string();
                SnapshotWriter writer(round_path);
                // NOLINTNEXTLINE
                (void)writer.write(round, entries);
                state.ResumeTiming();
                SnapshotReader reader(round_path);
                auto data = reader.readAll();
                benchmark::DoNotOptimize(data.has_value());
                benchmark::ClobberMemory();
            }
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmSnapshotWriteRead)->Range(128, 4096);

        void bmSnapshotStreamRead(benchmark::State& state) {
            const size_t n = static_cast<size_t>(state.range(0));
            std::vector<SnapshotEntry> entries;
            entries.reserve(n);
            for (size_t i = 0; i < n; ++i) {
                entries.push_back(
                    SnapshotEntry{
                        .key = "snap-key-" + std::to_string(i),
                        .value = std::string(256, 'y'),
                        .version = i + 1,
                    }
                );
            }
            const auto path = (benchDir() / "snap_stream.dat").string();
            {
                SnapshotWriter writer(path);
                // NOLINTNEXTLINE
                (void)writer.write(1, entries);
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                SnapshotReader reader(path);
                Version next = 0;
                size_t count = 0;
                auto res = reader.readStream(next, [&](SnapshotEntry& e) {
                    benchmark::DoNotOptimize(e.key.data());
                    ++count;
                });
                benchmark::DoNotOptimize(res.has_value());
                benchmark::ClobberMemory();
            }
            state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(n));
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmSnapshotStreamRead)->Range(128, 4096);
    } // namespace
} // namespace cinder
