#include <benchmark/benchmark.h>
#include <cstdint>
#include <string>
#include <vector>
#include <xxhash.h>

#include "cinder/node/anti_entropy/iblt.hpp"

// IBLT insert/peel micro-costs at the production operating point (64
// cells): is the XOR fold visible next to the XXH3 item hash, and what
// would a VLE count field actually save on a loaded sketch?

namespace cinder {
    namespace {

        auto makeItems(size_t n) -> std::vector<iblt::Item> {
            std::vector<iblt::Item> rows;
            rows.reserve(n);
            for (uint64_t i = 0; i < n; ++i) {
                rows.push_back({
                    .key_hash = 0x9E3779B97F4A7C15ULL * (i + 1),
                    .version = i + 1,
                    .writer = 7,
                    .tombstone = (i % 9) == 0,
                });
            }
            return rows;
        }

        void bmIbltInsert(benchmark::State& state) {
            auto rows = makeItems(48);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                iblt::Sketch sketch;
                sketch.cells.resize(iblt::K_DEFAULT_CELLS);
                for (const auto& row : rows) {
                    iblt::insert(sketch, row.key_hash, row.version, row.writer, row.tombstone);
                }
                benchmark::DoNotOptimize(sketch.cells.data());
            }
            state.SetItemsProcessed(state.iterations() * 48);
        }

        BENCHMARK(bmIbltInsert);

        // Same input shape as the item checksum (4 x u64 words): the hash
        // floor every insert pays. Ratio vs bmIbltInsert says whether the
        // fold is worth touching at all.
        void bmItemHashOnly(benchmark::State& state) {
            auto rows = makeItems(48);
            // NOLINTNEXTLINE
            for (auto _ : state) {
                for (const auto& row : rows) {
                    const uint64_t words[4] = {row.key_hash, row.version, row.writer, 1};
                    benchmark::DoNotOptimize(XXH3_64bits(words, sizeof(words)));
                }
            }
            state.SetItemsProcessed(state.iterations() * 48);
        }

        BENCHMARK(bmItemHashOnly);

        void bmIbltPeel(benchmark::State& state) {
            auto local_rows = makeItems(40);
            auto remote_rows = makeItems(48);
            auto local = iblt::build(local_rows, iblt::K_DEFAULT_CELLS);
            auto remote = iblt::build(remote_rows, iblt::K_DEFAULT_CELLS);
            assert(local.has_value() && remote.has_value());
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto diff = iblt::difference(*remote, *local);
                benchmark::DoNotOptimize(diff.has_value());
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmIbltPeel);

        // Wire size of a loaded 64-cell sketch plus the VLE projection:
        // every stored-sketch count is non-negative and small, so VLE would
        // take 1 byte per cell instead of 8.
        void bmIbltWireBytes(benchmark::State& state) {
            auto rows = makeItems(40);
            auto sketch = iblt::build(rows, iblt::K_DEFAULT_CELLS);
            assert(sketch.has_value());
            size_t vle_bytes = 0;
            for (const auto& cell : sketch->cells) {
                vle_bytes += (cell.count >= 0 && cell.count < 128) ? 1 : 8;
            }
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto blob = iblt::encode(*sketch);
                benchmark::DoNotOptimize(blob.data());
                state.counters["wire_bytes"] = static_cast<double>(blob.size());
                state.counters["vle_count_bytes"] = static_cast<double>(vle_bytes);
            }
            state.SetItemsProcessed(state.iterations());
        }

        BENCHMARK(bmIbltWireBytes);

    } // namespace
} // namespace cinder
