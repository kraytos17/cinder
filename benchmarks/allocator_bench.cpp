#include <ankerl/unordered_dense.h>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "cinder/common/slab_allocator.hpp"
#include "cinder/store/lru_store.hpp"

namespace cinder {
    namespace {

        // Raw allocation baseline: `malloc`-backed std::string churn.
        void bmRawStringAlloc(benchmark::State& state) {
            // NOLINTNEXTLINE
            for (auto _ : state) {
                std::string s(static_cast<size_t>(state.range(0)), 'x');
                benchmark::DoNotOptimize(s);
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmRawStringAlloc)->Arg(16)->Arg(256)->Arg(1 << 12);

        void bmDenseMapChurn(benchmark::State& state) {
            ankerl::unordered_dense::map<std::string, std::string> map;
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = std::to_string(i++);
                map.emplace(key, std::string(static_cast<size_t>(state.range(0)), 'x'));
                map.erase(key);
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmDenseMapChurn)->Arg(16)->Arg(256);

        void bmLruStoreChurn(benchmark::State& state) {
            LruStore store(static_cast<size_t>(state.range(0)));
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key" + std::to_string(i++);
                // NOLINTNEXTLINE
                (void)store.put(key, std::string(256, 'x'));
                (void)store.get(key);
            }
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLruStoreChurn)->Range(1 << 16, 1 << 20);

        // Production-shaped contention: 4 MiB store stripes over the max 16
        // shards, threads use disjoint keys (spread across shards, as in
        // production). Measures the whole put path — allocator CAS, dense
        // index, shard locks, global version/metrics counters — so it tells
        // whether per-shard striping already flattened allocator contention
        // (in which case a TLS allocator cache would add complexity for no
        // measurable win) or whether a shared line still collapses.
        void bmLruStoreContended(benchmark::State& state) {
            // 4 MiB store stripes over the max 16 shards (see shardCountFor).
            LruStore store(4U << 20U);
            const int stride = state.threads();
            const int tid = state.thread_index();
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key-" + std::to_string(tid) + "-" + std::to_string(i++);
                // NOLINTNEXTLINE
                (void)store.put(key, std::string(256, 'x'));
                (void)store.get(key);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLruStoreContended)->ThreadRange(1, 8);

        // Calibration: the key-building overhead alone (no store). Subtract
        // from bmLruStoreContended to isolate the store path.
        void bmKeyBuildOnly(benchmark::State& state) {
            const int tid = state.thread_index();
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key-" + std::to_string(tid) + "-" + std::to_string(i++);
                std::string value(256, 'x');
                benchmark::DoNotOptimize(key.data());
                benchmark::DoNotOptimize(value.data());
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmKeyBuildOnly)->ThreadRange(1, 8);

        // Single-shard control: 32 KiB capacity stays on one shard, so all
        // threads share one mutex/list/index. Comparing against the 16-shard
        // run isolates how much of the scaling cost is shard locking versus
        // the remaining global lines (version clock, byte/entry counters).
        void bmLruStoreContendedSingleShard(benchmark::State& state) {
            LruStore store(32U << 10U);
            const int tid = state.thread_index();
            int i = 0;
            // NOLINTNEXTLINE
            for (auto _ : state) {
                auto key = "key-" + std::to_string(tid) + "-" + std::to_string(i++);
                // NOLINTNEXTLINE
                (void)store.put(key, std::string(256, 'x'));
                (void)store.get(key);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmLruStoreContendedSingleShard)->ThreadRange(1, 8);

        struct BenchSlot {
            alignas(16) std::byte storage[120];
        };

        using BenchPool = SlabAllocator<BenchSlot>::Pool;

        // Baseline: alloc/dealloc ping-pong on one shared pool. Every op CASes
        // the single global free_head_.
        void bmSlabAllocDealloc(benchmark::State& state) {
            static auto pool = std::make_shared<BenchPool>();
            for (auto _ : state) {
                auto* p = pool->allocate();
                benchmark::DoNotOptimize(p);
                pool->deallocate(p);
            }
            state.SetItemsProcessed(state.iterations());
        }

        // NOLINTNEXTLINE
        BENCHMARK(bmSlabAllocDealloc)->ThreadRange(1, 8);
    } // namespace
} // namespace cinder
